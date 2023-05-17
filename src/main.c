/*
 * main.pc
 * driver for the tpcc transactions
 */

#include <stdio.h>
#include <stdlib.h>
#include <unistd.h>
#include <string.h>
#include <sys/time.h>
#include <signal.h>
#include <pthread.h>
#include <fcntl.h>
#include <time.h>

#include <sqlite3.h>

#include "tpc.h"
#include "trans_if.h"
#include "spt_proc.h"
#include "sequence.h"
#include "rthist.h"
#include "sb_percentile.h"
#include "main.h"

int num_ware;
int num_conn;
int lampup_time;
int measure_time;

int num_node; /* number of servers that consists of cluster i.e. RAC (0:normal mode)*/
#define NUM_NODE_MAX 8
int time_count;
int PRINT_INTERVAL = 5;
int multi_schema = 0;
int multi_schema_offset = 0;

all_tx_stat_t g_stats;
all_tx_stat_t* stats_per_thread;

int prev_s[5];
int prev_l[5];

double max_rt[5];
double total_rt[5];
double cur_max_rt[5];

double prev_total_rt[5];

#define RTIME_NEWORD 5
#define RTIME_PAYMENT 5
#define RTIME_ORDSTAT 5
#define RTIME_DELIVERY 80
#define RTIME_SLEV 20

int rt_limit[TX_NUMS] = { RTIME_NEWORD, RTIME_PAYMENT, RTIME_ORDSTAT, RTIME_DELIVERY,
	RTIME_SLEV };

const char *tx_name[TX_NUMS] = {
	"New-Order",
	"Payment",
	"Order-Status",
	"Delivery",
	"Stock-Level",
};

sb_percentile_t local_percentile;

int activate_transaction;
double time_taken;
clock_t time_start;
clock_t time_end;
int counting_on;
int num_trans;

long clk_tck;

int is_local = 0; /* "1" mean local */
int valuable_flg = 0; /* "1" mean valuable ratio */

char *dbpath = NULL;


/* stat helper functions */
void clear_tx_stat(tx_stat_t *st)
{
	st->success = st->late = st->retry = st->failure = 0;
}

void clear_all_tx_stats(all_tx_stat_t *st)
{
	for (enum tx_type tx = 0; tx < TX_NUMS; ++tx)
		clear_tx_stat(&st->stat[tx]);
}

void check_individual_constraint(enum tx_type tx, float lowerbound, int total)
{
	float f = 100.0 * (float)(g_stats.stat[tx].success + g_stats.stat[tx].late) /(float)total;
	printf("        %s: %3.2f%% (>=%.1f%%)", tx_name[tx], f, lowerbound);
	printf(f >= lowerbound ? " [OK]\n" : " [NG] *\n");
}

void check_individual_response_time(enum tx_type tx, float lowerbound)
{
	float f = 100.0 * (float)g_stats.stat[tx].success /
		(float)(g_stats.stat[tx].success + g_stats.stat[tx].late);
	printf("      %s: %3.2f%% ", tx_name[tx], f);
	printf(f >= lowerbound ? " [OK]\n" : " [NG] *\n");
}

void check_constraints_and_response_times()
{
	printf("\n<Constraint Check> (all must be [OK])\n [transaction percentage]\n");
	int j = 0;
	for (int i = 0; i < 5; i++)
		j += g_stats.stat[i].success + g_stats.stat[i].late;

	check_individual_constraint(TX_PAYMENT, 43.0, j);
	check_individual_constraint(TX_ORDSTAT, 4.0, j);
	check_individual_constraint(TX_DELIVERY, 4.0, j);
	check_individual_constraint(TX_SLEV, 4.0, j);

	printf(" [response time (at least 90%% passed)]\n");
	for (enum tx_type tx = 0; tx < TX_NUMS; ++tx)
		check_individual_response_time(tx, 90.0);
}


int thread_main(thread_arg *);

void alarm_handler(int signum);
void alarm_dummy();

int main(int argc, char *argv[])
{
	int i, k, t_num, arg_offset, c;
	long j;
	float f;
	thread_arg *thd_arg;
	struct itimerval itval;
	struct sigaction sigact;

	printf("CHECKING IF SQLITE IS THREADSAFE: RETURN VALUE = %d\n",
	       sqlite3_threadsafe());

	printf("***************************************\n");
	printf("*** ###easy### TPC-C Load Generator ***\n");
	printf("***************************************\n");

	/* initialize */
	hist_init();
	activate_transaction = 1;
	counting_on = 0;

	for (i = 0; i < 5; i++) {
		g_stats.stat[i].success = 0;
		g_stats.stat[i].late = 0;
		g_stats.stat[i].retry = 0;
		g_stats.stat[i].failure = 0;

		prev_s[i] = 0;
		prev_l[i] = 0;

		prev_total_rt[i] = 0.0;
		max_rt[i] = 0.0;
		total_rt[i] = 0.0;
	}

	/* dummy initialize*/
	num_ware = 1;
	num_conn = 10;
	lampup_time = 5;
	measure_time = 20;
	num_trans = 10000;

	/* number of node (default 0) */
	num_node = 0;
	arg_offset = 0;

	clk_tck = sysconf(_SC_CLK_TCK);

	/* Parse args */

	while ((c = getopt(argc, argv, "w:c:r:l:i:m:o:t:0:1:2:3:4:f:")) != -1) {
		switch (c) {
		case 'w':
			printf("option w with value '%s'\n", optarg);
			num_ware = atoi(optarg);
			break;
		case 'c':
			printf("option c with value '%s'\n", optarg);
			num_conn = atoi(optarg);
			break;
		case 'r':
			printf("option r with value '%s'\n", optarg);
			lampup_time = atoi(optarg);
			break;
		case 'l':
			printf("option l with value '%s'\n", optarg);
			measure_time = atoi(optarg);
			break;
		case 'm':
			printf("option m (multiple schemas) with value '%s'\n",
			       optarg);
			multi_schema = atoi(optarg);
			break;
		case 'o':
			printf("option o (multiple schemas offset) with value '%s'\n",
			       optarg);
			multi_schema_offset = atoi(optarg);
			break;
		case 't':
			printf("option t (number of transactions) with value '%s'\n",
			       optarg);
			num_trans = atoi(optarg);
			break;
		case 'i':
			printf("option i with value '%s'\n", optarg);
			PRINT_INTERVAL = atoi(optarg);
			break;
		case '0':
			printf("option 0 (response time limit for transaction 0) '%s'\n",
			       optarg);
			rt_limit[0] = atoi(optarg);
			break;
		case '1':
			printf("option 1 (response time limit for transaction 1) '%s'\n",
			       optarg);
			rt_limit[1] = atoi(optarg);
			break;
		case '2':
			printf("option 2 (response time limit for transaction 2) '%s'\n",
			       optarg);
			rt_limit[2] = atoi(optarg);
			break;
		case '3':
			printf("option 3 (response time limit for transaction 3) '%s'\n",
			       optarg);
			rt_limit[3] = atoi(optarg);
			break;
		case '4':
			printf("option 4 (response time limit for transaction 4) '%s'\n",
			       optarg);
			rt_limit[4] = atoi(optarg);
			break;
		case 'f':
			printf("option f with value '%s'\n", optarg);
			dbpath = strdup(optarg);
			break;
		case '?':
			printf("Usage: tpcc_start -w warehouses -c connections -r warmup_time -l running_time -i report_interval -f db_file\n");
			exit(0);
		default:
			printf("?? getopt returned character code 0%o ??\n", c);
		}
	}
	if (optind < argc) {
		printf("non-option ARGV-elements: ");
		while (optind < argc)
			printf("%s ", argv[optind++]);
		printf("\n");
	}
	if (!dbpath) {
		printf("DB file (-f) is not provided!\n");
		exit(-1);
	}

	if (valuable_flg == 1) {
		if ((atoi(argv[9 + arg_offset]) < 0) ||
		    (atoi(argv[10 + arg_offset]) < 0) ||
		    (atoi(argv[11 + arg_offset]) < 0) ||
		    (atoi(argv[12 + arg_offset]) < 0) ||
		    (atoi(argv[13 + arg_offset]) < 0)) {
			fprintf(stderr,
				"\n expecting positive number of ratio parameters\n");
			exit(1);
		}
	}

	if (num_node > 0) {
		if (num_ware % num_node != 0) {
			fprintf(stderr,
				"\n [warehouse] value must be devided by [num_node].\n");
			exit(1);
		}
		if (num_conn % num_node != 0) {
			fprintf(stderr,
				"\n [connection] value must be devided by [num_node].\n");
			exit(1);
		}
	}

	printf("<Parameters>\n");
	printf("  [warehouse]: %d\n", num_ware);
	printf(" [connection]: %d\n", num_conn);
	printf("     [rampup]: %d (sec.)\n", lampup_time);
	printf("    [measure]: %d (sec.)\n", measure_time);

	if (valuable_flg == 1) {
		printf("      [ratio]: %d:%d:%d:%d:%d\n",
		       atoi(argv[9 + arg_offset]), atoi(argv[10 + arg_offset]),
		       atoi(argv[11 + arg_offset]), atoi(argv[12 + arg_offset]),
		       atoi(argv[13 + arg_offset]));
	}

	/* alarm initialize */
	time_count = 0;

	// Interval print via sigalarm
	itval.it_interval.tv_sec = PRINT_INTERVAL;
	itval.it_interval.tv_usec = 0;
	itval.it_value.tv_sec = PRINT_INTERVAL;
	itval.it_value.tv_usec = 0;
	sigact.sa_handler = alarm_handler;
	sigact.sa_flags = 0;
	sigemptyset(&sigact.sa_mask);
	/* setup handler&timer */
	if (sigaction(SIGALRM, &sigact, NULL) == -1) {
		fprintf(stderr, "error in sigaction()\n");
		exit(1);
	}

	init_randomness();

	if (valuable_flg == 0) {
		seq_init(10, 10, 1, 1, 1); /* normal ratio */
	} else {
		seq_init(atoi(argv[9 + arg_offset]),
			 atoi(argv[10 + arg_offset]),
			 atoi(argv[11 + arg_offset]),
			 atoi(argv[12 + arg_offset]),
			 atoi(argv[13 + arg_offset]));
	}

	if (sb_percentile_init(&local_percentile, 100000, 1.0, 1e13))
		return -1;

	/* set up threads */
	thd_arg = malloc(sizeof(thread_arg) * num_conn);
	if (thd_arg == NULL) {
		fprintf(stderr, "error at malloc(thread_arg)\n");
		exit(1);
	}

	/* EXEC SQL WHENEVER SQLERROR GOTO sqlerr; */

	counting_on = 0;

	for (t_num = 0; t_num < num_conn; t_num++) {
		thread_arg *arg = &thd_arg[t_num];
		arg->number = t_num;
		clear_all_tx_stats(&arg->stats);
		arg->ctx = NULL;
		arg->stmt = malloc(sizeof(sqlite3_stmt *) * 40);
	}

	for (t_num = 0; t_num < num_conn; t_num++) {
		thread_arg *arg = &thd_arg[t_num];
		pthread_create(&arg->pth, NULL, (void *)thread_main, (void *)arg);
	}

	printf("\nRAMP-UP TIME.(%d sec.)\n", lampup_time);
	fflush(stdout);
	sleep(lampup_time);
	printf("\nMEASURING START.\n\n");
	fflush(stdout);

	/* sleep(measure_time); */
	/* start timer */

// #ifndef _SLEEP_ONLY_
// 	if (setitimer(ITIMER_REAL, &itval, NULL) == -1) {
// 		fprintf(stderr, "error in setitimer()\n");
// 	}
// #endif

	counting_on = 1;
	/* wait signal */
	/*
  for(i = 0; i < (measure_time / PRINT_INTERVAL); i++ ) {
  //while (activate_transaction) {
#ifndef _SLEEP_ONLY_
    pause();
#else
    sleep(PRINT_INTERVAL);
    alarm_dummy();
#endif
  }
  */
	for (int i = 0; i < (measure_time / PRINT_INTERVAL); ++i) {
		sleep(PRINT_INTERVAL);
		alarm_dummy();
	}
	// sleep(measure_time);
	counting_on = 0;

// #ifndef _SLEEP_ONLY_
// 	/* stop timer */
// 	itval.it_interval.tv_sec = 0;
// 	itval.it_interval.tv_usec = 0;
// 	itval.it_value.tv_sec = 0;
// 	itval.it_value.tv_usec = 0;
// 	if (setitimer(ITIMER_REAL, &itval, NULL) == -1) {
// 		fprintf(stderr, "error in setitimer()\n");
// 	}
// #endif

	printf("\nSTOPPING THREADS");
	activate_transaction = 0;

	/* wait threads' ending and close connections*/
	for (i = 0; i < num_conn; i++) {
		pthread_join(thd_arg[i].pth, NULL);
		free(thd_arg[i].stmt);
	}

	printf("\n");

	free(thd_arg);

	//hist_report();
	printf("\n<Raw Results>\n");
	for (enum tx_type tx = 0; tx < TX_NUMS; ++tx) {
		tx_stat_t *st = &g_stats.stat[tx];
		printf("  [%d:%s] sc:%d lt:%d  rt:%d  fl:%d avg_rt: %.1f (%d)\n",
		       tx, tx_name[tx], st->success, st->late, st->retry, st->failure,
		       total_rt[tx] / (st->success + st->late), rt_limit[tx]);
	}
	printf(" in %d sec.\n",
	       (measure_time / PRINT_INTERVAL) * PRINT_INTERVAL);

	printf("\n<Raw Results2(sum from per-thread stats)>\n");

	all_tx_stat_t stats_sum;
	for (enum tx_type tx = 0; tx < TX_NUMS; ++tx) {
		tx_stat_t *st = &stats_sum.stat[tx];
		st->success = 0;
		st->late = 0;
		st->retry = 0;
		st->failure = 0;
		for (k = 0; k < num_conn; k++) {
			tx_stat_t *per_thread = &thd_arg[k].stats.stat[tx];
			st->success += per_thread->success;
			st->late += per_thread->late;
			st->retry += per_thread->retry;
			st->failure += per_thread->failure;
		}
		printf("  [%d:%s] sc:%d lt:%d  rt:%d  fl:%d avg_rt: %.1f (%d)\n",
		       tx, tx_name[tx], st->success, st->late, st->retry, st->failure,
		       total_rt[tx] / (st->success + st->late), rt_limit[tx]);
	}

	// Checks
	check_constraints_and_response_times();

	printf("\n<TpmC>\n");
	f = (float)(g_stats.stat[0].success + g_stats.stat[0].late) * 60.0 /
	    (float)((measure_time / PRINT_INTERVAL) * PRINT_INTERVAL);
	printf("                 %.3f TpmC\n", f);

	printf("\nTime taken\n");
	time_taken = ((double)(time_end - time_start)) / CLOCKS_PER_SEC;
	printf("                 %.3f seconds\n", time_taken);

	exit(0);

sqlerr:
	fprintf(stdout, "error at main\n");
	error(thd_arg[i].ctx, 0);
	exit(1);
}

void alarm_handler(int signum)
{
	int i;
	int s[5], l[5];
	double rt90[5];
	double trt[5];
	double percentile_val;
	double percentile_val99;

	for (i = 0; i < 5; i++) {
		s[i] = g_stats.stat[i].success;
		l[i] = g_stats.stat[i].late;
		trt[i] = total_rt[i];
		//rt90[i] = hist_ckp(i);
	}

	time_count += PRINT_INTERVAL;
	percentile_val = sb_percentile_calculate(&local_percentile, 95);
	percentile_val99 = sb_percentile_calculate(&local_percentile, 99);
	sb_percentile_reset(&local_percentile);
	//  printf("%4d, %d:%.3f|%.3f(%.3f), %d:%.3f|%.3f(%.3f), %d:%.3f|%.3f(%.3f), %d:%.3f|%.3f(%.3f), %d:%.3f|%.3f(%.3f)\n",
	printf("%4d, trx: %d, 95%: %.3f, 99%: %.3f, max_rt: %.3f, %d|%.3f, %d|%.3f, %d|%.3f, %d|%.3f\n",
	       time_count, (s[0] + l[0] - prev_s[0] - prev_l[0]),
	       percentile_val, percentile_val99, (double)cur_max_rt[0],
	       (s[1] + l[1] - prev_s[1] - prev_l[1]), (double)cur_max_rt[1],
	       (s[2] + l[2] - prev_s[2] - prev_l[2]), (double)cur_max_rt[2],
	       (s[3] + l[3] - prev_s[3] - prev_l[3]), (double)cur_max_rt[3],
	       (s[4] + l[4] - prev_s[4] - prev_l[4]), (double)cur_max_rt[4]);
	fflush(stdout);

	for (i = 0; i < 5; i++) {
		prev_s[i] = s[i];
		prev_l[i] = l[i];
		prev_total_rt[i] = trt[i];
		cur_max_rt[i] = 0.0;
	}
}

void alarm_dummy()
{
	int i;
	int s[5], l[5];
	float rt90[5];

	for (i = 0; i < 5; i++) {
		s[i] = g_stats.stat[i].success;
		l[i] = g_stats.stat[i].late;
		rt90[i] = hist_ckp(i);
	}

	time_count += PRINT_INTERVAL;
	printf("%4d, %d(%d):%.2f, %d(%d):%.2f, %d(%d):%.2f, %d(%d):%.2f, %d(%d):%.2f\n",
	       time_count, (s[0] + l[0] - prev_s[0] - prev_l[0]),
	       (l[0] - prev_l[0]), rt90[0],
	       (s[1] + l[1] - prev_s[1] - prev_l[1]), (l[1] - prev_l[1]),
	       rt90[1], (s[2] + l[2] - prev_s[2] - prev_l[2]),
	       (l[2] - prev_l[2]), rt90[2],
	       (s[3] + l[3] - prev_s[3] - prev_l[3]), (l[3] - prev_l[3]),
	       rt90[3], (s[4] + l[4] - prev_s[4] - prev_l[4]),
	       (l[4] - prev_l[4]), rt90[4]);
	fflush(stdout);

	for (i = 0; i < 5; i++) {
		prev_s[i] = s[i];
		prev_l[i] = l[i];
	}
}

static inline const char *sql_statements[] =
{
	"SELECT c_discount, c_last, c_credit, w_tax FROM customer, warehouse WHERE w_id = ? AND c_w_id = w_id AND c_d_id = ? AND c_id = ?",
	"SELECT d_next_o_id, d_tax FROM district WHERE d_id = ? AND d_w_id = ?",
	"UPDATE district SET d_next_o_id = ? + 1 WHERE d_id = ? AND d_w_id = ?",
	"INSERT INTO orders (o_id, o_d_id, o_w_id, o_c_id, o_entry_d, o_ol_cnt, o_all_local) VALUES(?, ?, ?, ?, ?, ?, ?)",
	"INSERT INTO new_orders (no_o_id, no_d_id, no_w_id) VALUES (?,?,?)",
	"SELECT i_price, i_name, i_data FROM item WHERE i_id = ?",
	"SELECT s_quantity, s_data, s_dist_01, s_dist_02, s_dist_03, s_dist_04, s_dist_05, s_dist_06, s_dist_07, s_dist_08, s_dist_09, s_dist_10 FROM stock WHERE s_i_id = ? AND s_w_id = ?",
	"UPDATE stock SET s_quantity = ? WHERE s_i_id = ? AND s_w_id = ?",
	"INSERT INTO order_line (ol_o_id, ol_d_id, ol_w_id, ol_number, ol_i_id, ol_supply_w_id, ol_quantity, ol_amount, ol_dist_info) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)",
	"UPDATE warehouse SET w_ytd = w_ytd + ? WHERE w_id = ?",
	"SELECT w_street_1, w_street_2, w_city, w_state, w_zip, w_name FROM warehouse WHERE w_id = ?",
	"UPDATE district SET d_ytd = d_ytd + ? WHERE d_w_id = ? AND d_id = ?",
	"SELECT d_street_1, d_street_2, d_city, d_state, d_zip, d_name FROM district WHERE d_w_id = ? AND d_id = ?",
	"SELECT count(c_id) FROM customer WHERE c_w_id = ? AND c_d_id = ? AND c_last = ?",
	"SELECT c_id FROM customer WHERE c_w_id = ? AND c_d_id = ? AND c_last = ? ORDER BY c_first",
	"SELECT c_first, c_middle, c_last, c_street_1, c_street_2, c_city, c_state, c_zip, c_phone, c_credit, c_credit_lim, c_discount, c_balance, c_since FROM customer WHERE c_w_id = ? AND c_d_id = ? AND c_id = ?",
	"SELECT c_data FROM customer WHERE c_w_id = ? AND c_d_id = ? AND c_id = ?",
	"UPDATE customer SET c_balance = ?, c_data = ? WHERE c_w_id = ? AND c_d_id = ? AND c_id = ?",
	"UPDATE customer SET c_balance = ? WHERE c_w_id = ? AND c_d_id = ? AND c_id = ?",
	"INSERT INTO history(h_c_d_id, h_c_w_id, h_c_id, h_d_id, h_w_id, h_date, h_amount, h_data) VALUES(?, ?, ?, ?, ?, ?, ?, ?)",
	"SELECT count(c_id) FROM customer WHERE c_w_id = ? AND c_d_id = ? AND c_last = ?",
	"SELECT c_balance, c_first, c_middle, c_last FROM customer WHERE c_w_id = ? AND c_d_id = ? AND c_last = ? ORDER BY c_first",
	"SELECT c_balance, c_first, c_middle, c_last FROM customer WHERE c_w_id = ? AND c_d_id = ? AND c_id = ?",
	"SELECT o_id, o_entry_d, COALESCE(o_carrier_id,0) FROM orders WHERE o_w_id = ? AND o_d_id = ? AND o_c_id = ? AND o_id = (SELECT MAX(o_id) FROM orders WHERE o_w_id = ? AND o_d_id = ? AND o_c_id = ?)",
	"SELECT ol_i_id, ol_supply_w_id, ol_quantity, ol_amount, ol_delivery_d FROM order_line WHERE ol_w_id = ? AND ol_d_id = ? AND ol_o_id = ?",
	"SELECT COALESCE(MIN(no_o_id),0) FROM new_orders WHERE no_d_id = ? AND no_w_id = ?",
	"DELETE FROM new_orders WHERE no_o_id = ? AND no_d_id = ? AND no_w_id = ?",
	"SELECT o_c_id FROM orders WHERE o_id = ? AND o_d_id = ? AND o_w_id = ?",
	"UPDATE orders SET o_carrier_id = ? WHERE o_id = ? AND o_d_id = ? AND o_w_id = ?",
	"UPDATE order_line SET ol_delivery_d = ? WHERE ol_o_id = ? AND ol_d_id = ? AND ol_w_id = ?",
	"SELECT SUM(ol_amount) FROM order_line WHERE ol_o_id = ? AND ol_d_id = ? AND ol_w_id = ?",
	"UPDATE customer SET c_balance = c_balance + ? , c_delivery_cnt = c_delivery_cnt + 1 WHERE c_id = ? AND c_d_id = ? AND c_w_id = ?",
	"SELECT d_next_o_id FROM district WHERE d_id = ? AND d_w_id = ?",
	"SELECT DISTINCT ol_i_id FROM order_line WHERE ol_w_id = ? AND ol_d_id = ? AND ol_o_id < ? AND ol_o_id >= (? - 20)",
	"SELECT count(*) FROM stock WHERE s_w_id = ? AND s_i_id = ? AND s_quantity < ?",
};

int thread_main(thread_arg *arg)
{
	int t_num = arg->number;
	int r, i;
	sqlite3 *sqlite3_db = NULL;

	/* EXEC SQL WHENEVER SQLERROR GOTO sqlerr;*/

	// printf("Using schema: %s\n", db_string_full);

	/* exec sql connect :connect_string; */
	printf("%s: opening db=%s, thread id = %lu\n", __func__, dbpath, pthread_self());
	sqlite3_open(dbpath, &sqlite3_db);
	if (!sqlite3_db) {
		printf("%s: Failed to open DB=%s\n", __func__, dbpath);
	}
	printf("%s: opened db=%s, thread id = %lu\n", __func__, dbpath, pthread_self());

	sqlite3_exec(sqlite3_db, "PRAGMA journal_mode = WAL;", 0, 0, 0);

	if (!sqlite3_db) {
		goto sqlerr;
	}

	arg->ctx = sqlite3_db;

	/* Prepare ALL of SQLs */
	for (int i = 0; i < 35; ++i) {
		if (sqlite3_prepare_v2(sqlite3_db, sql_statements[i], -1, &arg->stmt[i], NULL) != SQLITE_OK)
			goto sqlerr;
	}

	INITIALIZE_TIMERS();

	time_start = clock();

	for (i = 0; i < num_trans; i++) {
		if (sqlite3_exec(sqlite3_db, "BEGIN TRANSACTION;", NULL, NULL, NULL) != SQLITE_OK)
			goto sqlerr;

		r = driver(t_num, arg);

		/* EXEC SQL COMMIT WORK; */
		if (sqlite3_exec(sqlite3_db, "COMMIT;", NULL, NULL, NULL) != SQLITE_OK)
			goto sqlerr;
	}

	PRINT_TIME();

	time_end = clock();

	for (i = 0; i < 40; i++) {
		sqlite3_reset(arg->stmt[i]);
	}

	/* EXEC SQL DISCONNECT; */
	sqlite3_close(arg->ctx);

	printf(".");
	fflush(stdout);

	return (r);

sqlerr:
	fprintf(stdout, "error at thread_main\n");
	printf("%s: error: %s\n", __func__, sqlite3_errmsg(arg->ctx));

	//error(ctx[t_num],0);
	return (0);
}

/*
 * driver.c
 * driver for the tpcc transactions
 */

#include <stdio.h>
#include <stdlib.h>
#include <sys/times.h>
#include <time.h>
#include "tpc.h" /* prototypes for misc. functions */
#include "trans_if.h" /* prototypes for transacation interface calls */
#include "sequence.h"
#include "rthist.h"
#include "sb_percentile.h"
#include <sqlite3.h>

#include "main.h"

static int other_ware(int home_ware);
static int do_neword(int t_num, thread_arg *arg);
static int do_payment(int t_num, thread_arg *arg);
static int do_ordstat(int t_num, thread_arg *arg);
static int do_delivery(int t_num, thread_arg *arg);
static int do_slev(int t_num, thread_arg *arg);

extern sqlite3 **ctx;
extern int num_ware;
extern int num_conn;
extern int activate_transaction;
extern int counting_on;
extern int time_start;
extern int time_end;
extern int num_trans;

extern int num_node;
extern int time_count;
extern FILE *freport_file;

extern double max_rt[];
extern double total_rt[];

extern int rt_limit[];

extern long clk_tck;
extern sb_percentile_t local_percentile;

#define MAX_RETRY 2000

static inline void inc_success(enum tx_type tx, thread_arg *arg)
{
	g_stats.stat[tx].success++;
	arg->stats.stat[tx].success++;
}

static inline void inc_late(enum tx_type tx, thread_arg *arg)
{
	g_stats.stat[tx].late++;
	arg->stats.stat[tx].late++;
}

static inline void inc_retry(enum tx_type tx, thread_arg *arg)
{
	g_stats.stat[tx].retry++;
	arg->stats.stat[tx].retry++;
}

static inline void inc_failure(enum tx_type tx, thread_arg *arg)
{
	g_stats.stat[tx].retry--;
	arg->stats.stat[tx].retry--;
	g_stats.stat[tx].failure++;
	arg->stats.stat[tx].failure++;
}

static void update_on_success(enum tx_type tx, thread_arg *arg,
			      struct timespec *tbuf1,
			      struct timespec *tbuf2)
{
	double rt = (double)(tbuf2->tv_sec * 1000.0 +
			     tbuf2->tv_nsec / 1000000.0 -
			     tbuf1->tv_sec * 1000.0 -
			     tbuf1->tv_nsec / 1000000.0);
	//printf("NOT : %.3f\n", rt);

	if (rt > max_rt[tx])
		max_rt[tx] = rt;
	total_rt[tx] += rt;
	sb_percentile_update(&local_percentile, rt);
	hist_inc(tx, rt);
	if (counting_on) {
		if (rt < rt_limit[tx]) {
			inc_success(tx, arg);
		} else {
			inc_late(tx, arg);
		}
	}
}


int driver(int t_num, thread_arg *arg)
{
	int i, j;
	instrumentation_type neword_time, payment_time, ordstat_time,
		delivery_time, slev_time;
	/* Actually, WaitTimes are needed... */

	switch (seq_get()) {
	case 0:
		START_TIMING(neword_t, neword_time);
		do_neword(t_num, arg);
		END_TIMING(neword_t, neword_time);
		break;
	case 1:
		START_TIMING(payment_t, payment_time);
		do_payment(t_num, arg);
		END_TIMING(payment_t, payment_time);
		break;
	case 2:
		START_TIMING(ordstat_t, ordstat_time);
		do_ordstat(t_num, arg);
		END_TIMING(ordstat_t, ordstat_time);
		break;
	case 3:
		START_TIMING(delivery_t, delivery_time);
		do_delivery(t_num, arg);
		END_TIMING(delivery_t, delivery_time);
		break;
	case 4:
		START_TIMING(slev_t, slev_time);
		do_slev(t_num, arg);
		END_TIMING(slev_t, slev_time);
		break;
	default:
		printf("Error - Unknown sequence.\n");
	}

	return (0);
}

/*
 * prepare data and execute the new order transaction for one order
 * officially, this is supposed to be simulated terminal I/O
 */
static int do_neword(int t_num, thread_arg *arg)
{
	int c_num;
	int i, ret;
	clock_t clk1, clk2;
	double rt;
	struct timespec tbuf1;
	struct timespec tbuf2;
	int w_id, d_id, c_id, ol_cnt;
	int all_local = 1;
	int notfound =
		MAXITEMS + 1; /* valid item ids are numbered consecutively
				    [1..MAXITEMS] */
	int rbk;
	int itemid[MAX_NUM_ITEMS];
	int supware[MAX_NUM_ITEMS];
	int qty[MAX_NUM_ITEMS];

	if (num_node == 0) {
		w_id = RandomNumber(1, num_ware);
	} else {
		c_num = ((num_node * t_num) / num_conn); /* drop moduls */
		w_id = RandomNumber(1 + (num_ware * c_num) / num_node,
				    (num_ware * (c_num + 1)) / num_node);
	}
	d_id = RandomNumber(1, DIST_PER_WARE);
	c_id = NURand(1023, 1, CUST_PER_DIST);

	ol_cnt = RandomNumber(5, 15);
	rbk = RandomNumber(1, 100);

	for (i = 0; i < ol_cnt; i++) {
		itemid[i] = NURand(8191, 1, MAXITEMS);
		if ((i == ol_cnt - 1) && (rbk == 1)) {
			itemid[i] = notfound;
		}
		if (RandomNumber(1, 100) != 1) {
			supware[i] = w_id;
		} else {
			supware[i] = other_ware(w_id);
			all_local = 0;
		}
		qty[i] = RandomNumber(1, 10);
	}

	clk1 = clock_gettime(CLOCK_MONOTONIC, &tbuf1);
	for (i = 0; i < MAX_RETRY; i++) {
		ret = neword(t_num, arg, w_id, d_id, c_id, ol_cnt, all_local, itemid,
			     supware, qty);
		clk2 = clock_gettime(CLOCK_MONOTONIC, &tbuf2);

		if (ret) {
			update_on_success(0, arg, &tbuf1, &tbuf2);
			return (1); /* end */
		} else {
			if (counting_on) {
				inc_retry(0, arg);
			}
		}
	}

	if (counting_on) {
		inc_failure(0, arg);
	}

	return (0);
}

/*
 * produce the id of a valid warehouse other than home_ware
 * (assuming there is one)
 */
static int other_ware(int home_ware)
{
	int tmp;

	if (num_ware == 1)
		return home_ware;
	while ((tmp = RandomNumber(1, num_ware)) == home_ware)
		;
	return tmp;
}

/*
 * prepare data and execute payment transaction
 */
static int do_payment(int t_num, thread_arg *arg)
{
	int c_num;
	int byname, i, ret;
	clock_t clk1, clk2;
	double rt;
	struct timespec tbuf1;
	struct timespec tbuf2;
	int w_id, d_id, c_w_id, c_d_id, c_id, h_amount;
	char c_last[17];

	if (num_node == 0) {
		w_id = RandomNumber(1, num_ware);
	} else {
		c_num = ((num_node * t_num) / num_conn); /* drop moduls */
		w_id = RandomNumber(1 + (num_ware * c_num) / num_node,
				    (num_ware * (c_num + 1)) / num_node);
	}
	d_id = RandomNumber(1, DIST_PER_WARE);
	c_id = NURand(1023, 1, CUST_PER_DIST);
	Lastname(NURand(255, 0, 999), c_last);
	h_amount = RandomNumber(1, 5000);
	if (RandomNumber(1, 100) <= 60) {
		byname = 1; /* select by last name */
	} else {
		byname = 0; /* select by customer id */
	}
	if (RandomNumber(1, 100) <= 85) {
		c_w_id = w_id;
		c_d_id = d_id;
	} else {
		c_w_id = other_ware(w_id);
		c_d_id = RandomNumber(1, DIST_PER_WARE);
	}

	clk1 = clock_gettime(CLOCK_MONOTONIC, &tbuf1);
	for (i = 0; i < MAX_RETRY; i++) {
		ret = payment(t_num, arg, w_id, d_id, byname, c_w_id, c_d_id, c_id,
			      c_last, h_amount);
		clk2 = clock_gettime(CLOCK_MONOTONIC, &tbuf2);

		if (ret) {
			update_on_success(1, arg, &tbuf1, &tbuf2);

			return (1); /* end */
		} else {
			if (counting_on) {
				inc_retry(1, arg);
			}
		}
	}

	if (counting_on) {
		inc_failure(1, arg);
	}

	return (0);
}

/*
 * prepare data and execute order status transaction
 */
static int do_ordstat(int t_num, thread_arg *arg)
{
	int c_num;
	int byname, i, ret;
	clock_t clk1, clk2;
	double rt;
	struct timespec tbuf1;
	struct timespec tbuf2;
	int w_id, d_id, c_id;
	char c_last[16];

	if (num_node == 0) {
		w_id = RandomNumber(1, num_ware);
	} else {
		c_num = ((num_node * t_num) / num_conn); /* drop moduls */
		w_id = RandomNumber(1 + (num_ware * c_num) / num_node,
				    (num_ware * (c_num + 1)) / num_node);
	}
	d_id = RandomNumber(1, DIST_PER_WARE);
	c_id = NURand(1023, 1, CUST_PER_DIST);
	Lastname(NURand(255, 0, 999), c_last);
	if (RandomNumber(1, 100) <= 60) {
		byname = 1; /* select by last name */
	} else {
		byname = 0; /* select by customer id */
	}

	clk1 = clock_gettime(CLOCK_MONOTONIC, &tbuf1);
	for (i = 0; i < MAX_RETRY; i++) {
		ret = ordstat(t_num, arg, w_id, d_id, byname, c_id, c_last);
		clk2 = clock_gettime(CLOCK_MONOTONIC, &tbuf2);

		if (ret) {
			update_on_success(2, arg, &tbuf1, &tbuf2);

			return (1); /* end */
		} else {
			if (counting_on) {
				inc_retry(2, arg);
			}
		}
	}

	if (counting_on) {
		inc_failure(2, arg);
	}

	return (0);
}

/*
 * execute delivery transaction
 */
static int do_delivery(int t_num, thread_arg *arg)
{
	int c_num;
	int i, ret;
	clock_t clk1, clk2;
	double rt;
	struct timespec tbuf1;
	struct timespec tbuf2;
	int w_id, o_carrier_id;

	if (num_node == 0) {
		w_id = RandomNumber(1, num_ware);
	} else {
		c_num = ((num_node * t_num) / num_conn); /* drop moduls */
		w_id = RandomNumber(1 + (num_ware * c_num) / num_node,
				    (num_ware * (c_num + 1)) / num_node);
	}
	o_carrier_id = RandomNumber(1, 10);

	clk1 = clock_gettime(CLOCK_MONOTONIC, &tbuf1);
	for (i = 0; i < MAX_RETRY; i++) {
		ret = delivery(t_num, arg, w_id, o_carrier_id);
		clk2 = clock_gettime(CLOCK_MONOTONIC, &tbuf2);

		if (ret) {
			update_on_success(3, arg, &tbuf1, &tbuf2);
			return (1); /* end */
		} else {
			if (counting_on) {
				inc_retry(3, arg);
			}
		}
	}

	if (counting_on) {
		inc_failure(3, arg);
	}

	return (0);
}

/*
 * prepare data and execute the stock level transaction
 */
static int do_slev(int t_num, thread_arg *arg)
{
	int c_num;
	int i, ret;
	clock_t clk1, clk2;
	double rt;
	struct timespec tbuf1;
	struct timespec tbuf2;
	int w_id, d_id, level;

	if (num_node == 0) {
		w_id = RandomNumber(1, num_ware);
	} else {
		c_num = ((num_node * t_num) / num_conn); /* drop moduls */
		w_id = RandomNumber(1 + (num_ware * c_num) / num_node,
				    (num_ware * (c_num + 1)) / num_node);
	}
	d_id = RandomNumber(1, DIST_PER_WARE);
	level = RandomNumber(10, 20);

	clk1 = clock_gettime(CLOCK_MONOTONIC, &tbuf1);
	for (i = 0; i < MAX_RETRY; i++) {
		ret = slev(t_num, arg, w_id, d_id, level);
		clk2 = clock_gettime(CLOCK_MONOTONIC, &tbuf2);

		if (ret) {
			update_on_success(4, arg, &tbuf1, &tbuf2);
			return (1); /* end */
		} else {
			if (counting_on) {
				inc_retry(4, arg);
			}
		}
	}

	if (counting_on) {
		inc_failure(4, arg);
	}

	return (0);
}

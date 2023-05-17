#pragma once

#include <pthread.h>
#include <fcntl.h>
#include <time.h>

#include <sqlite3.h>


enum tx_type {
	TX_NEWORD,
	TX_PAYMENT,
	TX_ORDSTAT,
	TX_DELIVERY,
	TX_SLEV,
	TX_NUMS
};


typedef struct {
	int success;
	int late;
	int retry;
	int failure;
} tx_stat_t;

typedef struct {
	tx_stat_t stat[TX_NUMS];
} all_tx_stat_t;

extern all_tx_stat_t g_stats;
extern all_tx_stat_t* stats_per_thread;

typedef struct {
	int number;
	pthread_t pth;
	all_tx_stat_t stats;
	sqlite3 *ctx;
	sqlite3_stmt **stmt;
} thread_arg;

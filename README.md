This is the tpcc-sqlite. It's possibly adopted from the tpcc for mysql. 

1. Build binaries
   * `make`

2. Load data
   * create database and tables
     `sqlite3 tpcc1000.db < schema2/create_table.sql`
   * create indexes and FK ( this step can be done after loading data)
     `sqlite3 tpcc1000.db < schema2/add_fkey_idx.sql`
   * populate data
     `./tpcc_load -f tpcc1000.db`

3. Start benchmark
   * `./tpcc_start -f tpcc1000.db -w1000 -c32 -r10 -l10800`
   *            |dbname| |WAREHOUSES| |CONNECTIONS| |WARMUP TIME| |BENCHMARK TIME|
   * ref. tpcc_start --help for all options 

Output
===================================

Most of the below is obsolete, check the code.

With the defined interval (-i option), the tool will produce the following output:
```
  10, trx: 12920, 95%: 9.483, 99%: 18.738, max_rt: 213.169, 12919|98.778, 1292|101.096, 1293|443.955, 1293|670.842
  20, trx: 12666, 95%: 7.074, 99%: 15.578, max_rt: 53.733, 12668|50.420, 1267|35.846, 1266|58.292, 1267|37.421
  30, trx: 13269, 95%: 6.806, 99%: 13.126, max_rt: 41.425, 13267|27.968, 1327|32.242, 1327|40.529, 1327|29.580
  40, trx: 12721, 95%: 7.265, 99%: 15.223, max_rt: 60.368, 12721|42.837, 1271|34.567, 1272|64.284, 1272|22.947
  50, trx: 12573, 95%: 7.185, 99%: 14.624, max_rt: 48.607, 12573|45.345, 1258|41.104, 1258|54.022, 1257|26.626
```

Where: 
* 10 - the seconds from the start of the benchmark
* trx: 12920 - New Order transactions executed during the gived interval (in this case, for the previous 10 sec). Basically this is the throughput per interval. The more the better
* 95%: 9.483: - The 95% Response time of New Order transactions per given interval. In this case it is 9.483 sec
* 99%: 18.738: - The 99% Response time of New Order transactions per given interval. In this case it is 18.738 sec
* max_rt: 213.169: - The Max Response time of New Order transactions per given interval. In this case it is 213.169 sec
* the rest: `12919|98.778, 1292|101.096, 1293|443.955, 1293|670.842` is throughput and max response time for the other kind of transactions and can be ignored




all:
	make -C src

prepare:
	sqlite3 a.db < schema2/create_table.sql

load:
	./tpcc_load -w 1 -m 10 -n 10 -f a.db
run:
	./tpcc_start -w 1 -c 1 -f a.db

clean:
	make -C src clean

clean-db:
	rm -f *.db *.db-shm *.db-wal

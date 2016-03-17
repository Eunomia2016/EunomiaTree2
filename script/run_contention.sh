#!/bin/bash
for warehouse in 10 5 4 2 1
do
	echo "Threads = 10 Warehouses = $warehouse"
	./simple_run.sh 10 $warehouse &> temp.$warehouse
	cat temp.$warehouse | grep "runtime"
	cat temp.$warehouse | grep "agg_nosync_mixed_throughput"
	cat temp.$warehouse | grep "total_abort_num"
	cat temp.$warehouse | grep "DBTX"
	cat temp.$warehouse | grep "ORLI"
	cat temp.$warehouse | grep "STOC"
	#rm temp.$warehouse
done

#!/bin/bash
for thread in 1 2 4 8 12 16 19
do
	echo "Threads = $thread Warehouses = 1"
	./simple_run.sh $thread 1 &> thread.$thread.csv
	#./simple_run_fix_workload.sh $thread 1 &> thread.$thread 
	cat thread.$thread.csv | grep "runtime"
	cat thread.$thread.csv | grep "reason"
	cat thread.$thread.csv | grep "phase"
	cat thread.$thread.csv | grep "agg_nosync_mixed_throughput"
	cat thread.$thread.csv | grep "total_abort_num"
	cat thread.$thread.csv | grep "DBTX"
	cat thread.$thread.csv | grep "ORLI"
	cat thread.$thread.csv | grep "ITEM"
	cat thread.$thread.csv | grep "STOC"
	cat thread.$thread.csv | grep "spec_time"
	cat thread.$thread.csv | grep "spec_hit"
	#rm thread.$thread.csv
done

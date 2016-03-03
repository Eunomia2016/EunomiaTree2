#!/bin/bash
for thread in 1 2 4 8 12 16 18
do
	echo "Threads = $thread Warehouses = 1"
	#./simple_run.sh $thread 1 &> thread.$thread
	./simple_run_fix_workload.sh $thread 1 &> thread.$thread 
	#./simple_run.sh $thread 1 &> temp.$thread
	cat thread.$thread | grep "runtime"
	cat thread.$thread | grep "agg_nosync_mixed_throughput"
	cat thread.$thread | grep "total_abort_num"
	cat thread.$thread | grep "DBTX"
	cat thread.$thread | grep "ORLI"
	cat thread.$thread | grep "ITEM"
	cat thread.$thread | grep "STOC"
	#rm thread.$thread
done

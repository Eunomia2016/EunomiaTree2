#!/bin/bash
if [ $# != 1 ]; then
	echo "./simple_run.sh <num_threads>"
	exit 1
fi

num_threads=$1
total_txns=2000000
#op_per_thread=$(expr $total_txns / $num_threads)
op_per_thread=100000
scale_factor=$(expr $num_threads / 1 )
echo "../dbtest --bench tpcc --num-threads $num_threads --ops-per-worker $op_per_thread --retry-aborted-transactions --verbose --txn-flags 1 --scale-factor $scale_factor"
#pin -t $PIN_ROOT/source/tools/ManualExamples/obj-intel64/inscount0.so -- ../dbtest --bench tpcc --num-threads $num_threads --ops-per-worker $m --retry-aborted-transactions --verbose
../dbtest --bench tpcc --num-threads $num_threads --ops-per-worker $op_per_thread --retry-aborted-transactions --verbose --txn-flags 1 --scale-factor $scale_factor

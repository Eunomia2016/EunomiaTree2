#!/bin/bash
if [ $# != 1 ]; then
	echo "./simple_run.sh <num_threads>"
	exit 1
fi

num_threads=$1
m=$(expr 1000000 / $num_threads)
echo "../dbtest --bench tpcc --num-threads $num_threads --ops-per-worker $m --retry-aborted-transactions --verbose"
../dbtest --bench tpcc --num-threads $num_threads --ops-per-worker $m --retry-aborted-transactions --verbose

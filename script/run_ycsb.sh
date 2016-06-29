#!/bin/bash
if [ $# != 4 ]; then
	echo "./run_ycsb.sh [thread_num] [type] [read_rate] [dist_type]"
	echo "dist_type: (1) Sequential Dist (2) Uniform Dist (3) Normal Dist (4)Cauchy Dist"
	exit 0
fi
../ycsb_test --benchmark=mix --threads=$1 --euno=$2 --read-rate=$3 --func=$4

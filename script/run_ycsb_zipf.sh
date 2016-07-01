#!/bin/bash
for thr in 1 2 4 8 12 16 20
do
	for theta in 0.5 0.7 0.9 0.99 
	do
		echo "./run_ycsb.sh $thr 0 0.2 4 12 $theta"
		./run_ycsb.sh $thr 0 0.2 4 12 $theta | tee ycsb_log/zipf/ycsb_zipf_${thr}_${theta}
	done
done

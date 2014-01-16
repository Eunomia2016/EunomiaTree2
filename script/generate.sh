for i in 1 2 4 6 8
do
	for j in origin padding dummy opt
	do
		echo "$i $j," >> nperf.csv
		python get_th.py per_${j}_${i}_  3  >> nperf.csv
	done
	
	#for j in profile padding
	#do
	#	python get_pth.py nper_${j}_${i}_ 3 >> nprof.csv
	#done
done

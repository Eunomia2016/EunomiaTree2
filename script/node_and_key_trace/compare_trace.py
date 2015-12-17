#!/usr/bin/python
import re
file_name = "2_traces"
d = dict()
pattern = r"\[(\d+)\]"

trace_file = open(file_name, 'r')

lines = trace_file.readlines()
for line in lines:
	match = re.search(pattern, line)
	if match:
		tid = int(match.group(1))
		if tid in d:
			d[tid] += 1
		else:
			d[tid] = 0
print d

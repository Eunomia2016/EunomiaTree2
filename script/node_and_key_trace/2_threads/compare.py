#!/usr/bin/python
import re

pattern = r"\[([A-Z]{3,5})\] node = (\d+) key = (\d+)"

file_name_list = ['worker_0', 'worker_1']

file_0 = open(file_name_list[0],'r')
file_1 = open(file_name_list[1],'r')

lines_0 = file_0.readlines()
lines_1 = file_1.readlines()

node_dup = 0
key_dup = 0

for line_0 in lines_0:
	match_0 = re.search(pattern, line_0)
	if match_0:
		mode_0 = match_0.group(1)
		node_0 = int(match_0.group(2))
		key_0 = int(match_0.group(3))
		for line_1 in lines_1:
			match_1 = re.search(pattern, line_1)
			if match_1:
				mode_1 = match_1.group(1)
				node_1 = int(match_1.group(2))
				key_1 = int(match_1.group(3))
				if node_0 == node_1 and (not (mode_0 == "GET" and mode_1 == "GET")):
					#print node_0, node_1
					node_dup += 1
				if key_0 == key_1 and (not (mode_0 == "GET" and mode_1 == "GET")):
					#print key_0, key_1
					key_dup += 1

print node_dup, key_dup

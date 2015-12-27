#!/usr/bin/python
import re

pattern = r"\[\d+\] ([A-Z]{3}) tableid = \s*(\d+) key = \s*(\d+)"
file_name = "4_threads"
GET = 0
ADD = 1
UPD = 2
DEL = 3
trace_file = open(file_name, 'r')
lines = trace_file.readlines()
table_map = [{} for i in range(0,11)]
for line in lines:
	match = re.search(pattern, line);
	if match:
		op_type = match.group(1)
		tableid = int(match.group(2))
		key = int(match.group(3))
		table = table_map[tableid]
		if not key in table:
			if op_type == "GET":
				table[key] = [1,0,0,0]
			elif op_type == "ADD":
				table[key] = [0,1,0,0]
			elif op_type == "UPD":
				table[key] = [0,0,1,0]
			elif op_type == "DEL":
				table[key] = [0,0,0,1]
		else:
			if op_type == "GET":
				table[key][GET] += 1
			elif op_type == "ADD":
				table[key][ADD] += 1
			if op_type == "UPD":
				table[key][UPD] += 1
			if op_type == "DEL":
				table[key][DEL] += 1

total_keys = 0
for table in table_map:
	total_keys += len(table)
print total_keys
	#key_num = 0
	#GET_num = 0
	#ADD_num = 0
	#UPD_num = 0
	#DEL_num = 0
	#for k in table:
		#GET_num += table[k][GET]
		#ADD_num += table[k][ADD]
		#UPD_num += table[k][UPD]
		#DEL_num += table[k][DEL]
	#read_only = 0
	#access_once = 0
	#access_total = 0
	#for k in table:
		#if table[k][GET] != 0 and table[k][UPD] == 0 and table[k][ADD] == 0 and table[k][DEL] == 0:
			#read_only += 1
		#access_total+=sum(table[k])
		#if sum(table[k]) == 1:
			#access_once += 1
	#print [GET_num, ADD_num, UPD_num, DEL_num]



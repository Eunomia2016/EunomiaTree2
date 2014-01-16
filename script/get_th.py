import re
import sys

def get_th(inpat, number):
	pt_re = re.compile("(\d+) (\d+) ([\d\.]+)")
	ab_re = re.compile(" ([\d,]+) r4c9")
	no_re = re.compile("([\d,]+) r8c9")
	co_re = re.compile("([\d,]+) r2c9")
	al_re = re.compile("([\d,]+) r1c9")
	th = 0; pth = 0;abs = 0; noc = 0; coc = 0; all = 0
	
	for i in range(1, number+1):
		infile = inpat+str(i)
		indata = enumerate(open(infile))
	
		for (lno, line) in indata:
			if pt_re.search(line):
				temp = pt_re.search(line)
				th = th + int(temp.group(1))
				pth = pth + int(temp.group(2))
			elif ab_re.search(line):
				temp = ab_re.search(line)
				abs = abs + int(temp.group(1).replace(',',''))
			elif no_re.search(line):
				temp = no_re.search(line)
				noc = noc + int(temp.group(1).replace(',',''))
			elif co_re.search(line):
				temp = co_re.search(line)
				coc = coc + int(temp.group(1).replace(',',''))
			elif al_re.search(line):
				temp = al_re.search(line)
				all = all + int(temp.group(1).replace(',',''))
			
	print "Throughput, "+str(th/number) + ", #Abort , "+str(abs/number)+", Memory Abort , "\
		+str(noc/number) + ", Commit , "+str(coc/number)+", RTM Begin ,  "+str(all/number)
			

if __name__ == "__main__":
	get_th(sys.argv[1], int(sys.argv[2]))

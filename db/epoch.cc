#include "port/atomic.h"
#include "epoch.h"
#include <assert.h>
#include <stdio.h>

__thread int Epoch::tid = 0;

Epoch::Epoch(int thrs) {

	thrs_num = thrs;
	counters = new uint64_t[thrs];
	for(int i = 0; i < thrs_num; i++)
		counters[i] = 0;
}

Epoch::Epoch(int thrs, uint64_t* cs) {
	thrs_num = thrs;
	counters = cs;
}

Epoch::~Epoch() {

	delete[] counters;
	
}

void Epoch::setTID(int i) {
	assert(tid < thrs_num);
	tid = i;	
}

Epoch* Epoch::getEpoch() {
	
	uint64_t * cs = new uint64_t[thrs_num];

	for(int i = 0; i < thrs_num; i++)
		cs[i] = counters[i];
	
	return new Epoch(thrs_num, cs);
}

//update current's thread's epoch number
void Epoch::updateEpoch()
{
	assert(tid < thrs_num);
	counters[tid]++;
	//atomic_inc64(&counters[tid]);
}

//Compare with another epoch: 1: this > e  0: this == e   < 0: this < e
int Epoch::Compare(Epoch* e)
{
	int res  = 0;
	int tmp = 0;
	for(int i = 0; i < thrs_num; i++) {		
		 if (counters[i] == e->counters[i]) {
		
			return 0;
			
		} else if (counters[i] < e->counters[i]) {
			
			res++;
			
		} 

	}
	
	if(res == thrs_num) {
		return -1;
	} else if(res == 0) {
		return 1;
	}

	assert(0);
}


void Epoch::Print()
{
	printf("Epoch[%d] ", tid);
	for(int i = 0; i < thrs_num; i++) {
		printf("[%d]: %d ", i, counters[i]);
	}
	printf("\n");
}


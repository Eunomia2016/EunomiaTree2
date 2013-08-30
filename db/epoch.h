#ifndef EPOCH_H
#define EPOCH_H

#include <stdint.h>

class Epoch {

static __thread int tid;

public:
	uint64_t *counters;
	int thrs_num;
	

	Epoch(int thrs);
	Epoch(int thrs, uint64_t* cs);
	
	~Epoch();

	void setTID(int i);
	
	//Get the current epoch copy
	Epoch* getEpoch();

	//update current's thread's epoch number
	void updateEpoch(int thr);

	//Compare with another epoch: 1: this > e  0: this == e   -1: this < e
	int Compare(Epoch* e);
};


#endif
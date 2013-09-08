#ifndef RMQueue_H
#define RMQueue_H

#include "db/epoch.h"
#include <stdint.h>
#include <stdio.h>
#include <assert.h>

#define RMTEST 1
class RMQueue {

struct RMElement {
	Epoch* epoch;
	uint64_t** gcarray;
	int len;
	

	RMElement(Epoch* e, uint64_t** arr, int l) 
	{
		epoch = e;
		gcarray = arr;
		len = l;
	}

	~RMElement() 
	{
		delete epoch;

		for(int i; i < len; i++) {
			if(gcarray[i] != NULL) {
				//printf("Free %lx\n", gcarray[i]);
				delete gcarray[i];
			}

		}

		delete[] gcarray;
	}
};

private:
	int qsize;
	int head;
	int tail;
	RMElement** queue;
	int elems;
	
public:
	RMQueue();
	
	~RMQueue();
	
	void AddRMElement(Epoch* e, uint64_t** arr, int len);

	void GC(Epoch* current);

	void Print();

	uint64_t need_del;
	uint64_t actual_del;
	
};


#endif

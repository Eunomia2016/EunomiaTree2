#ifndef GCQUEUE_H
#define GCQUEUE_H

#include "db/epoch.h"
#include <stdint.h>
#include <stdio.h>
#include <assert.h>

#define GCTEST 1
class GCQueue {

struct GCElement {
	Epoch* epoch;
	uint64_t** gcarray;
	int len;
	

	GCElement(Epoch* e, uint64_t** arr, int l) 
	{
		epoch = e;
		gcarray = arr;
		len = l;
	}

	~GCElement() 
	{
		delete epoch;

		for(int i; i < len; i++) {
			if(gcarray[i] != NULL) {
		//		printf("Free %lx\n", gcarray[i]);
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
	GCElement** queue;
	int elems;
	
public:
	GCQueue();
	
	~GCQueue();
	
	void AddGCElement(Epoch* e, uint64_t** arr, int len);

	void GC(Epoch* current);

	void Print();

	uint64_t need_del;
	uint64_t actual_del;
	
};


#endif

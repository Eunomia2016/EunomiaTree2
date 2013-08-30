#ifndef GCQUEUE_H
#define GCQUEUE_H

#include "db/epoch.h"
#include <stdint.h>


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

		for(int i; i < len; i++)
			delete gcarray[i];
		
		delete[] gcarray;
	}
};

private:
	int qsize;
	int head;
	int tail;
	GCElement** queue;
	
	
public:
	GCQueue();
	
	~GCQueue();
	
	void AddGCElement(Epoch* e, uint64_t** arr, int len);

	void GC(Epoch* current);
};


#endif
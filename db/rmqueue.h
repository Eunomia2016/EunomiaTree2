#ifndef RMQueue_H
#define RMQueue_H

#include "db/epoch.h"
#include <stdint.h>
#include <stdio.h>
#include <assert.h>
#include "memstore/memstore.h"
#include "db/dbtables.h"

#define RMTEST 1
class RMQueue {

struct RMElement {
	int tableid;
	uint64_t key;
	Memstore::MemNode* node;
	

	RMElement(int t, uint64_t k, Memstore::MemNode* mn) 
	{
		tableid = t;
		key = k;
		node = mn;
	}

	~RMElement() 
	{
		if(node->value != (uint64_t *)NULL 
			& node->value != (uint64_t *)1
			& node->value != (uint64_t *)2 )
			delete node->value;
		
		delete node;
	}
};


struct RMArray {
	Epoch* epoch;
	uint64_t** rmarray;
	int len;
	

	RMArray(Epoch* e, uint64_t** arr, int l) 
	{
		epoch = e;
		rmarray = arr;
		len = l;
	}

	~RMArray() 
	{
		delete epoch;

		for(int i; i < len; i++) {
			if(rmarray[i] != NULL) {
				//printf("Free %lx\n", gcarray[i]);
				delete rmarray[i];
			}

		}

		delete[] rmarray;
	}
};

private:
	int qsize;
	int head;
	int tail;
	RMArray** queue;
	int elems;
	leveldb::DBTables *store;
	
public:
	RMQueue(leveldb::DBTables *st);
	
	~RMQueue();
	
	void AddRMArray(Epoch* e, uint64_t** arr, int len);

	void Remove(Epoch* current);

	void Print();

	uint64_t need_del;
	uint64_t actual_del;
	
};


#endif

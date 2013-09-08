#ifndef DELBuffer_H
#define DELBuffer_H

#include <stdint.h>
#include <stdio.h>
#include <assert.h>
#include "memstore/memstore.h"

class DELBuffer {


struct DELElement {
	int tableid;
	uint64_t key;
	Memstore::MemNode* node;
	bool delay;
};



private:
	int qsize;
	DELElement* queue;
	
public:
	int elems;
	int delayNum;
	
	DELBuffer();
	
	~DELBuffer();

	void Reset();
	
	void Add(int tableid, uint64_t key, Memstore::MemNode* n, bool delay);

	uint64_t** getGCNodes();
	
	uint64_t** getDelayNodes();

	
};

#endif

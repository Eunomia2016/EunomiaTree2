#include "port/atomic.h"
#include "delset.h"
#include "rmqueue.h"
#include <assert.h>
#include <stdio.h>
#include <stdlib.h>
#include "util/rtm.h"


DELSet::DELSet()
{
	//default size 64;
	qsize = 64;
	queue = new DELElement[qsize];
	elems = 0;
	delayNum = 0;
}
	
DELSet::~DELSet()
{
	delete[] queue;
}

void DELSet::Reset()
{
	elems = 0;
	delayNum = 0;
}

void DELSet::Add(int tableid, uint64_t key, Memstore::MemNode* n, bool delay)
{
	queue[elems].tableid = tableid;
	queue[elems].key = key;
	queue[elems].node = n;
	queue[elems].delay = delay;

	if(delay)
		delayNum++;

	elems++;

	if(elems == qsize) {
		printf("Error Del Buffer Overflow!!!\n");
		exit(1);
	}
}

uint64_t** DELSet::getGCNodes()
{
	int len = elems - delayNum;
	if (0 == len)
		return NULL;
	
	uint64_t **res = new uint64_t*[len];

	int count = 0;
	
	for(int i = 0; i < elems; i++) {

		if(queue[i].delay)
			continue;
		
		res[count] = (uint64_t *)queue[elems].node;
		count++;
	}

	assert(count == len);
	
	return res;
}
	
uint64_t** DELSet::getDelayNodes()
{
	if (0 == delayNum)
		return NULL;
	
	uint64_t **res = new uint64_t*[delayNum];

	int count = 0;
	
	for(int i = 0; i < elems; i++) {

		if(queue[i].delay) {
			res[count] = (uint64_t *)new leveldb::RMQueue::RMElement(
				queue[i].tableid, queue[i].key, queue[i].node);
			count++;
		}
	}

	assert(count == delayNum);
	
	return res;
}

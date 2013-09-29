#include "port/atomic.h"
#include "objpool.h"
#include <assert.h>
#include <stdio.h>
#include <stdlib.h>
#include <pthread.h>


OBJPool::OBJPool()
{
	gcnum_ = 0;
	freenum_ = 0;
	gclist_ = NULL;
	freelist_ = NULL;
}
	
OBJPool::~OBJPool()
{
	//TODO: release all objects here
}
	
void OBJPool::AddGCObj(uint64_t* gobj)
{
	Obj* o = reinterpret_cast<Obj *>(gobj);
	o->next = gclist_;
	gclist_ = o;

	gcnum_++;
}

uint64_t* OBJPool::GetFreeObj()
{
	if(0 == freenum_)
		return NULL;

	assert(freenum_ > 0);
	
	uint64_t* r = reinterpret_cast<uint64_t *>(freelist_);
	
	freelist_ = freelist_->next;
	freenum_--;
	
	return r;
}

void OBJPool::GC()
{
	//TODO: Put the objects into 
	while (NULL != gclist_) {
		uint64_t * o = reinterpret_cast<uint64_t *>(gclist_);
		gclist_ = gclist_->next;
		//printf("[%lx] Delete %lx\n", pthread_self(), o);
		delete o;
	}
}


void OBJPool::Print()
{
	printf("OBJPool Free Object Number %d GC Object Number %d\n", freenum_, gcnum_);
}


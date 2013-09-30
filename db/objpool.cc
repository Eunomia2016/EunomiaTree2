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
	gctail_ = NULL;
	
	freelist_ = NULL;

	debug = false;
}
	
OBJPool::~OBJPool()
{
	FreeList();
}
	
void OBJPool::AddGCObj(uint64_t* gobj)
{
	Obj* o = reinterpret_cast<Obj *>(gobj);

	if(NULL == gctail_) {
		gctail_ = o;
		assert(NULL == gclist_);
	}
		
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
	if(gclist_ == NULL) {
		assert(gctail_ == NULL);
		return;
	}
	
	gctail_->next = freelist_;
	freelist_ = gclist_;
	
	freenum_ += gcnum_;
		
	gclist_ = NULL;
	gctail_ = NULL;
	gcnum_ = 0;
}

void OBJPool::FreeList()
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
	Obj* cur = NULL;
	
	printf("==================GC List=======================\n");
	cur = gclist_;
	while(cur != NULL) {
		printf("Cur %lx Next %lx\n", cur, cur->next);
		cur = cur->next;
	}

	printf("==================Free List=======================\n");
	cur = freelist_;
	while(cur != NULL) {
		printf("Cur %lx Next %lx\n", cur, cur->next);
		cur = cur->next;
	}
}


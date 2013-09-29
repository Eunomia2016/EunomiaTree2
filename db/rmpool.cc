#include "port/atomic.h"
#include "rmpool.h"
#include <assert.h>
#include <stdio.h>
#include <stdlib.h>
#include "util/rtm.h"
#include "db/dbtx.h"


namespace leveldb {

RTMProfile *RMPool::rtmProf = NULL;

RMPool::RMPool(DBTables *st)
{
	rmlist_ = NULL;
	elems = 0;
	store = st;
}
	
RMPool::~RMPool()
{
	//TODO
}
	
void RMPool::AddRMObj(int tableid, uint64_t key, uint64_t seq, Memstore::MemNode* node)
{

//	printf("[%lx] AddRMObj Add %lx\n",pthread_self(), node);
	RMObj* o = new RMObj(tableid, key, node, seq);
	o->next = rmlist_;
	rmlist_ = o;
	elems++;
}


void RMPool::RemoveAll()
{
	
	while(rmlist_ != NULL) {

		RMObj* o = rmlist_;
		rmlist_ = rmlist_->next;
		
		bool r = Remove(o);	

		if(r) {
		 	//store->AddDeletedNode((uint64_t *)o->node);
		}

		delete o;
			
	}

	elems = 0;
}

int RMPool::GCElems()
{
	return elems;
}


bool RMPool::Remove(RMObj* o)
{
#if GLOBALOCK
	SpinLockScope spinlock(&DBTX::slock);
#else
	RTMScope rtm(rtmProf);
#endif

	//Check if this node has been modified

	if(o->node->seq == o->seq) {

#if DEBUG_PRINT
	printf("[%ld] RMPool Remove key  %d node %lx seq %ld\n", 
				pthread_self(), o->key, o->node, o->node->seq);
#endif		
		
		//Physically removed
		o->node->value = HAVEREMOVED;
		Memstore::MemNode* n = store->tables[o->tableid]->GetWithDelete(o->key);
		n->seq++;

		if(n != o->node ) {
			printf("Error [%ld] Node %lx key %ld seq %ld Remove Node %lx seq %ld \n", 
						pthread_self(),  o->node, o->key, o->seq, n, n->seq);
			exit(1);
		}
		
		assert(n == o->node);

		return true;
	}

	return false;

}


void RMPool::Print()
{

}

}


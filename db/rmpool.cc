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
		 	store->AddDeletedNode((uint64_t *)o->node);
		}
			
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
	//printf("RMPool Remove %ld %ld\n",mn->node->seq,mn->seq);
	//Check if this node has been modified
	
	if(o->node->value == (uint64_t *)1 && o->node->seq == o->seq) {

		//Physically removed
		o->node->value = (uint64_t *)2;
		Memstore::MemNode* n = store->tables[o->tableid]->GetWithDelete(o->key);
		n->seq++;
		assert(n == o->node);

		return true;
	}

	return false;

}


void RMPool::Print()
{

}

}


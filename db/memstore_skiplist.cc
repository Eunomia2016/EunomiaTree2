#include <assert.h>
#include <stdlib.h>
#include <vector>
#include <malloc.h>
#include "port/port.h"
#include "port/atomic.h"
#include "util/arena.h"
#include "util/random.h"
#include "util/rtm.h"
#include "util/mutexlock.h"
#include "db/dbtx.h"
#include "port/port_posix.h"
#include "db/memstore_skiplist.h"


#define SKIPLISTGLOBALLOCK 0
#define SKIPLISTRTM 1
#define SKIPLISTLOCKFREE 0


namespace leveldb {


__thread Random* MemStoreSkipList::rnd_;
__thread bool MemStoreSkipList::localinit_ = false;


MemStoreSkipList::MemStoreSkipList()
{
    ThreadLocalInit();
	max_height_ = 1;

	head_ = reinterpret_cast<Node*>(malloc(sizeof(Node) + sizeof(void *) * (kMaxHeight - 1)));
 
  	for (int i = 0; i < kMaxHeight; i++) {
    	head_->next_[i] = NULL;
  	}

	snapshot = 1;
}

MemStoreSkipList::~MemStoreSkipList(){}



void MemStoreSkipList::ThreadLocalInit()
{
	if(localinit_ == false) {
		rnd_ = new Random(0xdeadbeef);
		localinit_ = true;
	}

}


MemStoreSkipList::Node* MemStoreSkipList::NewNode(uint64_t key, int height)
{
 
  Node* n = reinterpret_cast<Node*>(malloc(
	  			sizeof(Node) + sizeof(void *) * (height - 1)));

  n->key = key;
  n->counter = 0;
  n->value = NULL;
  n->next_[0] = NULL;
  return n;
}


inline void MemStoreSkipList::FreeNode(Node* n)
{
  free(n); 
}


inline uint32_t MemStoreSkipList::RandomHeight() 
{
  // Increase height with probability 1 in kBranching
  static const unsigned int kBranching = 4;
  uint32_t height = 1;
  while (height < kMaxHeight && ((rnd_->Next() % kBranching) == 0)) {
    height++;
  }
  assert(height > 0);
  assert(height <= kMaxHeight);
  return height;
}


inline MemStoreSkipList::Node* MemStoreSkipList::FindGreaterOrEqual(uint64_t key, Node** prev)
{

  Node* x = head_;
  int level = max_height_ - 1;

  while (true) {
    Node* next = x->next_[level];
	
    if (next != NULL && key > next->key) {
      // Keep searching in this list
      x = next;
    } else {
      if (prev != NULL) 
	  	prev[level] = x;
	  
      if (level == 0) {
        return next;
      } else {
       	level--;
      }
    }
  }
}


void MemStoreSkipList::Put(uint64_t k,uint64_t * val)
{
	
	MemStoreSkipList::Node* n = GetNodeWithInsert(k);
	n->value = val;
	n->counter = snapshot;
	n->seq = 1;
}

MemStoreSkipList::Node* MemStoreSkipList::GetLatestNodeWithInsert(uint64_t key)
{

#if SKIPLISTLOCKFREE
	Node* x = GetNodeWithInsertLockFree(key);
#else
	Node* x = GetNodeWithInsert(key);
#endif

	while(x->next_[0] != NULL && x->next_[0]->key == key)
		x = x->next_[0];

	return x;
}

MemStoreSkipList::Node* MemStoreSkipList::GetNodeWithInsert(uint64_t key)
{

  
  Node* preds[kMaxHeight];
  int height = RandomHeight();

  Node* newn = NewNode(key, height);
  newn->counter = snapshot;
  
  Node* x;

  {

	#if SKIPLISTGLOBALLOCK
		MutexLock lock(&DBTX::storemutex);
	#elif SKIPLISTRTM
		RTMScope rtm(NULL);
	#endif

    //find the prevs and succs
    x = FindGreaterOrEqual(key, preds);
  
    if(x != NULL && key == x->key ) {
  	  goto found;
    }
  
     if (height > max_height_){
      for (int i = max_height_; i < height; i++) {
        preds[i] = head_;
      }
      max_height_ = height;
    }
    	
    
    for (int i = 0; i < height; i++) {
  		newn->next_[i] = preds[i]->next_[i];
  		preds[i]->next_[i] = newn;	
    }
    return newn;
	
  }

found:
  FreeNode(newn);
  return x;
}

MemStoreSkipList::Node* MemStoreSkipList::GetNodeWithInsertLockFree(uint64_t key)
{

  
  Node* preds[kMaxHeight];
  Node* succs[kMaxHeight];
  Node* x;

  //find the prevs and succs
  x = FindGreaterOrEqual(key, preds);

  if(x != NULL && key == x->key ) {
	//FIXME: Only search the node at level 1
//	while(x->next_[0] != NULL && x->next_[0]->key == key)
	//	x = x->next_[0];

	return x;
  }

  
  int height = RandomHeight();
	
	uint32_t maxh = 0; 
  
	//We need to update the max_height_ atomically
	while (true) {
	  
	  maxh = max_height_;
	  
	  if (height > maxh) {
	  
		uint32_t oldv = atomic_cmpxchg32((uint32_t*)&max_height_, maxh, height);
		if(oldv == maxh)
		  break;
		
  
	  } else
	  {
		break; 
	  }
	}
	
	for (int i = maxh; i < height; i++) {
		preds[i] = head_;
	}

//	printf("put key %ld height %ld \n", key.k, height);
  x = NewNode(key, height);

  //We initialize any node with current snapshot counter
  x->counter = snapshot;
  
  for (int i = 0; i < height; i++) {
	Node *succ = NULL;

	while(true) {
		
		//We should first get succs[i] which is larger than the key
		succs[i] = preds[i]->next_[i];
		while(succs[i] != NULL && key > succs[i]->key) {
			preds[i] = succs[i];
			succs[i] = preds[i]->next_[i]; 		
		}
		
		if((succs[i]!= NULL) && key == succs[i]->key) {
			assert( i == 0);
			FreeNode(x);
			return succs[i];
		}
		
		x->next_[i] = preds[i]->next_[i];
		
		succ = (Node*)atomic_cmpxchg64((uint64_t*)&preds[i]->next_[i], (uint64_t)succs[i], (uint64_t)x);

		if(succ == succs[i])
		{	
	//		assert(compare_(preds[i]->key, key) < 0);
	//		assert(succs[i] == NULL || compare_(key, succs[i]->key) < 0);
			//printf("Insert %dth Node %lx Addr %lx Height %d\n", gcount, x->key, x, height);
	//		if(tmp != NULL)
		//		printf("	 %dth Node %lx Addr %lx Height %d\n", 41, tmp->key, tmp, height);
			
			/*
			if(succs[i] != NULL)
				printf("%lx ---> %lx ---> %lx\n", preds[i]->key, key, succs[i]->key);
			else
				printf("%lx ---> %lx ---> NULL\n", preds[i]->key, key);
			
			
			if(succs[i] != NULL && preds[i]->key != NULL && succs[i]->key != NULL)
				printf("%ld ---> %ld ---> %ld\n", DecodeFixed32(preds[i]->key), 
				DecodeFixed32(key), DecodeFixed32(succs[i]->key));
			else if(succs[i] == NULL && preds[i]->key != NULL)
				printf("%ld ---> %ld ---> NULL \n", DecodeFixed64(preds[i]->key), 
				DecodeFixed64(key));
			else if(succs[i] == NULL && preds[i]->key == NULL)
				printf("Head ---> %ld ---> NULL \n", DecodeFixed64(key));
			else if(succs[i] != NULL && preds[i]->key == NULL)
				printf("Head  ---> %ld ---> %ld \n",
				DecodeFixed64(key), DecodeFixed64(succs[i]->key));
			*/
			break;
		}
	//	retry++;
		//printf("Retry %d\n", retry);
	 }
	
  }
  //atomic_add64(&retryCount, retry);

  return x;
	
}



void MemStoreSkipList::PrintList(){

	/*

	Node* cur = head_;
		
	while(cur != NULL)
	{
		
		//Key prev = cur->key;
		cur = cur->next_[0];
		if(cur != NULL)
			printf("key %ld value %ld, seq %ld snapshot %d\n", 
			cur->key, cur->value, cur->seq, cur->counter);
	}
	*/	


	printf(" Max Height %d\n", max_height_);

	Node* cur = head_;
	int count = 0;	
		
	for(int i = kMaxHeight - 1; i >= 0; i--) {	
		
		printf(" Check Layer %d\n", i);
		
		Node* cur = head_;
		int count = 0;
		
		
		while(cur != NULL)
		{
			
			//Key prev = cur->key;
			cur = cur->next_[i];
			count++;

			//if( i == 0 && cur != NULL)
				//printf("key %lx\n", cur->key);
		}

		printf(" Layer %d Has %d Elements\n", i, count);
	}

}


}

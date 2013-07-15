#ifndef MEMSTORESKIPLIST_H
#define MEMSTORESKIPLIST_H


#include <assert.h>
#include <stdlib.h>
#include <vector>
#include <malloc.h>
#include "port/port.h"
#include "port/atomic.h"
#include "util/arena.h"
#include "util/random.h"
#include "port/port_posix.h"

namespace leveldb {

class MemStoreSkipList {

 public:

  uint64_t snapshot; // the counter for current snapshot
  
  struct Node
  {
	uint64_t key;
	uint64_t counter;
	uint64_t seq;
	uint64_t* value;
	Node* next_[1];
  };

 public:

  explicit MemStoreSkipList();
  ~MemStoreSkipList();

  //Only for initialization
  void Put(uint64_t k, uint64_t* val);
  
  Node* GetNodeWithInsertLockFree(uint64_t key);
  
  Node* GetLatestNodeWithInsert(uint64_t key);

  Node* GetNodeWithInsert(uint64_t key);

  bool GetValueWithSnapshot(uint64_t key, uint64_t **val, uint64_t counter);
  
  void PrintList();

  static Node* NewNode(uint64_t key, int height);
 
  inline void FreeNode(Node* n);
  
  inline uint32_t RandomHeight();

  
  inline Node* FindGreaterOrEqual(uint64_t key, Node** prev);
  
  void ThreadLocalInit();
  
  private:
  	enum { kMaxHeight = 12 };

	uint32_t max_height_;
	
	Node* head_;
	
	static __thread Random* rnd_;
	static __thread bool localinit_;
  
};

}  // namespace leveldb

#endif

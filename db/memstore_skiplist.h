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
	Node* oldVersions;
	Node* next_[1];
  };


  class Iterator {
   public:
    // Initialize an iterator over the specified list.
    // The returned iterator is not valid.
    explicit Iterator(MemStoreSkipList* list, uint64_t snapshotCounter);

    // Returns true iff the iterator is positioned at a valid node.
    bool Valid();

    // Returns the key at the current position.
    // REQUIRES: Valid()
    uint64_t Key();

	uint64_t* Value();

    // Advances to the next position.
    // REQUIRES: Valid()
    void Next();

    // Advances to the previous position.
    // REQUIRES: Valid()
    void Prev();

    // Advance to the first entry with a key >= target
    void Seek(uint64_t key);

    // Position at the first entry in list.
    // Final state of iterator is Valid() iff list is not empty.
    void SeekToFirst();

    // Position at the last entry in list.
    // Final state of iterator is Valid() iff list is not empty.
    void SeekToLast();

   private:
    MemStoreSkipList* list_;
    Node* node_;
	uint64_t snapshot_;
    // Intentionally copyable
  };

 public:

  explicit MemStoreSkipList();
  ~MemStoreSkipList();

  //Only for initialization
  void Put(uint64_t k, uint64_t* val);
  
  Node* GetNodeWithInsertLockFree(uint64_t key);

  Node* GetNodeWithInsert(uint64_t key);

  Node* GetLatestNodeWithInsert(uint64_t key);

  Node* GetLatestNode(uint64_t key);
  
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

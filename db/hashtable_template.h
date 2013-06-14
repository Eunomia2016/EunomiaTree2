// Copyright (c) 2011 The LevelDB Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file. See the AUTHORS file for names of contributors.
//
// A Cache is an interface that maps keys to values.  It has internal
// synchronization and may be safely accessed concurrently from
// multiple threads.  It may automatically evict entries to make room
// for new entries.  Values have a specified charge against the cache
// capacity.  For example, a cache where the values are variable
// length strings, may use the length of the string as the charge for
// the string.
//
// A builtin cache implementation with a least-recently-used eviction
// policy is provided.  Clients may use their own implementations if
// they want something more sophisticated (like scan-resistance, a
// custom eviction policy, variable cache sizing, etc.)

#ifndef STORAGE_LEVELDB_INCLUDE_HASHTABLETEMPLATE_H_
#define STORAGE_LEVELDB_INCLUDE_HASHTABLETEMPLATE_H_

#include <stdint.h>
#include <stdio.h>
#include "leveldb/slice.h"
#include <stdlib.h>
#include "util/rtm_arena.h"
#include "util/rtm_arena.h"
#include "port/port.h"
#include "util/mutexlock.h"


namespace leveldb {

class HashFunction {
	template <typename T>
	uint64_t hash(T&){return 0;}
};

template<typename Key, class HashFunction, class Comparator>
class HashTable {
 public:
  
  struct Node 
  {

	  uint64_t seq;
	  uint64_t hash;	  // Hash of key(); used for fast sharding and comparisons
	  Node* next; 
	  Key* key;
  };


  struct Head 
  {
	  Node* h;
	  port::SpinLock* spinlock;
  };

  
  HashTable(HashFunction hf, Comparator cmp);
  virtual ~HashTable();
  bool GetMaxWithHash(uint64_t hash, uint64_t *seq_ptr);
  void UpdateWithHash(uint64_t hash, uint64_t seq);
  
  
  Node* GetNode(Key* k);
  Node* GetNodeWithInsert(Key* k);
  Node* Insert(Key* k, uint64_t seq);
  
  void PrintHashTable();

  HashFunction hashfunc_;
  
  private:
  	
  int length_;
  int elems_;
  Head* list_;

  Comparator compare_;
  
  
  void Resize();
  Node* NewNode(Key* key);

 };


template<typename Key, class HashFunction, class Comparator>
HashTable<Key, HashFunction, Comparator>::HashTable(HashFunction hf, Comparator cmp)
    : compare_(cmp),
      hashfunc_(hf),
      length_(0), 
      elems_(0), 
      list_(NULL) {
      
	Resize();
	
}

template<typename Key, class HashFunction, class Comparator>
HashTable<Key, HashFunction, Comparator>::~HashTable() {

	for (uint32_t i = 0; i < length_; i++) {
	
	  delete list_[i].spinlock;
	}
	
	delete[] list_;	
}

template<typename Key, class HashFunction, class Comparator>
void HashTable<Key, HashFunction, Comparator>::Resize() 
{
	uint32_t new_length = 16; //16M
	
	while (new_length < elems_) {
	  new_length *= 2;
	}
	
	Head* new_list = new Head[new_length];
	for (uint32_t i = 0; i < new_length; i++) {

	  new_list[i].spinlock = new port::SpinLock();
	  new_list[i].h = NULL;
	}
	
	uint32_t count = 0;
	for (uint32_t i = 0; i < length_; i++) {
	  
	  Node* h = list_[i].h;
	  while (h != NULL) {
		Node* next = h->next;
		uint32_t hash = h->hash;
		Head ptr = new_list[hash & (new_length - 1)];
		h->next = ptr.h;
		ptr.h = h;
		h = next;
		count++;
	  }

	  delete list_[i].spinlock;
	}
	assert(elems_ == count);
	  
	delete[] list_;
	list_ = new_list;
	length_ = new_length;
  }


template<typename Key, class HashFunction, class Comparator>
bool HashTable<Key, HashFunction, Comparator>::GetMaxWithHash(uint64_t hash, uint64_t *seq_ptr)
{
	uint64_t max = 0;
	Head slot = list_[hash & (length_ - 1)];

	MutexSpinLock(slot.spinlock);
	Node* ptr = slot.h;
	
    while (ptr != NULL) {

	  if(ptr->hash == hash) {

		if(max < ptr->seq)
			max = ptr->seq;
	  }
      ptr = ptr->next;
    }

	if(max == 0)
		return false;

	*seq_ptr = max;

	return true;
}

template<typename Key, class HashFunction, class Comparator>
void HashTable<Key, HashFunction, Comparator>::UpdateWithHash(uint64_t hash, uint64_t seq)
{
	uint64_t max = 0;
	
	Head slot = list_[hash & (length_ - 1)];

	MutexSpinLock(slot.spinlock);
	Node* ptr = slot.h;
	
    while (ptr != NULL) {

	  if(ptr->hash == hash) 
		ptr->seq = seq;
	  
      ptr = ptr->next;
    }
}


template<typename Key, class HashFunction, class Comparator>
typename HashTable<Key, HashFunction, Comparator>::Node*
HashTable<Key, HashFunction, Comparator>::GetNode(Key* k) 
{

	uint64_t hash = hashfunc_.hash(*k);
	
	Head slot = list_[hash & (length_ - 1)];
	
	MutexSpinLock(slot.spinlock);
	Node* ptr = slot.h;
	
    while (ptr != NULL &&
           (ptr->hash != hash || compare_(*ptr->key, *k) != 0)) {
      ptr = ptr->next;
    }
    return ptr;
	
}


template<typename Key, class HashFunction, class Comparator>
typename HashTable<Key, HashFunction, Comparator>::Node* 
HashTable<Key, HashFunction, Comparator>::GetNodeWithInsert(Key* k)
{

	uint64_t hash = hashfunc_.hash(*k);
	Head *slot = &list_[hash & (length_ - 1)];

	MutexSpinLock(slot->spinlock);

	Node* ptr = slot->h;

	
    while (ptr != NULL &&
           (ptr->hash != hash || compare_(*ptr->key, *k) != 0)) {
  
      ptr = ptr->next;
    }

	if(ptr == NULL) {
		//insert an empty node
		ptr = NewNode(k);
		ptr->seq = 0;
		ptr->next = NULL;
		ptr->hash = hash;

		ptr->next = slot->h;
    	slot->h = ptr;

	}
    return ptr;
	
}


template<typename Key, class HashFunction, class Comparator>
typename HashTable<Key, HashFunction, Comparator>::Node* 
HashTable<Key, HashFunction, Comparator>::Insert(Key* k, uint64_t seq) 
{

	uint64_t hash = hashfunc_.hash(*k);
	Head *slot = &list_[hash & (length_ - 1)];
	Node* ptr = NewNode(k);
	
	MutexSpinLock(slot.spinlock);

	ptr->seq = 0;
	ptr->hash = hash;
	ptr->next = slot->h;
    slot->h = ptr;
	
    return ptr;
	
}





template<typename Key, class HashFunction, class Comparator>
void HashTable<Key, HashFunction, Comparator>::PrintHashTable() 
{
	int count = 0;
    int i = 0;
	
    for(; i < length_; i++) {
		
		printf("slot [%d] : ", i);
		Head slot = list_[i];

		MutexSpinLock(slot.spinlock);
		Node* ptr = slot.h;
		
        while (ptr != NULL) {
			count++;
	   		printf("Key %ld Hash: %ld, Seq: %ld  ",*ptr->key, ptr->hash, ptr->seq);
           ptr = ptr->next;
        }
		printf("\n");
    }

	printf(" Hash Table Elements %d\n", count);
}



template<typename Key, class HashFunction, class Comparator>
typename HashTable<Key, HashFunction, Comparator>::Node* 
HashTable<Key, HashFunction, Comparator>::NewNode(Key* k)
{
	Node* e = new Node();
	e->key = k;
	
    return e;
}



}  // namespace leveldb

#endif  // STORAGE_LEVELDB_UTIL_CACHE_H_

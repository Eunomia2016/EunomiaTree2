// Copyright (c) 2011 The LevelDB Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file. See the AUTHORS file for names of contributors.

#include <assert.h>
#include <stdio.h>
#include <stdlib.h>

#include "db/hashtable.h"
#include "port/port.h"
#include "util/hash.h"
#include "util/mutexlock.h"

namespace leveldb {

HashTable::HashTable() : length_(0), elems_(0), list_(NULL) {
	arena_ = new RTMArena();
    Resize();
}

HashTable::~HashTable() {
	delete arena_;

	for (uint32_t i = 0; i < length_; i++) {
	
	  delete list_[i].spinlock;
	}
	
	delete[] list_;	
}


void HashTable::Resize() 
{
	uint32_t new_length = 16384000; //16M
	
	while (new_length < elems_) {
	  new_length *= 2;
	}

	//seqs = reinterpret_cast<SeqNumber*>
		(arena_->AllocateAligned(new_length * sizeof(SeqNumber)));
	//seqIndex = 0;
	
	Head* new_list = new Head[new_length];
	for (uint32_t i = 0; i < new_length; i++) {

	  new_list[i].spinlock = new port::SpinLock();

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

uint64_t HashTable::HashSlice(const Slice& s)
{
	return Hash(s.data(), s.size(), 0);
}

bool HashTable::GetMaxWithHash(uint64_t hash, uint64_t *seq_ptr)
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

void HashTable::UpdateWithHash(uint64_t hash, uint64_t seq)
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

HashTable::Node* HashTable::GetNode(const Slice& key) 
{

	uint64_t hash = HashSlice(key);
	Head slot = list_[hash & (length_ - 1)];
	
	MutexSpinLock(slot.spinlock);
	Node* ptr = slot.h;
	
    while (ptr != NULL &&
           (ptr->hash != hash || key != ptr->Getkey())) {
      ptr = ptr->next;
    }
    return ptr;
	
}

HashTable::Node* HashTable::GetNodeWithInsert(const Slice& key) 
{

	uint64_t hash = HashSlice(key);
	Head slot = list_[hash & (length_ - 1)];
	
	MutexSpinLock(slot.spinlock);
	Node* ptr = slot.h;
	
    while (ptr != NULL &&
           (ptr->hash != hash || key != ptr->Getkey())) {
      ptr = ptr->next;
    }

	if(ptr == NULL) {
		//insert an empty node
		ptr = NewNode(key);
		ptr->seq = 0;
		ptr->next = NULL;
		ptr->hash = hash;

		ptr->next = slot.h;
    	slot.h = ptr;
	}
    return ptr;
	
}


HashTable::Node* HashTable::Insert(const Slice& key, uint64_t seq) 
{

	uint64_t hash = HashSlice(key);
	Head slot = list_[hash & (length_ - 1)];
	Node* ptr = NewNode(key);
	
	MutexSpinLock(slot.spinlock);

	
	ptr->seq = 0;
	ptr->next = NULL;
	ptr->hash = HashSlice(key);
	ptr->next = slot.h;
    slot.h = ptr;
	
    return ptr;
	
}





void HashTable::PrintHashTable() 
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
	   		printf("Hash: %ld, Seq: %ld  ",  ptr->hash, ptr->seq);
           ptr = ptr->next;
        }
		printf("\n");
    }

	printf(" Hash Table Elements %d\n", count);
}



HashTable::Node* HashTable::NewNode(const Slice & key)
{
	//Node* e = reinterpret_cast<Node*>(arena_->AllocateAligned(
      //sizeof(Node) + (key.size() - 1)));

	Node* e = reinterpret_cast<Node*>(malloc(sizeof(Node) + (key.size() - 1)));

	e->key_length = key.size();
	memcpy(e->key_contents, key.data(), key.size());
	
    return e;
}




}

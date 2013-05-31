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
    Resize();
}

HashTable::~HashTable() {
	//TODO garbage collection
	/*
	int i = 0;
    for(; i < length_; i++) {
        Node** ptr = &list_[i];
        while (*ptr != NULL) {
		   Node* tmp = *ptr;
           ptr = &(*ptr)->next;
		   tmp->Unref();
        }
    }
	delete[] list_;*/
	
}

void HashTable::Resize() 
{
	uint32_t new_length = 16384;
	while (new_length < elems_) {
	  new_length *= 2;
	}
	Node** new_list = new Node*[new_length];
	memset(new_list, 0, sizeof(new_list[0]) * new_length);
	uint32_t count = 0;
	for (uint32_t i = 0; i < length_; i++) {
	  Node* h = list_[i];
	  while (h != NULL) {
		Node* next = h->next;
		Slice key = h->key->Getslice();
		uint32_t hash = h->hash;
		Node** ptr = &new_list[hash & (new_length - 1)];
		h->next = *ptr;
		*ptr = h;
		h = next;
		count++;
	  }
	}
	assert(elems_ == count);
	delete[] list_;
	list_ = new_list;
	length_ = new_length;
  }

bool HashTable::GetMaxWithHash(uint64_t hash, uint64_t *seq_ptr)
{
	uint64_t max = 0;
	Node* ptr = list_[hash & (length_ - 1)];
	
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
	Node* ptr = list_[hash & (length_ - 1)];
	
    while (ptr != NULL) {

	  if(ptr->hash == hash) 
		ptr->seq = seq;
	  
      ptr = ptr->next;
    }
}

HashTable::Node* HashTable::Insert(const Slice& key, uint64_t seq)
{
	Node* e = new Node();
	e->seq = seq;
	e->hash = HashSlice(key);
	
    Data* kp = reinterpret_cast<Data*>(
    	malloc(sizeof(Data)-1 + key.size()));
    kp->length = key.size();
    memcpy(kp->contents, key.data(), key.size());

	e->key = kp;
	
    //printf("Memcp key %s\n", key.ToString().c_str());
    InsertNode(e);
	//e->refs = 1;
    return e;
}


HashTable::Node* HashTable::Remove(const Slice& key, uint32_t hash) 
{
    Node** ptr = FindNode(key, hash);
    Node* result = *ptr;
    if (result != NULL) {
      *ptr = result->next;
      //--elems_;
    }
    return result;

}


bool HashTable::Update(const Slice& key,  uint64_t seq) 
{
    Node** ptr = FindNode(key, HashSlice(key));
    assert(ptr != NULL && *ptr != NULL);
	
    (*ptr)->seq = seq;
	
    return true;
}


bool HashTable::Lookup(const Slice& key, uint64_t *seq_ptr) 
{
    Node** ptr = FindNode(key, HashSlice(key));
    if(ptr == NULL || *ptr == NULL)
	return false;
    *seq_ptr = (*ptr)->seq;
    return true;
}

HashTable::Node* HashTable::GetNode(const Slice& key) 
{
    Node** ptr = FindNode(key, HashSlice(key));
    if(ptr == NULL || *ptr == NULL)
		return NULL;
	
    return *ptr;
}


void HashTable::PrintHashTable() 
{
	int count = 0;
    int i = 0;
    for(; i < length_; i++) {
	printf("slot [%d] : ", i);
        Node** ptr = &list_[i];
        while (*ptr != NULL) {
			count++;
	   printf("Hash: %ld, Seq: %ld  ",  (*ptr)->hash, (*ptr)->seq);
           ptr = &(*ptr)->next;
        }
	printf("\n");
    }

	printf(" Hash Table Elements %d\n", count);
}

HashTable::Iterator::Iterator(const HashTable* htable)
{
	this->htable = htable;
	slotIndex = -1;
	current = NULL;
}

// Advances to the next position
bool HashTable::Iterator::Next()
{
	if(slotIndex >= htable->length_)
		return false;

	if (current == NULL || current->next == NULL) {
		slotIndex++;
		while( slotIndex < htable->length_ 
			&& htable->list_[slotIndex] == NULL)
			slotIndex++;

		//printf("slotIndex %d length_ %d\n", slotIndex, htable->length_);

		if(slotIndex >= htable->length_)
			return false;
	
		current = htable->list_[slotIndex];
		return true;
	} 
	else if (current->next != NULL) {
		current = current->next;
		return true;
	}
	
}

   
HashTable::Node* HashTable::Iterator::Current() 
{
	return current;
}

uint32_t HashTable::HashSlice(const Slice& s) 
{
    return Hash(s.data(), s.size(), 0);
}

HashTable::Node* HashTable::InsertNode(Node* h) 
{
	Node* ptr = list_[h->hash & (length_ - 1)];
	
    h->next = ptr;
    list_[h->hash & (length_ - 1)] = h;
	
    /*
    if (old == NULL) {
      ++elems_;
      if (elems_ > length_) {
        // Since each cache entry is fairly large, we aim for a small
        // average linked list length (<= 1).
        Resize();
      }
    }*/
    return ptr;  
}



HashTable::Node** HashTable::FindNode(const Slice& key, uint32_t hash) 
{
    Node** ptr = &list_[hash & (length_ - 1)];
    while (*ptr != NULL &&
           ((*ptr)->hash != hash || key != (*ptr)->key->Getslice())) {
      ptr = &(*ptr)->next;
    }
    return ptr;
}
}

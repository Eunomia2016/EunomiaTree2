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
}

void HashTable::Resize() 
{
	uint32_t new_length = 4;
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
		Slice key = h->key();
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

bool HashTable::Insert(const Slice& key, void* value,
					   void (*deleter)(const Slice& key, void* value))
{
    Node* e = reinterpret_cast<Node*>(
    	malloc(sizeof(Node)-1 + key.size()));
    e->value = value;
    e->deleter = deleter;
    e->key_length = key.size();
    e->hash = HashSlice(key);
    memcpy(e->key_data, key.data(), key.size());
    //printf("Memcp key %s\n", key.ToString().c_str());
    InsertNode(e);
    return true;
}

bool HashTable::Lookup(const Slice& key, void **vp) 
{
    Node** ptr = FindNode(key, HashSlice(key));
    if(ptr == NULL)
	return false;
    *vp = (*ptr)->value;
    return true;
}

void HashTable::PrintHashTable() 
{
    int i = 0;
    for(; i < length_; i++) {
	printf("slot [%d] : ", i);
        Node** ptr = &list_[i];
        while (*ptr != NULL) {
	   printf("Key: %s , Hash: %d, Value: %d  ", (*ptr)->key_data, (*ptr)->hash, (*ptr)->value);
           ptr = &(*ptr)->next;
        }
	printf("\n");
    }
}

uint32_t HashTable::HashSlice(const Slice& s) 
{
    return Hash(s.data(), s.size(), 0);
}

HashTable::Node* HashTable::InsertNode(Node* h) {
    Node** ptr = FindNode(h->key(), h->hash);
    Node* old = *ptr;
    h->next = (old == NULL ? NULL : old->next);
    *ptr = h;
    if (old == NULL) {
      ++elems_;
      if (elems_ > length_) {
        // Since each cache entry is fairly large, we aim for a small
        // average linked list length (<= 1).
        Resize();
      }
    }
    return old;  
}


HashTable::Node** HashTable::FindNode(const Slice& key, uint32_t hash) 
{
    Node** ptr = &list_[hash & (length_ - 1)];
    while (*ptr != NULL &&
           ((*ptr)->hash != hash || key != (*ptr)->key())) {
      ptr = &(*ptr)->next;
    }
    return ptr;
}

}

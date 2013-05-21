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

#ifndef STORAGE_LEVELDB_INCLUDE_HASHTABLE_H_
#define STORAGE_LEVELDB_INCLUDE_HASHTABLE_H_

#include <stdint.h>
#include "leveldb/slice.h"

namespace leveldb {

class HashTable {
 public:
  HashTable();

  // Destroys all existing entries by calling the "deleter"
  // function that was passed to the constructor.
  virtual ~HashTable();

  
  bool Insert(const Slice& key, void* value,
                         void (*deleter)(const Slice& key, void* value));


  bool Lookup(const Slice& key, void **vp);

  void PrintHashTable();
  private:


  
  struct Node 
  {
  	void* value;
  	void (*deleter)(const Slice&, void* value);
  	Node* next;
  	Node* prev;
  	size_t key_length;
  	uint32_t hash;      // Hash of key(); used for fast sharding and comparisons
  	char key_data[1];   // Beginning of key

	Slice key() const {
		// For cheaper lookups, we allow a temporary Handle object
		// to store a pointer to a key in "value".
	  if (next == this) {
		return *(reinterpret_cast<Slice*>(value));
	  } else {
		return Slice(key_data, key_length);
	  }
	}
};

  uint32_t length_;
  uint32_t elems_;
  Node** list_;

  void Resize();
  uint32_t HashSlice(const Slice& s);
  Node* InsertNode(Node* h);
  Node** FindNode(const Slice& key, uint32_t hash); 
 
  	
};

}  // namespace leveldb

#endif  // STORAGE_LEVELDB_UTIL_CACHE_H_

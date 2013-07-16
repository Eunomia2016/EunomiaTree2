// Copyright (c) 2011 The LevelDB Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file. See the AUTHORS file for names of contributors.

#ifndef DBROTX_H
#define DBROTX_H

#include <string>
#include "leveldb/db.h"
#include "util/rtm.h"

#include "db/dbformat.h"
#include "port/port_posix.h"
#include "util/txprofile.h"
#include "util/spinlock.h"
#include "util/mutexlock.h"
#include "db/memstore_skiplist.h"



namespace leveldb {


class DBROTX {
 public:


	DBROTX (MemStoreSkipList* store);
	~DBROTX();

	void Begin();
	bool Abort();
	bool End();
	
	bool Get(uint64_t key, uint64_t** val);
	
	
	class Iterator {
	 public:
	  // Initialize an iterator over the specified list.
	  // The returned iterator is not valid.
	  explicit Iterator(DBROTX* rotx);
	
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
	  DBROTX* rotx_;
	  MemStoreSkipList::Node* cur_;
	  MemStoreSkipList::Iterator *iter_;
	  // Intentionally copyable
	};
	
public:

	inline bool GetValueOnSnapshot(MemStoreSkipList::Node* n, uint64_t** val);
	
	uint64_t oldsnapshot;
	MemStoreSkipList *txdb_ ;

};

}  // namespace leveldb

#endif  // STORAGE_LEVELDB_DB_MEMTABLE_H_

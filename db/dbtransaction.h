// Copyright (c) 2011 The LevelDB Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file. See the AUTHORS file for names of contributors.

#ifndef STORAGE_LEVELDB_DB_DBTRANSACTION_H_
#define STORAGE_LEVELDB_DB_DBTRANSACTION_H_

#include <string>
#include "leveldb/db.h"
#include "db/dbformat.h"
#include "db/hashtable.h"
#include "db/memtable.h"
#include "port/port_posix.h"



namespace leveldb {

class DBTransaction {
 public:
 	
  explicit DBTransaction();
  ~DBTransaction();

  void Begin();
  bool End();
  void Add(ValueType type, Slice& key, Slice& value);

  bool Get(const Slice& key, std::string* value, Status* s);
  
 private:

	struct WSNode {
		Slice* key;
		Slice* value;
		ValueType type;
		SequenceNumber seq;
	};
	
 	HashTable readset;
	HashTable writeset;

	port::Mutex* storemutex;
	port::Mutex* seqmutex;
	HashTable *latestseq_ ;
	MemTable *memstore_ ;
	
  
};

}  // namespace leveldb

#endif  // STORAGE_LEVELDB_DB_MEMTABLE_H_

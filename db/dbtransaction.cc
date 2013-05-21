// Copyright (c) 2011 The LevelDB Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file. See the AUTHORS file for names of contributors.

#include "db/dbtransaction.h"
#include "db/hashtable.h"
#include "db/dbformat.h"
#include "leveldb/comparator.h"
#include "leveldb/env.h"
#include "leveldb/iterator.h"
#include "util/coding.h"

namespace leveldb {
 	
  DBTransaction::DBTransaction(){}
  
  DBTransaction::~DBTransaction(){}

  void DBTransaction::begin(){}
  void DBTransaction::end(){}
  void DBTransaction::Add(ValueType type,
           const Slice& key,
           const Slice& value)
           { }

  bool DBTransaction::Get(const Slice& key, std::string* value, Status* s)
  {
  		return false;
  }


}  // namespace leveldb

int main()
{
    leveldb::HashTable ht;
    int i = 0;
    for(; i < 100; i++) {
	char key[100];
        snprintf(key, sizeof(key), "%d", i);

	//printf("Insert %s\n", *s);
	ht.Insert(leveldb::Slice(key), NULL,NULL);
    }
    ht.PrintHashTable();
    //printf("helloworld\n");
    return 0;
 }



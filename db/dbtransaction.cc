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
    for(; i < 10; i++) {
	ht.Insert(leveldb::Slice(std::string(i+"k")), NULL,NULL);
    }
    ht.PrintHashTable();
    printf("helloworld\n");
    return 0;
 }



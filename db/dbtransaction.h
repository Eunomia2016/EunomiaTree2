// Copyright (c) 2011 The LevelDB Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file. See the AUTHORS file for names of contributors.

#ifndef STORAGE_LEVELDB_DB_DBTRANSACTION_H_
#define STORAGE_LEVELDB_DB_DBTRANSACTION_H_

#include <string>
#include "leveldb/db.h"
#include "db/dbformat.h"
#include "db/skiplist.h"
#include "util/arena.h"
#include <map>

namespace leveldb {

class DBTransaction {
 public:
 	
  explicit DBTransaction();
  ~DBTransaction();

  void begin();
  void end();
  void Add(ValueType type,
           const Slice& key,
           const Slice& value);

  bool Get(const Slice& key, std::string* value, Status* s);
  
 //private:
  
};

}  // namespace leveldb

#endif  // STORAGE_LEVELDB_DB_MEMTABLE_H_

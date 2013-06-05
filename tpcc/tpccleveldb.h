// Copyright (c) 2011 The LevelDB Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file. See the AUTHORS file for names of contributors.

#include <string>
#include "util/random.h"
#include "db/hashtable.h"
#include "db/memtable.h"
#include "tpccdb.h"

namespace leveldb {

class TPCCLevelDB : public TPCCDB {
 public:

  uint32_t warehouse_num;
  port::Mutex* storemutex;
  HashTable *latestseq_ ;
  MemTable *memstore_ ;	

  TPCCLevelDB(uint32_t w_num, HashTable* ht, MemTable* store, port::Mutex* mutex); 
  
  
  virtual bool newOrder(int32_t warehouse_id, int32_t district_id, int32_t customer_id,
            const std::vector<NewOrderItem>& items, const char* now,
            NewOrderOutput* output, TPCCUndo** undo);
  virtual bool newOrderHome(int32_t warehouse_id, int32_t district_id, int32_t customer_id,
            const std::vector<NewOrderItem>& items, const char* now,
            NewOrderOutput* output, TPCCUndo** undo);
  virtual bool newOrderRemote(int32_t home_warehouse, int32_t remote_warehouse,
            const std::vector<NewOrderItem>& items, std::vector<int32_t>* out_quantities,
            TPCCUndo** undo);
 




  Slice TPCCLevelDB::marshallWarehouseKey(int32_t w_id);
};

}


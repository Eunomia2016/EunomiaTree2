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


  Slice marshallWarehouseKey(int32_t w_id);
  Slice marshallWarehouseValue(const Warehouse& w);
  float getW_TAX(std::string& value);
  Slice marshallDistrictKey(int32_t d_w_id, int32_t d_id);
  Slice marshallDistrictValue(const District& d);
  float getD_TAX(std::string& value);
  int32_t getD_NEXT_O_ID(std::string& value);
  std::string updateD_NEXT_O_ID(std::string& value, int32_t id);
  Slice marshallCustomerKey(int32_t c_w_id, int32_t c_d_id, int32_t c_id);
  Slice marshallCustomerValue(const Customer& c);
  float getC_DISCOUNT(std::string& value);
  char* getC_LAST(std::string& value);
  char* getC_CREDIT(std::string& value);
  Slice marshallOrderKey(int32_t o_w_id, int32_t o_d_id, int32_t o_id);
  Slice marshallOrderValue(Order o);
  Slice marshallNewOrderKey(const NewOrder& no);
  Slice marshallStockKey(int32_t s_w_id, int32_t s_i_id);
  Slice marshallStockValue(Stock s);
  Stock unmarshallStockValue(std::string& value);
  Slice marshallItemkey(int32_t i_id);
  Slice marshallItemValue(Item i);
  Item unmarshallItemValue(std::string& value);
  Slice marshallOrderLineKey(int32_t ol_w_id, int32_t ol_d_id, int32_t ol_o_id, int32_t ol_number);
  Slice marshallOrderLineValue(OrderLine line);
  //char* getS_DATA(std::string& value);
  //char** getS_DIST(std::string& value);
};

}


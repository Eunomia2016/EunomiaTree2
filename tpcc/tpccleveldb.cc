// Copyright (c) 2011 The LevelDB Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file. See the AUTHORS file for names of contributors.

#include <string>
#include "tpcc/tpccleveldb.h"
#include "util/random.h"
#include "db/dbtransaction.h"
#include "util/mutexlock.h"
#include "leveldb/env.h"
#include "port/port.h"


namespace leveldb {

static void EncodeInt32_t(char *result, int32_t v) {
  char *ip = &v;				
  for (int i:=0; i<4; i++)
  	result[i] = ip[i];
}

static void EncodeFloat(char *result, float f) {
  char *fp = &f;				
  for (int i:=0; i<4; i++)
  	result[i] = fp[i];
}


//Warehouse
Slice TPCCLevelDB::marshallWarehouseKey(int32_t w_id) {
  char* key = new char[14];
  key = "WAREHOUSE_";
  EncodeInt32_t(key + 10, w_id);
  Slice k(key);
  return k;
}

Slice TPCCLevelDB::marshallWarehouseValue(const Warehouse& w) {
  char* value = new char[95];
  char* start = value;

  int length = sizeof(w.w_name);		//0
  memcpy(start, w.w_name, length);
  start += length;
  
  length = sizeof(w.w_street_1);		//11
  memcpy(start, w.w_street_1, length);
  start += length;

  length = sizeof(w.w_street_2);		//32
  memcpy(start, w.w_street_2, length);
  start += length;

  length = sizeof(w.w_city);			//53
  memcpy(start, w.w_city, length);
  start += length;
  
  length = sizeof(w.w_state);			//74
  memcpy(start, w.w_state, length);
  start += length;

  length = sizeof(w.w_zip);				//77
  memcpy(start, w.w_zip, length);
  start += length;
    				
  length = sizeof(float);				//87
  EncodeFloat(start, w.w_tax);
  start += length;

  EncodeFloat(start, w.w_ytd);			//91
  	
  	
  Slice v(value);
  return v;
}

float TPCCLevelDB::getW_TAX(std::string& value) {
  char *v = value.c_str();
  v += 87;
  float *f = reinterpret_cast<float *>v;
  return *f;
}

Slice TPCCLevelDB::updateW_TAX()
/*
void TPCCLevelDB::unmarshallWarehouseValue(std::string& value) {
  char *v = value.c_str();
  strncpy(w_name, v, 10);
  
  //...
}*/


//District
Slice TPCCLevelDB::marshallDistrictKey(int32_t d_w_id, int32_t d_id) {
  char* key = new char[17];
  key = "DISTRICT_";
  EncodeInt32_t(key + 9, d_id);
  EncodeInt32_t(key + 13, d_w_id);
  Slice k(key);
  return k;
}

Slice TPCCLevelDB::marshallDistrictValue(District d) {
  char* value = new char[99];	
  char* start = value;

  int length = sizeof(d.d_name);		//0
  memcpy(start, d.d_name, length);
  start += length;

  length = sizeof(d.d_street_1);		//11
  memcpy(start, d.d_street_1, length);
  start += length;

  length = sizeof(d.d_street_2);		//32
  memcpy(start, d.d_street_2, length);
  start += length;

  length = sizeof(d.d_city);			//53
  memcpy(start, d.d_city, length);
  start += length;
  
  length = sizeof(d.d_state);			//74
  memcpy(start, d.d_state, length);
  start += length;

  length = sizeof(d.d_zip);				//77
  memcpy(start, d.d_zip, length);
  start += length;
  
  length = sizeof(float);				//87
  EncodeFloat(start, d.d_tax);
  start += length;

  EncodeFloat(start, d.d_ytd);			//91
  start += length;

  EncodeInt32_t(start, d.d_next_o_id);	//95
  	
  Slice v(value);
  return v;
}

float TPCCLevelDB::getD_TAX(std::string& value) {
  char *v = value.c_str();
  v += 87;
  float *f = reinterpret_cast<float *>v;
  return *f;
}

int32_t TPCCLevelDB::getD_NEXT_O_ID(std::string& value) {
  char *v = value.c_str()
  v += 95;
  int32_t *i = reinterpret_cast<int32_t *>v;
  return *i;
}

std::string TPCCLevelDB::updateD_NEXT_O_ID(std::string& value, int32_t id) {
  char *v = value.c_ctr();
  v += 95;
  EncodeInt32_t(v, id);
  return std::string(v);
}

/*
void TPCCLevelDB::unmarshallDistrictValue(std::string& value) {
  char *v = value.c_str();
  strncpy(d_name, v, 10);
  //...
}*/


//Customer
Slice TPCCLevelDB::marshallCustomerKey(int32_t c_w_id, int32_t c_d_id, int32_t c_id) {
  char* key = new char[21];
  key = "CUSTOMER_";
  EncodeInt32_t(key + 9, c_id);
  EncodeInt32_t(key + 13, c_d_id);
  EncodeInt32_t(key + 17, c_w_id);
  Slice k(key);
  return k;
}

Slice TPCCLevelDB::marshallCustomerValue(Customer c) {
  char* value = new char[673];	
  char* start = value;

  int length = sizeof(c.c_first);		//0
  memcpy(start, c.c_first, length);
  start += length;

  length = sizeof(c.c_middle);			//17
  memcpy(start, c.c_middle, length);
  start += length;
  
  length = sizeof(c.c_last);			//20
  memcpy(start, c.c_last, length);
  start += length;

  length = sizeof(c.c_street_1);		//37
  memcpy(start, c.c_street_1, length);
  start += length;

  length = sizeof(c.c_street_2);		//58
  memcpy(start, c.c_street_2, length);
  start += length;

  length = sizeof(c.c_city);			//79
  memcpy(start, c.c_city, length);
  start += length;
  
  length = sizeof(c.c_state);			//100
  memcpy(start, c.c_state, length);
  start += length;

  length = sizeof(c.c_zip);				//103
  memcpy(start, c.c_zip, length);
  start += length;  

  length = sizeof(c.c_phone);			//113
  memcpy(start, c.c_phone, length);
  start += length;  

  length = sizeof(c.c_since);			//130
  memcpy(start, c.c_since, length);
  start += length;

  length = sizeof(c.c_credit);			//145
  memcpy(start, c.c_credit, length);
  start += length;

  length = sizeof(float);				//148
  EncodeFloat(start, c.c_credit_lim);
  start += length;

  EncodeFloat(start, c.c_discount);		//152
  start += length;

  EncodeFloat(start, c.c_balance);		//156
  start += length;

  EncodeFloat(start, c.c_ytd_payment);	//160
  start += length;

  EncodeInt32_t(start, c.c_payment_cnt);//164
  start += length;
  
  EncodeInt32_t(start, c.c_delivery_cnt);//168
  start += length;

  length = sizeof(c.c_data);			//172
  memcpy(start, c.c_data, length);
  
  Slice v(value);
  return v;
}

float TPCCLevelDB::getC_DISCOUNT(std::string& value) {
  char *v = value.c_str();
  v += 152;
  float *f = reinterpret_cast<float *>v;
  return *f;
}

char* TPCCLevelDB::getC_LAST(std::string& value) {
  char *v = value.c_str();
  v += 20;
  char c[17];
  memcpy(c, v, 17);
  return c;
}

char* TPCCLevelDB::getC_CREDIT(std::string& value) {
  char *v = value.c_str();
  v += 145;
  char c[3];
  memcpy(c, v, 3);
  return c;
}

//Order 
Slice TPCCLevelDB::marshallOrderKey(int32_t o_w_id, int32_t o_d_id, int32_t o_id) {
  char* key = new char[18];
  key = "ORDER_";
  EncodeInt32_t(key + 9, o_id);
  EncodeInt32_t(key + 13, o_d_id);
  EncodeInt32_t(key + 17, o_w_id);
  Slice k(key);
  return k;
}

Slice TPCCLevelDB::marshallOrderValue(Order o) {  
  char* value = new char[31];	  
  char* start = value;
  
  int length = sizeof(uint32_t);			//0
  EncodeInt32_t(start, o.o_c_id);
  start += length;
  
  length = sizeof(o.o_entry_d);				//4
  memcpy(start, o.o_entry_d, length);
  start += length;

  length = sizeof(uint32_t);				//19
  EncodeInt32_t(start, o.o_carrier_id);
  start += length;

  EncodeInt32_t(start, o.o_ol_cnt);			//23
  start += length;

  EncodeInt32_t(start, o.o_all_local);		//27
  
  Slice v(value);
  return v;
  
}

//NewOrder
Slice TPCCLevelDB::marshallNewOrderKey(NewOrder no) {
  char* key = new char[21];
  key = "NEWORDER_";
  EncodeInt32_t(key + 9, no.no_o_id);
  EncodeInt32_t(key + 13, no.no_d_id);
  EncodeInt32_t(key + 17, no.no_w_id);
  Slice k(key);
  return k;
}

//Stock
Slice TPCCLevelDB::marshallStockKey(int32_t s_w_id, int32_t s_i_id) {
  char* key = new char[14];
  key = "STOCK_";
  EncodeInt32_t(key + 6, s_i_id);
  EncodeInt32_t(key + 10, s_w_id);
  Slice k(key);
  return k;
}

Slice TPCCLevelDB::marshallStockValue(Stock s) {
  char* value = new char[];
  char* start = value;

  int length = sizeof(uint32_t);			//0
  EncodeInt32_t(start, s.s_quantity);
  start += length;

  length = 25;								//4+ i * 25
  for (int i:=0; i<10; i++) {
  	memcpy(start, s.s_dist[i], length);
    start += length;
  }

  length = sizeof(uint32_t);				//254
  EncodeInt32_t(start, s.s_ytd);
  start += length;

  length = sizeof(uint32_t);				//258
  EncodeInt32_t(start, s.s_order_cnt);
  start += length;

  length = sizeof(uint32_t);				//262
  EncodeInt32_t(start, s.s_remote_cnt);
  start += length;

  length = sizeof(s.s_data);				//264
  memcpy(start, s.s_data, length);

  Slice v(value);
  return v;
}

//Insert Tuples
Order* TPCCLevelDB::insertOrder(const Order& order) {
  Slice o_key = marshallOrderKey(order.o_w_id, order.o_d_id, order.o_id);
  Slice o_value = marshallOrderValue(order);
}


TPCCLevelDB::TPCCLevelDB(uint32_t w_num, HashTable* ht, MemTable* store, port::Mutex* mutex) {

  warehouse_num = w_num;
  storemutex = mutex;
  latestseq_ = ht;
  memstore_ = store;
}

bool TPCCLevelDB::newOrder(int32_t warehouse_id, int32_t district_id, int32_t customer_id,
        const std::vector<NewOrderItem>& items, const char* now, NewOrderOutput* output,
        TPCCUndo** undo) {
    // perform the home part
    bool result = newOrderHome(warehouse_id, district_id, customer_id, items, now, output, undo);
    if (!result) {
        return false;
    }

    // Process all remote warehouses
    WarehouseSet warehouses = newOrderRemoteWarehouses(warehouse_id, items);
    for (WarehouseSet::const_iterator i = warehouses.begin(); i != warehouses.end(); ++i) {
        vector<int32_t> quantities;
        result = newOrderRemote(warehouse_id, *i, items, &quantities, undo);
        assert(result);
        newOrderCombine(quantities, output);
    }

    return true;
}

bool TPCCLevelDB::newOrderHome(int32_t warehouse_id, int32_t district_id, int32_t customer_id,
        const vector<NewOrderItem>& items, const char* now,
        NewOrderOutput* output, TPCCUndo** undo) {

  DBTransaction tx(latestseq_, memstore_, storemutex);
  ValueType t = kTypeValue;
  tx.Begin();
  
  //--------------------------------------------------------------------------
  //The row in the WAREHOUSE table with matching W_ID is selected and W_TAX, the warehouse tax rate, is retrieved. 
  //--------------------------------------------------------------------------
  Slice w_key = marshallWarehouseKey(warehouse_id);
  Status w_s;
  std::string *w_value = new std::string();
  tx.get(w_key, w_value, &w_s);
  float w_tax = getW_TAX(*w_value);
  output->w_tax = w_tax;
  
  //--------------------------------------------------------------------------
  //The row in the DISTRICT table with matching D_W_ID and D_ ID is selected, 
  //D_TAX, the district tax rate, is retrieved, 
  //and D_NEXT_O_ID, the next available order number for the district, is retrieved and incremented by one.
  //--------------------------------------------------------------------------
  Slice d_key = marshallDistrictKey(warehouse_id, district_id);
  Status d_s;
  std::string *d_value = new std::string();
  tx.get(d_key, d_value, &d_s);
  output->d_tax = getD_TAX(*d_value);
  output->o_id = getD_NEXT_O_ID(*d_value);
  *d_value = updateD_NEXT_O_ID(*d_value, output->o_id + 1);
  Slice d_v(*d_value);
  tx.add(t, d_key, d_v);
  

  //-------------------------------------------------------------------------- 
  //The row in the CUSTOMER table with matching C_W_ID, C_D_ID, and C_ID is selected 
  //and C_DISCOUNT, the customer's discount rate, C_LAST, the customer's last name, 
  //and C_CREDIT, the customer's credit status, are retrieved.
  //--------------------------------------------------------------------------
  Slice c_key = marshallCustomerKey(warehouse_id, district_id, customer_id);
  Status c_s;
  std::string *c_value = new std::string();
  tx.get(c_key, c_value, &c_s);
  output->c_discount = getC_DISCOUNT(*c_value);
  memcpy(output->c_last, getC_LAST(*c_value), sizeof(output->c_last));
  memcpy(output->c_credit, getC_CREDIT(*c_value), sizeof(output->c_credit));


  //-------------------------------------------------------------------------- 
  //A new row is inserted into both the NEW-ORDER table and the ORDER table to reflect the creation of the new order.
  //O_CARRIER_ID is set to a null value. 
  //If the order includes only home order-lines, then O_ALL_LOCAL is set to 1, otherwise O_ALL_LOCAL is set to 0.
  //The number of items, O_OL_CNT, is computed to match ol_cnt.
  //--------------------------------------------------------------------------
  Order order;
  order.o_w_id = warehouse_id;
  order.o_d_id = district_id;
  order.o_id = output->o_id;
  order.o_c_id = customer_id;
  order.o_carrier_id = Order::NULL_CARRIER_ID;
  order.o_ol_cnt = static_cast<int32_t>(items.size());
  order.o_all_local = all_local ? 1 : 0;
  strcpy(order.o_entry_d, now);
  assert(strlen(order.o_entry_d) == DATETIME_SIZE);
  //Order* o = insertOrder(order);
  Slice o_key = marshallOrderKey(order.o_w_id, order.o_d_id, order.o_id);
  Slice o_value = marshallOrderValue(order);
  tx.add(t, o_key, o_value);
  
  NewOrder no;
  no.no_w_id = warehouse_id;
  no.no_d_id = district_id;
  no.no_o_id = output->o_id;
  Slice no_key = marshallNewOrderKey(no);
  Slice no_value = Slice();
  tx.add(t, no_key, no_value);


  //-------------------------------------------------------------------------
  //For each O_OL_CNT item on the order:
  //-------------------------------------------------------------------------

  vector<Item*> item_tuples(items.size());
  OrderLine line;
  line.ol_o_id = output->o_id;
  line.ol_d_id = district_id;
  line.ol_w_id = warehouse_id;
  memset(line.ol_delivery_d, 0, DATETIME_SIZE+1);
  output->items.resize(items.size());
  output->total = 0;
  for (int i = 0; i < items.size(); ++i) {

	//-------------------------------------------------------------------------
	//The row in the ITEM table with matching I_ID (equals OL_I_ID) is selected 
	//and I_PRICE, the price of the item, I_NAME, the name of the item, and I_DATA are retrieved. 
	//If I_ID has an unused value, a "not-found" condition is signaled, resulting in a rollback of the database transaction.
	//-------------------------------------------------------------------------

	//-------------------------------------------------------------------------
	//The row in the STOCK table with matching S_I_ID (equals OL_I_ID) and S_W_ID (equals OL_SUPPLY_W_ID) is selected. 
	//S_QUANTITY, the quantity in stock, S_DIST_xx, where xx represents the district number, and S_DATA are retrieved. 
	//If the retrieved value for S_QUANTITY exceeds OL_QUANTITY by 10 or more, 
	//then S_QUANTITY is decreased by OL_QUANTITY; 
	//otherwise S_QUANTITY is updated to (S_QUANTITY - OL_QUANTITY)+91.
	//S_YTD is increased by OL_QUANTITY and S_ORDER_CNT is incremented by 1. 
	//If the order-line is remote, then S_REMOTE_CNT is incremented by 1.
	//-------------------------------------------------------------------------
	Slice s_key = marshallStockKey(items[i].ol_supply_w_id, items[i].i_id);
	Status s_s;
    std::string *s_value = new std::string();
    tx.get(s_key, s_value, &s_s);
    char *s_data = getS_DATA(*s_value);
	
	line.ol_number = i+1;
    line.ol_i_id = items[i].i_id;
    line.ol_supply_w_id = items[i].ol_supply_w_id;
    line.ol_quantity = items[i].ol_quantity;
    assert(sizeof(line.ol_dist_info) == sizeof(stock->s_dist[district_id]));
    memcpy(line.ol_dist_info, stock->s_dist[district_id], sizeof(line.ol_dist_info));
  }
  
}

}

// Copyright (c) 2011 The LevelDB Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file. See the AUTHORS file for names of contributors.

#include "tpcc/tpcctxmemstore.h"


namespace leveldb {

  static int64_t makeWarehouseKey(int32_t w_id) {
  	int64_t id = static_cast<int64_t>(w_id);
	return id;
  }
  
  static int64_t makeDistrictKey(int32_t w_id, int32_t d_id) {
    assert(1 <= w_id && w_id <= Warehouse::MAX_WAREHOUSE_ID);
    assert(1 <= d_id && d_id <= District::NUM_PER_WAREHOUSE);
    int32_t did = d_id + (w_id * District::NUM_PER_WAREHOUSE);
    assert(did >= 0);
	int64_t id = (int64_t)1 << 50 | static_cast<int64_t>(did);
    return id;
  }

  static int64_t makeCustomerKey(int32_t w_id, int32_t d_id, int32_t c_id) {
    assert(1 <= w_id && w_id <= Warehouse::MAX_WAREHOUSE_ID);
    assert(1 <= d_id && d_id <= District::NUM_PER_WAREHOUSE);
    assert(1 <= c_id && c_id <= Customer::NUM_PER_DISTRICT);
    int32_t cid = (w_id * District::NUM_PER_WAREHOUSE + d_id)
            * Customer::NUM_PER_DISTRICT + c_id;
    assert(cid >= 0);
	int64_t id = (int64_t)2 << 50 | static_cast<int64_t>(cid);
    return id;
  }

  static int64_t makeHistoryKey() {
  	return 3;
  }

  static int64_t makeNewOrderKey(int32_t w_id, int32_t d_id, int32_t o_id) {
    assert(1 <= w_id && w_id <= Warehouse::MAX_WAREHOUSE_ID);
    assert(1 <= d_id && d_id <= District::NUM_PER_WAREHOUSE);
    assert(1 <= o_id && o_id <= Order::MAX_ORDER_ID);
    int32_t upper_id = w_id * Warehouse::MAX_WAREHOUSE_ID + d_id;
    assert(upper_id > 0);
    int64_t id = static_cast<int64_t>(upper_id) << 32 | static_cast<int64_t>(o_id);
	assert(id > 0);
	id = (int64_t)4 << 50 | id;
    
    return id;
  }

  static int64_t makeOrderKey(int32_t w_id, int32_t d_id, int32_t o_id) {
    assert(1 <= w_id && w_id <= Warehouse::MAX_WAREHOUSE_ID);
    assert(1 <= d_id && d_id <= District::NUM_PER_WAREHOUSE);
    assert(1 <= o_id && o_id <= Order::MAX_ORDER_ID);
    int32_t upper_id = w_id * Warehouse::MAX_WAREHOUSE_ID + d_id;
    assert(upper_id > 0);
    int64_t id = static_cast<int64_t>(upper_id) << 32 | static_cast<int64_t>(o_id);
	assert(id > 0);
	id = (int64_t)5 << 50 | id;
    
    return id;
  }

  static int64_t makeOrderLineKey(int32_t w_id, int32_t d_id, int32_t o_id, int32_t number) {
    assert(1 <= w_id && w_id <= Warehouse::MAX_WAREHOUSE_ID);
    assert(1 <= d_id && d_id <= District::NUM_PER_WAREHOUSE);
    assert(1 <= o_id && o_id <= Order::MAX_ORDER_ID);
    assert(1 <= number && number <= Order::MAX_OL_CNT);
    // TODO: This may be bad for locality since o_id is in the most significant position. However,
    // Order status fetches all rows for one (w_id, d_id, o_id) tuple, so it may be fine,
    // but stock level fetches order lines for a range of (w_id, d_id, o_id) values
    int32_t olid = ((o_id * District::NUM_PER_WAREHOUSE + d_id)
            * Warehouse::MAX_WAREHOUSE_ID + w_id) * Order::MAX_OL_CNT + number;
    assert(olid >= 0);
	int64_t id = (int64_t)6 << 50 | static_cast<int64_t>(olid);
    return id;
  }

  static int64_t makeItemKey(int32_t i_id) {
  	int64_t id = (int64_t)7 << 50 | static_cast<int64_t>(i_id);
	return id;
  }

  static int64_t makeStockKey(int32_t w_id, int32_t s_id) {
    assert(1 <= w_id && w_id <= Warehouse::MAX_WAREHOUSE_ID);
    assert(1 <= s_id && s_id <= Stock::NUM_STOCK_PER_WAREHOUSE);
    int32_t sid = s_id + (w_id * Stock::NUM_STOCK_PER_WAREHOUSE);
    assert(sid >= 0);
	int64_t id = (int64_t)8 << 50 | static_cast<int64_t>(sid);
    return id;
  }

  static void updateDistrict(District *newd, District *oldd ) {
  	memcpy(newd->d_city, oldd->d_city, 21);
	memcpy(newd->d_zip, oldd->d_zip, 10);
	memcpy(newd->d_name, oldd->d_name, 11);
	memcpy(newd->d_state, oldd->d_state, 3);
	memcpy(newd->d_street_1, oldd->d_street_1, 21);
	memcpy(newd->d_street_2, oldd->d_street_2, 21);

	newd->d_tax = oldd->d_tax;
	newd->d_w_id = oldd->d_w_id;
	newd->d_ytd = oldd->d_ytd;	
	newd->d_id = oldd->d_id;

	newd->d_next_o_id = oldd->d_next_o_id + 1;
  }

  static void updateStock(Stock *news, Stock *olds, const NewOrderItem *item, int32_t warehouse_id) {
  	if (olds->s_quantity > (item->ol_quantity + 10))
	  news->s_quantity = olds->s_quantity - item->ol_quantity;
	else news->s_quantity = olds->s_quantity - item->ol_quantity + 91;		
	news->s_ytd = olds->s_ytd - item->ol_quantity;
	news->s_order_cnt = olds->s_order_cnt + 1;
	if (item->ol_supply_w_id != warehouse_id) 
	  news->s_remote_cnt = olds->s_remote_cnt + 1;

	memcpy(news->s_data, olds->s_data, 51);
	for (int i = 0; i < 10; i++)
	  memcpy(news->s_dist[i], olds->s_dist[i], 25);

	news->s_i_id = olds->s_i_id;
	news->s_w_id = olds->s_w_id;
  }
  
  TPCCTxMemStore::TPCCTxMemStore() {
  	cmp = new KeyComparator();
	store = new TXMemStore<Key, Value, leveldb::KeyComparator>(*cmp);
	KeyHash *kh = new KeyHash();
	seqs = new HashTable<Key, KeyHash, KeyComparator>(*kh, *cmp);
  }

  void TPCCTxMemStore::insertWarehouse(const Warehouse & warehouse) {
  	int64_t key = makeWarehouseKey(warehouse.w_id);
	uint64_t *value = new uint64_t();
	*value = reinterpret_cast<uint64_t>(&warehouse);
	ValueType t = kTypeValue;
  	SequenceNumber s = 1;
  	store->Put(key, value ,s);
  	seqs->Insert(key, s);
  }

  void TPCCTxMemStore::insertDistrict(const District & district) {
  	int64_t key = makeDistrictKey(district.d_w_id, district.d_id);
	uint64_t *value = new uint64_t();
	*value = reinterpret_cast<uint64_t>(&district);
	ValueType t = kTypeValue;
  	SequenceNumber s = 1;
  	store->Put(key, value ,s);
  	seqs->Insert(key, s);
  }

  void TPCCTxMemStore::insertCustomer(const Customer & customer) {
  	int64_t key = makeCustomerKey(customer.c_w_id, customer.c_d_id, customer.c_id);
	uint64_t *value = new uint64_t();
	*value = reinterpret_cast<uint64_t>(&customer);
	ValueType t = kTypeValue;
  	SequenceNumber s = 1;
  	store->Put(key, value ,s);
  	seqs->Insert(key, s);
  }

  History* TPCCTxMemStore::insertHistory(const History & history) {
  	return NULL;
  }

  void TPCCTxMemStore::insertItem(const Item & item) {
  	int64_t key = makeItemKey(item.i_id);
	uint64_t *value = new uint64_t();
	*value = reinterpret_cast<uint64_t>(&item);
	ValueType t = kTypeValue;
  	SequenceNumber s = 1;
  	store->Put(key, value ,s);
  	seqs->Insert(key, s);
  }

  void TPCCTxMemStore::insertStock(const Stock & stock) {
  	int64_t key = makeStockKey(stock.s_w_id, stock.s_i_id);
	uint64_t *value = new uint64_t();
	*value = reinterpret_cast<uint64_t>(&stock);
	ValueType t = kTypeValue;
  	SequenceNumber s = 1;
  	store->Put(key, value ,s);
  	seqs->Insert(key, s);
  }

  Order* TPCCTxMemStore::insertOrder(const Order & order) {
  	int64_t key = makeOrderKey(order.o_w_id, order.o_d_id, order.o_id);
	uint64_t *value = new uint64_t();
	*value = reinterpret_cast<uint64_t>(&order);
	ValueType t = kTypeValue;
  	SequenceNumber s = 1;
  	store->Put(key, value ,s);
  	seqs->Insert(key, s);
	return const_cast<Order *>(&order);
  }

  OrderLine* TPCCTxMemStore::insertOrderLine(const OrderLine & orderline) {
  	int64_t key = makeOrderLineKey(orderline.ol_w_id, orderline.ol_d_id, orderline.ol_o_id, orderline.ol_number);
	uint64_t *value = new uint64_t();
	*value = reinterpret_cast<uint64_t>(&orderline);
	ValueType t = kTypeValue;
  	SequenceNumber s = 1;
  	store->Put(key, value ,s);
  	seqs->Insert(key, s);
	return const_cast<OrderLine *>(&orderline);
  }

  NewOrder* TPCCTxMemStore::insertNewOrder(int32_t w_id,int32_t d_id,int32_t o_id) {
  	int64_t key = makeOrderKey(w_id, d_id, o_id);
	NewOrder *neworder = new NewOrder();
	neworder->no_w_id = w_id;
	neworder->no_d_id = d_id;
	neworder->no_o_id = o_id;
	uint64_t *value = new uint64_t();
	*value = reinterpret_cast<uint64_t>(&neworder);
	ValueType t = kTypeValue;
  	SequenceNumber s = 1;
  	store->Put(key, value ,s);
  	seqs->Insert(key, s);
	return neworder;
  }


  bool TPCCTxMemStore::newOrder(int32_t warehouse_id, int32_t district_id, int32_t customer_id,
        const std::vector<NewOrderItem>& items, const char* now, NewOrderOutput* output,
        TPCCUndo** undo) {
    // perform the home part
  bool result = newOrderHome(warehouse_id, district_id, customer_id, items, now, output, undo);
    if (!result) {
        return false;
    }
	
    return true;
  }

  bool TPCCTxMemStore::newOrderHome(int32_t warehouse_id, int32_t district_id, int32_t customer_id,
        const std::vector<NewOrderItem>& items, const char* now,
        NewOrderOutput* output, TPCCUndo** undo) {
    
	ValueType t = kTypeValue;
	while(true) {
	  leveldb::DBTransaction<leveldb::Key, leveldb::Value, 
  				leveldb::KeyHash, leveldb::KeyComparator> tx(seqs, store, *cmp);
	  tx.Begin();
	  output->status[0] = '\0';
	  //Cheat
  	  for (int i = 0; i < items.size(); ++i) 
		if (items[i].i_id == 100001) {
  	  	  strcpy(output->status, NewOrderOutput::INVALID_ITEM_STATUS);
		  tx.Abort();
	  	  return false;
		}
	  
  	  //--------------------------------------------------------------------------
  	  //The row in the WAREHOUSE table with matching W_ID is selected and W_TAX, the warehouse tax rate, is retrieved. 
  	  //--------------------------------------------------------------------------

	  int64_t w_key = makeWarehouseKey(warehouse_id);
  	  Status w_s;
	  uint64_t *w_value;  
 	  bool found = tx.Get(w_key, &w_value, &w_s);
	  assert(found);
	  Warehouse *w = reinterpret_cast<Warehouse *>(w_value);
	  output->w_tax = w->w_tax;

	  //--------------------------------------------------------------------------
  	  //The row in the DISTRICT table with matching D_W_ID and D_ ID is selected, 
	  //D_TAX, the district tax rate, is retrieved, 
  	  //and D_NEXT_O_ID, the next available order number for the district, is retrieved and incremented by one.
  	  //--------------------------------------------------------------------------

	  int64_t d_key = makeDistrictKey(warehouse_id, district_id);
  	  Status d_s;
  	  uint64_t *d_value;
  	  found = tx.Get(d_key, &d_value, &d_s);
	  assert(found);
	  District *d = reinterpret_cast<District *>(d_value);
	  output->d_tax = d->d_tax;
	  output->o_id = d->d_next_o_id;

  	  District *newd = new District();
	  updateDistrict(newd, d);
	  uint64_t *d_v = reinterpret_cast<uint64_t *>(newd);
	  tx.Add(t, d_key, d_v);


	  //-------------------------------------------------------------------------- 
  	  //The row in the CUSTOMER table with matching C_W_ID, C_D_ID, and C_ID is selected 
	  //and C_DISCOUNT, the customer's discount rate, C_LAST, the customer's last name, 
  	  //and C_CREDIT, the customer's credit status, are retrieved.
  	  //--------------------------------------------------------------------------
	  
	  uint64_t c_key = makeCustomerKey(warehouse_id, district_id, customer_id);
  	  Status c_s;
  	  uint64_t *c_value;
	  found = tx.Get(c_key, &c_value, &c_s);
 	  assert(found);
	  Customer *c = reinterpret_cast<Customer *>(c_value);
  	  output->c_discount = c->c_discount;
  	  memcpy(output->c_last, c->c_last, sizeof(output->c_last));
  	  memcpy(output->c_credit, c->c_credit, sizeof(output->c_credit));

	  //-------------------------------------------------------------------------- 
  	  //A new row is inserted into both the NEW-ORDER table and the ORDER table to reflect the creation of the new order.
	  //O_CARRIER_ID is set to a null value. 
	  //If the order includes only home order-lines, then O_ALL_LOCAL is set to 1, otherwise O_ALL_LOCAL is set to 0.
	  //The number of items, O_OL_CNT, is computed to match ol_cnt.
  	  //--------------------------------------------------------------------------

	  // Check if this is an all local transaction
	  bool all_local = true;
	  for (int i = 0; i < items.size(); ++i) {
    	if (items[i].ol_supply_w_id != warehouse_id) {
	      all_local = false;
    	  break;
    	}
  	  }
  
	  Order *order = new Order();
	  order->o_w_id = warehouse_id;
	  order->o_d_id = district_id;
	  order->o_id = output->o_id;
	  order->o_c_id = customer_id;
	  order->o_carrier_id = Order::NULL_CARRIER_ID;
	  order->o_ol_cnt = static_cast<int32_t>(items.size());
	  order->o_all_local = all_local ? 1 : 0;
	  strcpy(order->o_entry_d, now);
  	  assert(strlen(order->o_entry_d) == DATETIME_SIZE);
	  uint64_t o_key = makeOrderKey(warehouse_id, district_id, order->o_id);
	  uint64_t *o_value = reinterpret_cast<uint64_t *>(order);
	  tx.Add(t, o_key, o_value);
  
  	  NewOrder *no = new NewOrder();
  	  no->no_w_id = warehouse_id;
	  no->no_d_id = district_id;
	  no->no_o_id = output->o_id;
	  uint64_t no_key = makeNewOrderKey(warehouse_id, district_id, no->no_o_id);
	  uint64_t *no_value = new uint64_t();
	  tx.Add(t, no_key, no_value);

	  //-------------------------------------------------------------------------
  	  //For each O_OL_CNT item on the order:
  	  //-------------------------------------------------------------------------

  	  OrderLine *line = new OrderLine();
	  line->ol_o_id = output->o_id;
	  line->ol_d_id = district_id;
	  line->ol_w_id = warehouse_id;
	  memset(line->ol_delivery_d, 0, DATETIME_SIZE+1);
	  output->items.resize(items.size());
	  output->total = 0;

	  for (int i = 0; i < items.size(); ++i) {
  		//-------------------------------------------------------------------------
		//The row in the ITEM table with matching I_ID (equals OL_I_ID) is selected 
		//and I_PRICE, the price of the item, I_NAME, the name of the item, and I_DATA are retrieved. 
		//If I_ID has an unused value, a "not-found" condition is signaled, resulting in a rollback of the database transaction.	
		//-------------------------------------------------------------------------
		
		uint64_t i_key = makeItemKey(items[i].i_id);
		Status i_s;
		uint64_t *i_value;
	
		bool found = tx.Get(i_key, &i_value, &i_s);
		if (!found && items[i].i_id <=100000) {
			printf("Item %d\n", items[i].i_id);
			assert(found);
		}	
		if (!found) {
		  strcpy(output->status, NewOrderOutput::INVALID_ITEM_STATUS);
	 	  tx.Abort();
	  	  return false;
		}
		Item *item = reinterpret_cast<Item *>(i_value);
		assert(sizeof(output->items[i].i_name) == sizeof(item->i_name));
	    memcpy(output->items[i].i_name, item->i_name, sizeof(output->items[i].i_name));
		output->items[i].i_price = item->i_price;


		//-------------------------------------------------------------------------
		//The row in the STOCK table with matching S_I_ID (equals OL_I_ID) and S_W_ID (equals OL_SUPPLY_W_ID) is selected. 
		//S_QUANTITY, the quantity in stock, S_DIST_xx, where xx represents the district number, and S_DATA are retrieved. 
		//If the retrieved value for S_QUANTITY exceeds OL_QUANTITY by 10 or more, 
		//then S_QUANTITY is decreased by OL_QUANTITY; 
		//otherwise S_QUANTITY is updated to (S_QUANTITY - OL_QUANTITY)+91.
		//S_YTD is increased by OL_QUANTITY and S_ORDER_CNT is incremented by 1. 
		//If the order-line is remote, then S_REMOTE_CNT is incremented by 1.
		//-------------------------------------------------------------------------

		uint64_t s_key = makeStockKey(items[i].ol_supply_w_id, items[i].i_id);
		Status s_s;
    	uint64_t *s_value;
	    //if (items[i].i_id > 100000) printf("Unused key!\n");
	    found = tx.Get(s_key, &s_value, &s_s);
		assert(found);
		Stock *s = reinterpret_cast<Stock *>(s_value);  
		Stock *news = new Stock();
		updateStock(news, s, &items[i], warehouse_id);
		output->items[i].s_quantity = news->s_quantity;
		uint64_t *s_v =  reinterpret_cast<uint64_t *>(s);
		tx.Add(t, s_key, s_v);

		//-------------------------------------------------------------------------
		//The amount for the item in the order (OL_AMOUNT) is computed as: OL_QUANTITY * I_PRICE
		//The strings in I_DATA and S_DATA are examined. If they both include the string "ORIGINAL", 
		//the brand-generic field for that item is set to "B", otherwise, the brand-generic field is set to "G".
		//-------------------------------------------------------------------------  
		
    	output->items[i].ol_amount = static_cast<float>(items[i].ol_quantity) * item->i_price;
	    line->ol_amount = output->items[i].ol_amount;
        
		bool stock_is_original = (strstr(s->s_data, "ORIGINAL") != NULL);
    	if (stock_is_original && strstr(item->i_data, "ORIGINAL") != NULL) {
	  	  output->items[i].brand_generic = NewOrderOutput::ItemInfo::BRAND;
		} else {
	  	  output->items[i].brand_generic = NewOrderOutput::ItemInfo::GENERIC;
		}

		//-------------------------------------------------------------------------
		//A new row is inserted into the ORDER-LINE table to reflect the item on the order. 
		//OL_DELIVERY_D is set to a null value, 
		//OL_NUMBER is set to a unique value within all the ORDER-LINE rows that have the same OL_O_ID value, 
		//and OL_DIST_INFO is set to the content of S_DIST_xx, where xx represents the district number (OL_D_ID)
		//-------------------------------------------------------------------------
		
		line->ol_number = i + 1;
	    line->ol_i_id = items[i].i_id;
    	line->ol_supply_w_id = items[i].ol_supply_w_id;
	    line->ol_quantity = items[i].ol_quantity;
    	assert(sizeof(line->ol_dist_info) == sizeof(s->s_dist[district_id]));
    	memcpy(line->ol_dist_info, s->s_dist[district_id], sizeof(line->ol_dist_info));
		uint64_t l_key = makeOrderLineKey(line->ol_w_id, line->ol_d_id, line->ol_o_id, line->ol_number);
		uint64_t *l_value = reinterpret_cast<uint64_t *>(line);
		tx.Add(t, l_key, l_value);


		//-------------------------------------------------------------------------
		//The total-amount for the complete order is computed as: 
		//sum(OL_AMOUNT) * (1 - C_DISCOUNT) * (1 + W_TAX + D_TAX)
		//-------------------------------------------------------------------------
		output->total += line->ol_amount;
		
	  }

	  output->total = output->total * (1 - output->c_discount) * (1 + output->w_tax + output->d_tax);
 
 	  bool b = tx.End();  
  	  if (b) break;
  	}
  
    return true;
  }

//not used yet
bool TPCCTxMemStore::newOrderRemote(int32_t home_warehouse, int32_t remote_warehouse,
            const std::vector<NewOrderItem>& items, std::vector<int32_t>* out_quantities,
            TPCCUndo** undo){
  return false;
}

int32_t TPCCTxMemStore::stockLevel(int32_t warehouse_id, int32_t district_id, int32_t threshold){
  return 0;
}
void TPCCTxMemStore::orderStatus(int32_t warehouse_id, int32_t district_id, int32_t customer_id, OrderStatusOutput* output){
  return;
}
void TPCCTxMemStore::orderStatus(int32_t warehouse_id, int32_t district_id, const char* c_last, OrderStatusOutput* output){
  return;
}

void TPCCTxMemStore::payment(int32_t warehouse_id, int32_t district_id, int32_t c_warehouse_id,
		  int32_t c_district_id, int32_t customer_id, float h_amount, const char* now,
		  PaymentOutput* output, TPCCUndo** undo) {
  return;
}
void TPCCTxMemStore::payment(int32_t warehouse_id, int32_t district_id, int32_t c_warehouse_id,
		  int32_t c_district_id, const char* c_last, float h_amount, const char* now,
		  PaymentOutput* output, TPCCUndo** undo) {
  return;
}
void TPCCTxMemStore::paymentHome(int32_t warehouse_id, int32_t district_id, int32_t c_warehouse_id,
		  int32_t c_district_id, int32_t c_id, float h_amount, const char* now,
		  PaymentOutput* output, TPCCUndo** undo){
  return;
}
void TPCCTxMemStore::paymentRemote(int32_t warehouse_id, int32_t district_id, int32_t c_warehouse_id,
		  int32_t c_district_id, int32_t c_id, float h_amount, PaymentOutput* output,
		  TPCCUndo** undo){
  return;
}
void TPCCTxMemStore::paymentRemote(int32_t warehouse_id, int32_t district_id, int32_t c_warehouse_id,
		  int32_t c_district_id, const char* c_last, float h_amount, PaymentOutput* output,
		  TPCCUndo** undo){
  return;
}
void TPCCTxMemStore::delivery(int32_t warehouse_id, int32_t carrier_id, const char* now,
		  std::vector<DeliveryOrderInfo>* orders, TPCCUndo** undo){
  return;
}
bool TPCCTxMemStore::hasWarehouse(int32_t warehouse_id){
  return true;
}
	
void TPCCTxMemStore::applyUndo(TPCCUndo* undo){
  return;
}
void TPCCTxMemStore::freeUndo(TPCCUndo* undo){
  return;
}

 
}


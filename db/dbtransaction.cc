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
 	
  DBTransaction::DBTransaction()
  {
	//TODO: get the globle store and versions passed by the parameter
  }
  
  DBTransaction::~DBTransaction()
  {
	//TODO: clear all the data
  }

  void DBTransaction::Begin()
  {
	//TODO: reset the local read set and write set
  }
  
  bool DBTransaction::End()
  {
	//TODO
	return false;
  }
  void DBTransaction::Add(ValueType type, Slice& key, Slice& value)
  {
	//write the key value into local buffer
	WSNode *wn = new WSNode();
	wn->key = &key;
	wn->value = &value;
	wn->type = type;
	wn->seq = 0;

	//TODO: Pass the deleter of wsnode into function
	writeset.Insert(key, wn, NULL);
  }

  bool DBTransaction::Get(const Slice& key, std::string* value, Status* s)
  {
  	//step 1. First check if the <k,v> is in the write set
  	
	WSNode* wn;
	if(writeset.Lookup(key, (void **)&wn)) {
		//Found
		switch (wn->type) {
          case kTypeValue: {
          	value->assign(wn->value->data(), wn->value->size());
          	return true;
          }
          case kTypeDeletion:
          	*s = Status::NotFound(Slice());
          	return true;
      	}	
	}

	//step 2.  Read the <k,v> from the in memory store

	//first get the seq number
	bool found = false;
	uint64_t seq = 0;
	
	found = latestseq_->Lookup(key, (void **)&seq);


	if (!found)
		return found;
	
	//construct the lookup key and find the key value in the in memory storage
	LookupKey lkey(key, seq);

	storemutex->Lock();
	found = memstore_->Get(lkey, value, s);
	storemutex->Unlock();
	
	assert(found);
	

	// step 3. put into the read set
	
	readset.Insert(key, (void *)seq, NULL);

	return found;
  }


}  // namespace leveldb

int main()
{
	leveldb::HashTable ht;
    
    for(int i = 0; i < 10; i++) {
		char key[100];
        snprintf(key, sizeof(key), "%d", i);

	//printf("Insert %s\n", *s);
		ht.Insert(leveldb::Slice(key), (void *)i,NULL);
    }

    for(int i = 0; i < 20; i++) {
	char key[100];
        snprintf(key, sizeof(key), "%d", i);

	//printf("Insert %s\n", *s);
	void* v;
	if(!ht.Lookup(leveldb::Slice(key), &v))
		printf("key %s Not Found\n", key);
    }
    //ht.PrintHashTable();
    
	leveldb::HashTable::Iterator* iter = new leveldb::HashTable::Iterator(&ht);
	while(iter->Next()) {
		leveldb::HashTable::Node *n = iter->Current();
		
		printf("Key: %s , Hash: %d, Value: %d \n", n->key_data, n->hash, n->value);
		
	}
	
    //printf("helloworld\n");
    return 0;
 }



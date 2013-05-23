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
#include "util/mutexlock.h"
#include "util/rtm.h"

namespace leveldb {
 	
  DBTransaction::DBTransaction(HashTable* ht, MemTable* store, port::Mutex* mutex)
  {
	//TODO: get the globle store and versions passed by the parameter
	storemutex = mutex;
	latestseq_ = ht;
	memstore_ = store;

	readset = NULL;
	writeset = NULL;
	
  }
  
  DBTransaction::~DBTransaction()
  {
	//TODO: clear all the data
	if(readset != NULL) {
		delete readset;
		readset = NULL;
	}
	
	if(writeset != NULL) {
		delete writeset;
		writeset = NULL;
	}
  }

  void DBTransaction::Begin()
  {
	//TODO: reset the local read set and write set
	if(readset != NULL) {
		delete readset;
		readset = NULL;
	}
	
	if(writeset != NULL) {
		delete writeset;
		writeset = NULL;
	}
	
	readset = new HashTable();
	writeset = new HashTable();
  }
  
  bool DBTransaction::End()
  {
	if( !Validation())
		return false;

	GlobalCommit();
	return true;
  }
  void DBTransaction::Add(ValueType type, Slice& key, Slice& value)
  {
	//write the key value into local buffer
	WSNode *wn = new WSNode();
	wn->value = &value;
	wn->type = type;
	wn->seq = 0;
	//TODO: Pass the deleter of wsnode into function
	writeset->Insert(key, wn, NULL);
  }

  bool DBTransaction::Get(const Slice& key, std::string* value, Status* s)
  {
  	//step 1. First check if the <k,v> is in the write set
  	
	WSNode* wn;
	if(writeset->Lookup(key, (void **)&wn)) {
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


	if (!found) {
		//even not found, still need to put the k into read set to avoid concurrent insertion		
		readset->Insert(key, (void *)seq, NULL);
		return found;

	}
	
	//construct the lookup key and find the key value in the in memory storage
	LookupKey lkey(key, seq);

	found = false;

	//may be not found, should wait for a while
	while(!found) {
		
		storemutex->Lock();
		found = memstore_->GetWithSeq(lkey, value, s);
		storemutex->Unlock();
		
	}

	// step 3. put into the read set
	
	readset->Insert(key, (void *)seq, NULL);
	
	//printf("Get seq %ld value %s\n", seq, value->c_str());
	
	return found;
  }

  bool DBTransaction::Validation() {
	//TODO use tx to protect
	//MutexLock mu(storemutex);
	RTMScope rtm(NULL);
	
	//step 1. check if the seq has been changed (any one change the value after reading)
	HashTable::Iterator *riter = new HashTable::Iterator(readset);
	while(riter->Next()) {
		HashTable::Node *cur = riter->Current();
		uint64_t oldseq = (uint64_t)cur->value;
		uint64_t curseq = 0;
		bool found = latestseq_->Lookup(cur->key(),(void **)&curseq);
		assert(oldseq == 0 || found);
		
		if(oldseq != curseq)
			return false; //Return false is safe, because it hasn't modify any data, then no need to abort
	}

	//step 2.  update the the seq set 
	HashTable::Iterator *witer = new HashTable::Iterator(writeset);
	while(witer->Next()) {
		HashTable::Node *cur = witer->Current();
		WSNode *wcur = (WSNode *)cur->value;
		
		uint64_t curseq;
		bool found = latestseq_->Lookup(cur->key(),(void **)&curseq);
		if(!found) {
			//The node is inserted into the list first time
			curseq = 1;
			latestseq_->Insert(cur->key(),(void *)curseq, NULL);
		}
		else {			
			curseq++;		
			latestseq_->Update(cur->key(),(void *)curseq);
		}
		
		wcur->seq = curseq;
	
	}

	return true;
	
  }


  
  void DBTransaction::GlobalCommit() {
	//commit the local write set into the memory storage
	HashTable::Iterator *witer = new HashTable::Iterator(writeset);
	while(witer->Next()) {
		HashTable::Node *cur = witer->Current();
		WSNode *wcur = (WSNode *)cur->value;
		
		storemutex->Lock();
		//printf("Commit seq %ld value %s\n", wcur->seq,  *wcur->value);
		memstore_->Add(wcur->seq, wcur->type, cur->key(), *wcur->value);
		storemutex->Unlock();
	}

	if(readset != NULL) {
		delete readset;
		readset = NULL;
	}
	
	if(writeset != NULL) {
		delete writeset;
		writeset = NULL;
	}
	
  }


}  // namespace leveldb

void testht()
{
	leveldb::HashTable ht;
	char key[100];
		
	for(int i = 0; i < 10; i++) {
	
		snprintf(key, sizeof(key), "%d", 10);

	//printf("Insert %s\n", *s);
		ht.Insert(leveldb::Slice(key), (void *)i,NULL);
	}

	ht.Update(leveldb::Slice(key), (void *)1000);
	
	for(int i = 0; i < 20; i++) {
	
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
 }

/*
int main()
{
    
	leveldb::Options options;
	leveldb::InternalKeyComparator cmp(options.comparator);
	
	leveldb::HashTable seqs;
	leveldb::MemTable *store = new leveldb::MemTable(cmp);
	leveldb::port::Mutex mutex;
	
	leveldb::DBTransaction tx(&seqs, store, &mutex);

	leveldb::ValueType t = leveldb::kTypeValue;

	char* key = new char[100];
    snprintf(key, sizeof(key), "%d", 1024);
	leveldb::Slice k(key);
	leveldb::SequenceNumber seq = 1;
	
	store->Add(seq, t, k, k);


	key = new char[100];
    snprintf(key, sizeof(key), "%d", 2048);
	leveldb::Slice k2(key);
	seq = 100;
	
	store->Add(seq, t, k, k2);

	std::string str;
	leveldb::Status s;
	seq = 2;
	leveldb::LookupKey lkey(k, seq);
	bool found = store->GetWithSeq(lkey, &str, &s);
	printf("Found %d value %s\n", found, str.c_str());
	
	/*
	tx.Begin();
	
    for(int i = 0; i < 10; i++) {
		char* key = new char[100];
        snprintf(key, sizeof(key), "%d", i);
		leveldb::Slice k(key);
		leveldb::Slice *v = new leveldb::Slice(key);
		printf("Insert %s ", k);
		printf(" Value %s\n", *v);
		tx.Add(t, k, *v);
		std::string *str = new std::string();
		leveldb::Status s;
		tx.Get(k, str, &s);
    }

	tx.End();

	tx.Begin();	

	for(int i = 0; i < 10; i++) {
		char key[100];
        snprintf(key, sizeof(key), "%d", i);
		leveldb::Slice k(key);
		std::string *str = new std::string();
		leveldb::Status s;
		tx.Get(k, str, &s);
		printf("Get %s ", k);
		printf(" Value %s\n", str->c_str());
    }

	tx.End();
	
	leveldb::Iterator* iter = store->NewIterator();
	iter->SeekToFirst();
	int count = 0;
	
	while(iter->Valid()) {		
		count++;
		printf("Key: %s  ", iter->key());
		printf("Value: %s \n", iter->value());
		iter->Next();
		
	}
	
    printf("Total Elements %d\n", count);
	
    return 0;
 }

 */


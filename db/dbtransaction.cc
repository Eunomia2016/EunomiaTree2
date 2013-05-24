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

  static void UnrefWSN(const Slice& key, void* value) {
	   DBTransaction::WSNode* wsn = reinterpret_cast<DBTransaction::WSNode*>(value);
	   wsn->Unref();
  }

 	
  DBTransaction::DBTransaction(HashTable* ht, MemTable* store, port::Mutex* mutex)
  {
	//TODO: get the globle store and versions passed by the parameter
	storemutex = mutex;
	latestseq_ = ht;
	memstore_ = store;

	readset = NULL;
	writeset = NULL;
	committedValues = NULL;

  }
  
  DBTransaction::~DBTransaction()
  {
	//clear all the data
	if(readset != NULL) {
		delete readset;
		readset = NULL;
	}
	
	if(writeset != NULL) {
		delete writeset;
		writeset = NULL;
	}

	WSNode *wsn = committedValues;
	while(wsn != NULL) {
		WSNode* tmp = wsn;
		tmp->Unref();
		wsn = wsn->next;
	}
	committedValues = NULL;
	
  }

  void DBTransaction::Begin()
  {
	//reset the local read set and write set
	if(readset != NULL) {
		delete readset;
		readset = NULL;
	}
	
	if(writeset != NULL) {
		delete writeset;
		writeset = NULL;
	}

	WSNode *wsn = committedValues;
	while(wsn != NULL) {
		WSNode* tmp = wsn;
		tmp->Unref();
		wsn = wsn->next;
	}
	committedValues = NULL;
	
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
	WSNode* wsn = reinterpret_cast<WSNode*>(
    	malloc(sizeof(WSNode)-1 + value.size()));
	
    wsn->value_length= value.size();
	memcpy(wsn->value_data, value.data(), value.size());;
	wsn->type = type;
	wsn->seq = 0;
	wsn->refs = 1;
	wsn->next = NULL;
	
	//Pass the deleter of wsnode into function
	wsn->knode = writeset->Insert(key, wsn, &UnrefWSN);
	wsn->knode->Ref();

	//Insert to the committed values linked list
	if(committedValues != NULL) {
		wsn->next = committedValues->next;
	}
	committedValues = wsn;
	wsn->Ref();
		
  }

  bool DBTransaction::Get(const Slice& key, std::string* value, Status* s)
  {
  	//step 1. First check if the <k,v> is in the write set
  	
	WSNode* wsn;
	if(writeset->Lookup(key, (void **)&wsn)) {
		//Found
		switch (wsn->type) {
          case kTypeValue: {
          	value->assign(wsn->value_data, wsn->value_length);
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
	
	
	HashTable::Iterator *riter = new HashTable::Iterator(readset);	
	HashTable::Iterator *witer = new HashTable::Iterator(writeset);
	
	bool validate = true;

	{
		//RTMScope rtm(NULL);
		MutexLock mu(storemutex);
		
		//step 1. check if the seq has been changed (any one change the value after reading)
		while(riter->Next()) {
			HashTable::Node *cur = riter->Current();
			uint64_t oldseq = (uint64_t)cur->value;
			uint64_t curseq = 0;
			bool found = latestseq_->Lookup(cur->key(),(void **)&curseq);
			assert(oldseq == 0 || found);
			
			if(oldseq != curseq) {
				validate = false; //Return false is safe, because it hasn't modify any data, then no need to abort
				goto end;

			}
		}
		//step 2.  update the the seq set 
		while(witer->Next()) {
			HashTable::Node *cur = witer->Current();
			WSNode *wcur = (WSNode *)cur->value;
			
			uint64_t curseq;
			bool found = latestseq_->Lookup(cur->key(),(void **)&curseq);
			if(!found) {
				//The node is inserted into the list first time
				curseq = 1;

				//Still use the node in the write set to avoid memory allocation
				HashTable::Node* n = writeset->Remove(cur->key(), cur->hash);
			
				assert(n == cur);
				
				if(cur->deleter != NULL)
					cur->deleter(cur->key(), cur->value);
				
				cur->deleter = NULL;
				cur->value = (void *)curseq;
				
				//latestseq_->Insert(cur->key(),(void *)curseq, NULL);
				latestseq_->InsertNode(cur);
				
			}
			else {			
				curseq++;		
				latestseq_->Update(cur->key(),(void *)curseq);
			}
			
			wcur->seq = curseq;
		
		}

	}
	
end:
	delete riter;
	delete witer;
	
	return validate;
	
  }


  
  void DBTransaction::GlobalCommit() {
	//commit the local write set into the memory storage
	WSNode *wsn = committedValues;
	while(wsn != NULL) {
		HashTable::Node *cur = wsn->knode;;
		
		storemutex->Lock();
		memstore_->Add(wsn->seq, wsn->type, cur->key(), wsn->value());
		storemutex->Unlock();
		
		wsn = wsn->next;
	}

	if(readset != NULL) {
		delete readset;
		readset = NULL;
	}
	
	if(writeset != NULL) {
		delete writeset;
		writeset = NULL;
	}


	wsn = committedValues;
	while(wsn != NULL) {
		WSNode* tmp = wsn;
		tmp->Unref();
		wsn = wsn->next;
	}
	committedValues = NULL;
	
  }

}  // namespace leveldb


/*

int main()
{
	leveldb::HashTable ht;
	char key[100];
		
	for(int i = 0; i < 10; i++) {
	
		snprintf(key, sizeof(key), "%d", i);

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


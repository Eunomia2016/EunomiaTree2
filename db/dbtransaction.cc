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
#include "util/hash.h"


namespace leveldb {

  static void UnrefWSN(const Slice& key, void* value) {
	   DBTransaction::WSNode* wsn = reinterpret_cast<DBTransaction::WSNode*>(value);
	   wsn->Unref();
  }

  DBTransaction::ReadSet::ReadSet() {

	max_length = 1024; //first allocate 1024 numbers
  	elems = 0;
	
	seqs = new RSSeqPair[max_length];
	hashes = new uint64_t[max_length];
	keys = new Key*[max_length];
  }

  DBTransaction::ReadSet::~ReadSet() {

	delete[] seqs;
	delete[] hashes;

	for(int i = 0; i < elems; i++)
		free(keys[i]);
	delete[] keys;
	
  }

void  DBTransaction::ReadSet::Resize() {
  	
	max_length = max_length * 2;

	RSSeqPair *ns = new RSSeqPair[max_length];
	uint64_t* nh = new uint64_t[max_length];
	Key** nk = new Key*[max_length];

	for(int i = 0; i < elems; i++) {
		ns[i] = seqs[i];
		nh[i] = hashes[i];
		nk[i] = keys[i];
	}

	delete[] seqs;
	delete[] hashes;
	delete[] keys;

	seqs = ns;
	hashes = nh;
	keys = nk;
	
  }
  
  void DBTransaction::ReadSet::Add(const Slice& key, uint64_t hash, uint64_t seq_addr)
  {

	assert(elems <= max_length);
	
	if(elems == max_length)
		Resize();

	int cur = elems;
	elems++;

	if(0 == seq_addr) {
		seqs[cur].seq = (uint64_t *)0;
		seqs[cur].oldseq = 0;
	} else {
		seqs[cur].seq = (uint64_t *)seq_addr;
		seqs[cur].oldseq = *seqs[cur].seq;
	}
	
	hashes[cur] = hash;

	Key* kp = reinterpret_cast<Key*>(
    	malloc(sizeof(Key)-1 + key.size()));

	kp->key_length = key.size();
	memcpy(kp->key_data, key.data(), key.size());

	keys[cur] = kp;
  }

  bool DBTransaction::ReadSet::Validate(HashTable* ht) {

	//This function should be protected by rtm or mutex
	
	for(int i = 0; i < elems; i++) {

		if(seqs[i].seq != NULL 
			&& seqs[i].oldseq != *seqs[i].seq)
			return false;

		if(seqs[i].seq == NULL) {
			
			//doesn't read any thing
			uint64_t curseq = 0; //Here must initialized as 0

			//TODO: we can just use the hash to find the key
			bool found = ht->Lookup(keys[i]->keySlice(),(void **)&curseq);
			
			if(seqs[i].oldseq != curseq) {
				assert(found);
				return false;
			}
		}
	}

	return true;
  }

  void DBTransaction::ReadSet::Print()
  {
	for(int i = 0; i < elems; i++) {
		printf("Key[%d] ", i);
		if(seqs[i].seq != NULL) {
			printf("Old Seq %ld Cur Seq %ld Seq Addr 0x%lx ", 
				seqs[i].oldseq, *seqs[i].seq, seqs[i].seq);
		}

		printf("key %s  ", keys[i]->keySlice());
		printf("hash %ld\n", hashes[i]);
	}
  }
 	
  DBTransaction::DBTransaction(HashTable* ht, MemTable* store, port::Mutex* mutex)
  {
	//get the globle store and versions passed by the parameter
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
	
	readset = new ReadSet();
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
	memcpy(wsn->value_data, value.data(), value.size());
	wsn->type = type;
	wsn->seq = 0;
	wsn->refs = 1;
	wsn->next = NULL;
	
	//Pass the deleter of wsnode into function
	wsn->knode = writeset->Insert(key, wsn, &UnrefWSN);
	wsn->knode->Ref();

	//Insert to the committed values linked list
	if(committedValues != NULL) {
		wsn->next = committedValues;
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

	HashTable::Node* node = latestseq_->GetNode(key);
	
	if ( NULL == node) {
		//even not found, still need to put the k into read set to avoid concurrent insertion
		readset->Add(key, Hash(key.data(), key.size(), 0), 0);
		
		return found;
	}

	seq = (uint64_t)node->value;
	
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
	readset->Add(key, node->hash, (uint64_t)&node->value);
	
	//printf("Get seq %ld value %s\n", seq, value->c_str());
	
	return found;
  }

  bool DBTransaction::Validation() {
	

	//writeset->PrintHashTable();	
	//RTMScope rtm(&rtmProf);
	MutexLock mu(storemutex);
	
	//step 1. check if the seq has been changed (any one change the value after reading)
	if( !readset->Validate(latestseq_))
		return false;
	
	int count = 0;
	//step 2.  update the the seq set 
	//can't use the iterator because the cur node may be deleted 

	for(int i = 0; i < writeset->length_; i++) {
		
    	HashTable::Node* ptr = writeset->list_[i];
    	while (ptr != NULL) {
			HashTable::Node *cur = ptr;
			//must get next before the following operation
       		ptr = ptr->next;

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
				cur->next = NULL;
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
	
	return true;
	
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


int main(){
	leveldb::DBTransaction::ReadSet rs;

	uint64_t arr[100];
	char key[100];

	for(int i = 0; i < 100; i++)  {
		arr[i] = i;
	}
	
	for(int i = 0; i < 100; i++) {
		snprintf(key, sizeof(key), "%d", i);

		rs.Add(leveldb::Slice(key), i, (uint64_t)&arr[i]);
	}

	for(int i = 0; i < 100; i++)  {
		arr[i] = i*i;
	}

	rs.Print();
	
	//printf("helloworld\n");

}




int main()
{
	leveldb::HashTable ht;
	char key[100];
		
	for(int i = 0; i < 100; i++) {
	
		snprintf(key, sizeof(key), "%d", i);

	//printf("Insert %s\n", *s);
		ht.Insert(leveldb::Slice(key), (void *)i,NULL);
	}

	ht.Update(leveldb::Slice(key), (void *)1000);
	
	for(int i = 0; i < 20; i++) {
	
		snprintf(key, sizeof(key), "%d", i);

	void* v;
	if(!ht.Lookup(leveldb::Slice(key), &v))
		printf("key %s Not Found\n", key);
	}
	
	
	ht.PrintHashTable();

	
	
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



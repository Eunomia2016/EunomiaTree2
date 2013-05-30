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

  DBTransaction::ReadSet::ReadSet() {

	max_length = 1024; //first allocate 1024 numbers
  	elems = 0;
	
	seqs = new RSSeqPair[max_length];
	keys = new Data*[max_length];
  }

  DBTransaction::ReadSet::~ReadSet() {

	delete[] seqs;

	for(int i = 0; i < elems; i++)
		free(keys[i]);
	delete[] keys;
	
  }

void  DBTransaction::ReadSet::Resize() {
  	
	max_length = max_length * 2;

	RSSeqPair *ns = new RSSeqPair[max_length];
	Data** nk = new Data*[max_length];

	for(int i = 0; i < elems; i++) {
		ns[i] = seqs[i];
		nk[i] = keys[i];
	}

	delete[] seqs;
	delete[] keys;

	seqs = ns;
	keys = nk;
	
  }
  
  void DBTransaction::ReadSet::Add(const Slice& key, uint64_t hash, uint64_t oldeseq, uint64_t seq_addr)
  {

	assert(elems <= max_length);
	
	if(elems == max_length)
		Resize();

	int cur = elems;
	elems++;

	seqs[cur].seq = (uint64_t *)seq_addr;
	seqs[cur].oldseq = oldeseq;
	seqs[cur].hash = hash;
	
	Data* kp = reinterpret_cast<Data*>(
    	malloc(sizeof(Data)-1 + key.size()));
	
	kp->length = key.size();
	memcpy(kp->contents, key.data(), key.size());

	keys[cur] = kp;
  }

  bool DBTransaction::ReadSet::Validate(HashTable* ht) {

	//This function should be protected by rtm or mutex
	
	for(int i = 0; i < elems; i++) {

		if(seqs[i].seq != NULL 
			&& seqs[i].oldseq != *seqs[i].seq)
			return false;
		//printf("[%lx Get Validate] old seq %ld seq %ld\n", pthread_self(), seqs[i].oldseq, *seqs[i].seq);
		if(seqs[i].seq == NULL) {
			
			//doesn't read any thing
			uint64_t curseq = 0; //Here must initialized as 0

			//TODO: we can just use the hash to find the key
			bool found = ht->GetMaxWithHash(seqs[i].hash, &curseq);
			
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

		printf("key %s  ", keys[i]->Getslice());
	}
  }


  DBTransaction::WriteSet::WriteSet() {

	max_length = 1024; //first allocate 1024 numbers
  	elems = 0;

	keys = new WSKey[max_length];;

	hashes = new uint64_t[max_length];
	values = new WSValue[max_length];
	
  }

  DBTransaction::WriteSet::~WriteSet() {

	delete[] hashes;

/*
	for(int i = 0; i < elems; i++)
		keys[i].Unref();
	delete[] keys;
*/

	for(int i = 0; i < elems; i++)
		free(values[i].val);
	delete[] values;
	
  }

void  DBTransaction::WriteSet::Resize() {
  	
	max_length = max_length * 2;

	uint64_t* nh = new uint64_t[max_length];
	WSKey* nk = new WSKey[max_length];;
	WSValue* nv = new WSValue[max_length];
	
	for(int i = 0; i < elems; i++) {
		nh[i] = hashes[i];
		nk[i] = keys[i];
		nv[i] = values[i];
	}

	delete[] hashes;
	delete[] keys;
	delete[] values;

	hashes = nh;
	keys = nk;
	values = nv;
  }
  
  void DBTransaction::WriteSet::Add(ValueType type, const Slice& key, 
  											uint64_t hash, const Slice& val)
  {

	assert(elems <= max_length);
	
	if(elems == max_length)
		Resize();

	int cur = elems;
	elems++;
	
	hashes[cur] = hash;

	//TODO: don't use the hashtable node, it is too big
	keys[cur].node.seq = 0;
	keys[cur].node.next = NULL;
	keys[cur].node.hash = hash;
	keys[cur].wseq = 0;
//	keys[cur].refs = 1;
	
	HashTable::Data* kp = reinterpret_cast<HashTable::Data*>(
    	malloc(sizeof(HashTable::Data)-1 + key.size()));

    kp->length = key.size();
    memcpy(kp->contents, key.data(), key.size());

	keys[cur].node.key = kp;

	Data* vp = reinterpret_cast<Data*>(
    	malloc(sizeof(Data)-1 + val.size()));	
	vp->length = val.size();
	memcpy(vp->contents, val.data(), val.size());

	
	values[cur].type = type;
	values[cur].val= vp;
		
  }

  void DBTransaction::WriteSet::UpdateGlobalSeqs(HashTable* ht) {

	//This function should be protected by rtm or mutex
	
	uint64_t seq = 0;
	for(int i = 0; i < elems; i++) {
		seq = 0;
		bool found = ht->GetMaxWithHash(keys[i].node.hash, &seq);
		
		if(!found) {
			//The node is inserted into the list first time
			seq = 1;
			//Still use the node in the write set to avoid memory allocation
			keys[i].node.seq = seq;
			ht->InsertNode(&keys[i].node);	
		}
		else {			
			seq++;		
			ht->UpdateWithHash(keys[i].node.hash,seq);
		}
		
		keys[i].wseq= seq;

	}

  }

  bool DBTransaction::WriteSet::Lookup(const Slice& key, ValueType* type, Slice* val) 
  {
	  for(int i = 0; i < elems; i++) {
		if(keys[i].node.key->Getslice()== key) {
			*type = values[i].type;
			*val = values[i].val->Getslice();
			return true;
		}
	  }

	  return false;
  }

  void DBTransaction::WriteSet::Commit(MemTable *memstore) 
  {
	//commit the local write set into the memory storage
	//should holde the mutex of memstore
	for(int i = 0; i < elems; i++) {
		//printf("[%lx Commit] Insert Seq %ld Value %s\n", pthread_self(), seqs[i], values[i].val->Getslice());
		memstore->Add(keys[i].wseq, values[i].type, keys[i].node.key->Getslice(), values[i].val->Getslice());
	}
  }

  void DBTransaction::WriteSet::Print()
  {
	for(int i = 0; i < elems; i++) {
		/*
		printf("Key[%d] ", i);
		if(seqs[i].seq != NULL) {
			printf("Old Seq %ld Cur Seq %ld Seq Addr 0x%lx ", 
				seqs[i].oldseq, *seqs[i].seq, seqs[i].seq);
		}

		printf("key %s  ", keys[i]->Getslice());
		printf("hash %ld\n", hashes[i]);
		*/
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

	readset = new ReadSet();
	writeset = new WriteSet();
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
	uint64_t h = Hash(key.data(), key.size(), 0);
	writeset->Add(type, key, h, value);
  }

  bool DBTransaction::Get(const Slice& key, std::string* value, Status* s)
  {
  	//step 1. First check if the <k,v> is in the write set
  	
	Slice val;
	ValueType type;
	if(writeset->Lookup(key, &type, &val)) {
		//Found
		switch (type) {
          case kTypeValue: {
          	value->assign(val.data(), val.size());
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
		readset->Add(key, Hash(key.data(), key.size(), 0), seq, 0);
		
		return found;
	}

	seq = node->seq;
	
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
	readset->Add(key, node->hash, seq, (uint64_t)&node->seq);
	
	return found;
  }

  bool DBTransaction::Validation() {
	

	//writeset->PrintHashTable();	
	RTMScope rtm(&rtmProf);
	//MutexLock mu(storemutex);
	
	//step 1. check if the seq has been changed (any one change the value after reading)
	if( !readset->Validate(latestseq_))
		return false;
	
	
	//step 2.  update the the seq set 
	//can't use the iterator because the cur node may be deleted 
	writeset->UpdateGlobalSeqs(latestseq_);
	
	return true;
	
  }


  
  void DBTransaction::GlobalCommit() {
	//commit the local write set into the memory storage		
	storemutex->Lock();
	writeset->Commit(memstore_);
	storemutex->Unlock();
	
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



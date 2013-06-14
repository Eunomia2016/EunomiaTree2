// Copyright (c) 2011 The LevelDB Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file. See the AUTHORS file for names of contributors.

#ifndef STORAGE_LEVELDB_DB_DBTRANSACTIONTEMPLATE_H_
#define STORAGE_LEVELDB_DB_DBTRANSACTIONTEMPLATE_H_

#include <string>
#include "leveldb/db.h"
#include "db/dbformat.h"
#include "db/hashtable_template.h"
#include "port/port_posix.h"
#include "util/txprofile.h"
#include "db/txmemstore_template.h"



namespace leveldb {

template<typename Key, typename Value, class HashFunction, class Comparator>
class DBTransaction {
 public:

	RTMProfile rtmProf;
	int count;

	DBTransaction(HashTable<Key, HashFunction, Comparator>* ht, 
		TXMemStore<Key, Value, Comparator>* store, Comparator comp);
	
	~DBTransaction();

	void Begin();
	bool Abort();
	bool End();
	void Add(ValueType type, Key* key, Value* val);
	bool Get(Key* key, Value** value, Status* s);

	
public:

 	class ReadSet {

	  struct RSSeqPair {
	    uint64_t oldseq; //seq got when read the value
	    uint64_t *seq; //pointer to the global memory location
		uint64_t hash;
	  };
		
	  private:
	    int max_length;
	    int elems;

	    RSSeqPair *seqs;
	    void Resize();
			
	  public:
	    ReadSet();
	    ~ReadSet();			
	    void Add(uint64_t hash, uint64_t oldeseq, uint64_t *ptr);
	    bool Validate(HashTable<Key, HashFunction, Comparator>* ht);
	    void Print();
	};


	class WriteSet {
		
		struct WSKV{
			ValueType type; //seq got when read the value
			Key *key; //pointer to the written key 
			Value *val;
		};
		
		struct WSSeqPair {
			uint64_t wseq; //seq of the written key
			uint64_t *seqptr; //pointer to the sequence number memory location
		};
		
	  private:

		int cacheset[64];
		uint64_t cacheaddr[64][8];
		uint64_t cachetypes[64][8];
		int max_length;
		int elems;

		WSSeqPair *seqs;
		WSKV *kvs;

		void Resize();
			
	  public:
		WriteSet();
		~WriteSet();	
		void TouchAddr(uint64_t addr, int type);
		
		void Add(ValueType type, Key* key, Value* val, uint64_t* seqptr);
		void UpdateGlobalSeqs();
		bool Lookup(Key* key, ValueType* type, Value** val, Comparator& cmp);
		
		void Commit(TXMemStore<Key, Value, Comparator> *memstore);
		void Print();
	};

//	HashTable *readset;
	ReadSet* readset;
	WriteSet *writeset;
	
	port::Mutex* storemutex;
	HashTable<Key, HashFunction, Comparator> *latestseq_ ;
	TXMemStore<Key, Value, Comparator> *txdb_ ;

	Comparator comp_;
	
  	bool Validation();
	void GlobalCommit();
};


template<typename Key, typename Value, class HashFunction, class Comparator>
DBTransaction<Key, Value, HashFunction, Comparator>::ReadSet::ReadSet() 
{
	max_length = 1024;
	elems = 0;	seqs = new RSSeqPair[max_length];
 	 
}

template<typename Key, typename Value, class HashFunction, class Comparator>
DBTransaction<Key, Value, HashFunction, Comparator>::ReadSet::~ReadSet() {
	delete[] seqs;	
}

template<typename Key, typename Value, class HashFunction, class Comparator>
void DBTransaction<Key, Value, HashFunction, Comparator>::ReadSet::Resize() {
	
  max_length = max_length * 2;

  RSSeqPair *ns = new RSSeqPair[max_length];


  for(int i = 0; i < elems; i++) {
	ns[i] = seqs[i];
  }

  delete[] seqs;

  seqs = ns;
}


template<typename Key, typename Value, class HashFunction, class Comparator>
void DBTransaction<Key, Value, HashFunction, Comparator>::ReadSet::Add(
											uint64_t hash, uint64_t oldeseq, uint64_t *ptr)
{
  if (max_length < elems) printf("ELEMS %d MAX %d\n", elems, max_length);
  assert(elems <= max_length);

  if(elems == max_length)
    Resize();

  int cur = elems;
  elems++;

  seqs[cur].seq = ptr;
  seqs[cur].oldseq = oldeseq;
  seqs[cur].hash = hash;
}

template<typename Key, typename Value, class HashFunction, class Comparator>
bool DBTransaction<Key, Value, HashFunction, Comparator>::ReadSet::Validate(
 											HashTable<Key, HashFunction, Comparator>* ht) {

//This function should be protected by rtm or mutex

for(int i = 0; i < elems; i++) {

	if(seqs[i].seq != NULL 
		&& seqs[i].oldseq != *seqs[i].seq)
		return false;
	//printf("[%lx Get Validate] old seq %ld seq %ld\n", pthread_self(), seqs[i].oldseq, *seqs[i].seq);
	if(seqs[i].seq == NULL) {
		
		//doesn't read any thing
		uint64_t curseq = 0; //Here must initialized as 0

		//we can just use the hash to find the key
		bool found = ht->GetMaxWithHash(seqs[i].hash, &curseq);
		
		if(seqs[i].oldseq != curseq) {
			assert(found);
			return false;
		}
	}
}

return true;
}

template<typename Key, typename Value, class HashFunction, class Comparator>
void DBTransaction<Key, Value, HashFunction, Comparator>::ReadSet::Print()
{
  for(int i = 0; i < elems; i++) {
    printf("Key[%d] ", i);
    if(seqs[i].seq != NULL) {
      printf("Old Seq %ld Cur Seq %ld Seq Addr 0x%lx ", 
	  	seqs[i].oldseq, *seqs[i].seq, seqs[i].seq);
    }
   }
}


template<typename Key, typename Value, class HashFunction, class Comparator>
DBTransaction<Key, Value, HashFunction, Comparator>::WriteSet::WriteSet() {

  max_length = 1024; //first allocate 1024 numbers
  elems = 0;

  seqs = new WSSeqPair[max_length];
  kvs = new WSKV[max_length];

  for(int i = 0; i < 64; i++) {
	cacheset[i] = 0;
	for(int j = 0; j < 8; j++) {
		cacheaddr[i][j] = 0;
		cachetypes[i][j] = 0;
	}
  }

}

template<typename Key, typename Value, class HashFunction, class Comparator>
DBTransaction<Key, Value, HashFunction, Comparator>::WriteSet::~WriteSet() {
	//FIXME: Do nothing here
}

template<typename Key, typename Value, class HashFunction, class Comparator>
void DBTransaction<Key, Value, HashFunction, Comparator>::WriteSet::Resize() {
	
  max_length = max_length * 2;

  WSSeqPair* nss = new WSSeqPair[max_length];
  WSKV* nkv = new WSKV[max_length];

  for(int i = 0; i < elems; i++) {
	nss[i] = seqs[i];
	nkv[i] = kvs[i];
  }

  delete[] seqs;
  delete[] kvs;

  seqs = nss;
  kvs = nkv;
}

template<typename Key, typename Value, class HashFunction, class Comparator>
void DBTransaction<Key, Value, HashFunction, Comparator>::WriteSet::Add(
										ValueType type, Key* key,
										Value* val, uint64_t *seqptr)
{
  assert(elems <= max_length);

  if(elems == max_length) {
	printf("Resize\n");
	Resize();

  }

  int cur = elems;
  elems++;

  kvs[cur].key = key;
  kvs[cur].val = val;
  kvs[cur].type = type;

  seqs[cur].seqptr = seqptr;
  seqs[cur].wseq = 0;

//TouchAddr((uint64_t)seqptr, 3);
	
}

template<typename Key, typename Value, class HashFunction, class Comparator>
void DBTransaction<Key, Value, HashFunction, Comparator>::WriteSet::TouchAddr(
																	uint64_t addr, int type)
{
	
  uint64_t caddr = addr >> 12;
  int index = (int)((addr>>6)&0x3f);

  for(int i = 0; i < 8; i++) {

    if(cacheaddr[index][i] == caddr)
	  return;

  }
 
  cacheset[index]++;
  static int count = 0;
  if( cacheset[index] > 8) {
    count++;
    printf("Cache Set [%d] Conflict type %d\n", index ,type );
	for(int i = 0; i < 8; i++) { 
	  printf("[%d][%lx] ", cachetypes[index][i], cacheaddr[index][i]);
	}
	printf(" %d \n", count);
  }

  for(int i = 0; i < 8; i++) {
    if(cacheaddr[index][i] == 0) {
 	  cacheaddr[index][i] = caddr;
	  cachetypes[index][i] = type;
	  return;
 	}
  }
}


template<typename Key, typename Value, class HashFunction, class Comparator>
void DBTransaction<Key, Value, HashFunction, Comparator>::WriteSet::UpdateGlobalSeqs() 
{

//This function should be protected by rtm or mutex

  for(int i = 0; i < elems; i++) {
    seqs[i].wseq = *seqs[i].seqptr;
    seqs[i].wseq++;
    *seqs[i].seqptr = seqs[i].wseq;

//	TouchAddr((uint64_t)&seqs[i].wseq, 1);
//	TouchAddr((uint64_t)&seqs[i].seqptr, 2);
//	TouchAddr((uint64_t)seqs[i].seqptr, 3);
	
  }

}

template<typename Key, typename Value, class HashFunction, class Comparator>
bool DBTransaction<Key, Value, HashFunction, Comparator>::WriteSet::Lookup(
									Key* key, ValueType* type, Value** val, Comparator& cmp) 
{
  for(int i = 0; i < elems; i++) {
	if(cmp(*kvs[i].key , *key) == 0) {
		*type = kvs[i].type;
		*val = kvs[i].val;
		return true;
	}
  }

  return false;
}

template<typename Key, typename Value, class HashFunction, class Comparator>
void DBTransaction<Key, Value, HashFunction, Comparator>::WriteSet::Commit(
											  TXMemStore<Key, Value, Comparator> *memstore) 
{
  //commit the local write set into the memory storage
  //should holde the mutex of memstore
  for(int i = 0; i < elems; i++)
	memstore->Put(kvs[i].key, kvs[i].val, seqs[i].wseq);
  
}

template<typename Key, typename Value, class HashFunction, class Comparator>
void DBTransaction<Key, Value, HashFunction, Comparator>::WriteSet::Print()
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


template<typename Key, typename Value, class HashFunction, class Comparator>
DBTransaction<Key, Value, HashFunction, Comparator>::DBTransaction(
HashTable<Key, HashFunction, Comparator>* ht, 
TXMemStore<Key, Value, Comparator>* store, Comparator comp)
{
  //get the globle store and versions passed by the parameter
  storemutex = new port::Mutex();
  latestseq_ = ht;
  txdb_ = store;

  readset = NULL;
  writeset = NULL;

  count = 0;
}

template<typename Key, typename Value, class HashFunction, class Comparator>
DBTransaction<Key, Value, HashFunction, Comparator>::~DBTransaction()
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

template<typename Key, typename Value, class HashFunction, class Comparator>
void DBTransaction<Key, Value, HashFunction, Comparator>::Begin()
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

template<typename Key, typename Value, class HashFunction, class Comparator>
bool DBTransaction<Key, Value, HashFunction, Comparator>::Abort()
{
  return false;
}

template<typename Key, typename Value, class HashFunction, class Comparator>
bool DBTransaction<Key, Value, HashFunction, Comparator>::End()
{
  if( !Validation())
    return false;
  
  GlobalCommit();
  return true;
}

template<typename Key, typename Value, class HashFunction, class Comparator>
void DBTransaction<Key, Value, HashFunction, Comparator>::Add(
													ValueType type, Key* key, Value* value)
{
	//Get the seq addr from the hashtable
   typename HashTable<Key, HashFunction, Comparator>::Node* node 
   													= latestseq_->GetNodeWithInsert(key);
    //write the key value into local buffer
     writeset->Add(type, key, value, &node->seq);
}


template<typename Key, typename Value, class HashFunction, class Comparator>
bool DBTransaction<Key, Value, HashFunction, Comparator>::Get(
														Key* key, Value** value, Status* s)
{
  //step 1. First check if the <k,v> is in the write set
	
  ValueType type;
  if(writeset->Lookup(key, &type, value, comp_)) {
	//Found
	switch (type) {
      case kTypeValue: {
	  	*s = Status::OK();
      	return true;
      }
      case kTypeDeletion:
      	*s = Status::NotFound(Slice());
      	return true;
  	}	
  }


  //step 2.  Read the <k,v> from the in memory store

  uint64_t seq = 0;

  typename HashTable<Key, HashFunction, Comparator>::Node* node = latestseq_->GetNode(key);

  if ( NULL == node) {
	//even not found, still need to put the k into read set to avoid concurrent insertion
	readset->Add(node->hash, seq, (uint64_t *)0);
	*s = Status::NotFound(Slice());
	return false;
  }

  seq = node->seq;

  //This is an empty node (garbage)
  if(seq == 0) {
	*s = Status::NotFound(Slice());
	return false;
  }

  Status res;
  //may be not found, should wait for a while
  int count = 0;
  do{
	
	res = txdb_->Get(key, value, seq);

   }while(res.IsNotFound());

// step 3. put into the read set
  readset->Add(node->hash, seq, &node->seq);

  *s = Status::OK();
  return true;
}

template<typename Key, typename Value, class HashFunction, class Comparator>
bool DBTransaction<Key, Value, HashFunction, Comparator>::Validation() {


//writeset->PrintHashTable();	
 //RTMScope rtm(&rtmProf);
  MutexLock mu(storemutex);

  //step 1. check if the seq has been changed (any one change the value after reading)
  if( !readset->Validate(latestseq_))
    return false;


  //step 2.  update the the seq set 
  //can't use the iterator because the cur node may be deleted 
  writeset->UpdateGlobalSeqs();

  return true;

}



template<typename Key, typename Value, class HashFunction, class Comparator>
void DBTransaction<Key, Value, HashFunction, Comparator>::GlobalCommit() {

  //commit the local write set into the memory storage		
  writeset->Commit(txdb_);

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

#endif  // STORAGE_LEVELDB_DB_MEMTABLE_H_

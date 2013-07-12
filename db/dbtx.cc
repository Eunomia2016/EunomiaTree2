// Copyright (c) 2011 The LevelDB Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file. See the AUTHORS file for names of contributors.

#ifndef STORAGE_LEVELDB_DB_DBTRANSACTIONTEMPLATE_H_
#define STORAGE_LEVELDB_DB_DBTRANSACTIONTEMPLATE_H_

#include <string>
#include "leveldb/db.h"
#include "util/rtm.h"

#include "db/dbformat.h"
#include "db/dbtx.h"
#include "port/port_posix.h"
#include "util/txprofile.h"
#include "util/spinlock.h"
#include "db/txmemstore_template.h"

#define CACHESIM 0
#define GLOBALOCK 0

namespace leveldb {

port::Mutex DBTX::storemutex;

SpinLock DBTX::slock;

__thread DBTX::WriteSet* DBTX::writeset = NULL;
__thread DBTX::ReadSet* DBTX::readset = NULL;


void DBTX::ThreadLocalInit()
{

	if(readset == NULL)
	  readset = new ReadSet();
	
	if(writeset == NULL)
	  writeset = new WriteSet();
	
}


DBTX::ReadSet::ReadSet() 
{
	max_length = 64;
	elems = 0;	
	seqs = new RSSeqPair[max_length];	 
}

DBTX::ReadSet::~ReadSet() 
{
	delete[] seqs;	
}

inline void DBTX::ReadSet::Reset() 
{
	elems = 0;
}

void DBTX::ReadSet::Resize() 
{
	
  max_length = max_length * 2;

  RSSeqPair *ns = new RSSeqPair[max_length];


  for(int i = 0; i < elems; i++) {
	ns[i] = seqs[i];
  }

  delete[] seqs;

  seqs = ns;
}


inline void DBTX::ReadSet::Add(uint64_t *ptr)
{
  if (max_length < elems) printf("ELEMS %d MAX %d\n", elems, max_length);
  assert(elems <= max_length);

  if(elems == max_length)
    Resize();

  int cur = elems;
  elems++;

  seqs[cur].seq = *ptr;
  seqs[cur].seqptr = ptr;
}

inline bool DBTX::ReadSet::Validate() 
{

  //This function should be protected by rtm or mutex
  for(int i = 0; i < elems; i++) {
  	assert(seqs[i].seqptr != NULL);
	if(seqs[i].seq != *seqs[i].seqptr) {
		return false;
	}
  }

  return true;
}

void DBTX::ReadSet::Print()
{
  for(int i = 0; i < elems; i++) {
    printf("Key[%d] ", i);
    printf("Old Seq %ld Cur Seq %ld Seq Addr 0x%lx ", 
	  	seqs[i].seq, *seqs[i].seqptr, seqs[i].seqptr);
    
   }
}


DBTX::WriteSet::WriteSet() 
{
  max_length = 64; //first allocate 1024 numbers
  elems = 0;

  kvs = new WSKV[max_length];

#if CACHESIM
  for(int i = 0; i < 64; i++) {
	cacheset[i] = 0;
	for(int j = 0; j < 8; j++) {
		cacheaddr[i][j] = 0;
		cachetypes[i][j] = 0;
	}
  }
#endif

}

DBTX::WriteSet::~WriteSet() {
	//FIXME: Do nothing here
}

void DBTX::WriteSet::Resize() {
	
  max_length = max_length * 2;
  WSKV* nkv = new WSKV[max_length];

  for(int i = 0; i < elems; i++) {
	nkv[i] = kvs[i];
  }

  delete[] kvs;

  kvs = nkv;
}

void DBTX::WriteSet::Reset() 
{
	elems = 0;
#if CACHESIM
	for(int i = 0; i < 64; i++) {
	    cacheset[i] = 0;
		for(int j = 0; j < 8; j++) {
			cacheaddr[i][j] = 0;
			cachetypes[i][j] = 0;
		}
    }
#endif
}

void DBTX::WriteSet::TouchAddr(uint64_t addr, int type)
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


void DBTX::WriteSet::Add(uint64_t key, uint64_t* val, MemStoreSkipList::Node* node)
{
  assert(elems <= max_length);

  if(elems == max_length) {
	printf("Resize\n");
	Resize();

  }

  int cur = elems;
  elems++;

  kvs[cur].key = node->key;
  kvs[cur].val = val;
  kvs[cur].node = node;

  //Allocate the dummy node
  //FIXME: Just allocate the dummy node as 1 height
  kvs[cur].dummy = MemStoreSkipList::NewNode(key, 1);
  kvs[cur].dummy->value = val;
  kvs[cur].dummy->counter = 0;
  kvs[cur].dummy->next_[0] = NULL;
	
}


inline bool DBTX::WriteSet::Lookup(uint64_t key, uint64_t** val)
{
  for(int i = 0; i < elems; i++) {
    if(kvs[i].key == key) {
	   *val = kvs[i].val;
	   return true;
    }
  }
  
  return false;
  
}

//gcounter should be added into the rtm readset
inline void DBTX::WriteSet::Write(uint64_t gcounter)
{
  
  for(int i = 0; i < elems; i++) {
  	
    if(kvs[i].node->counter == gcounter) {
		
	  //If counter of the node is equal to the global counter, then just change the value pointer
	 
	  //FIXME: the old value should be deleted eventually
	  kvs[i].node->value = kvs[i].val;

	  //Should first update the value, then the seq, to guarantee the seq is always older than the value
	  kvs[i].node->seq++;
		
	} else {
	  //If global counter is updated, we need to find the node with the global counter
	  
	  MemStoreSkipList::Node* cur = kvs[i].node;
	  
	  while(cur->next_[0]!=NULL 
	  		&& cur->next_[0]->key != cur->key
	  		&& cur->counter != gcounter) {
		cur = cur->next_[0];
	  }

	   //if node is found, just update the seq number and value
	   if(cur->counter == gcounter) {
		 //FIXME: the old value should be deleted eventually
	     cur->value = kvs[i].val;
		 cur->seq++;
	   } else {
	     //if node is not found, insert the dummy node but also need to update cur sequence

		 cur->seq++;
		 kvs[i].dummy->seq = 1;
		 kvs[i].dummy->counter = gcounter;
		 //FIXME: just insert the dummy node into the 1st layer
		 kvs[i].dummy->next_[0] = cur->next_[0];
		 cur->next_[0] = kvs[i].dummy;
	   }
	}
  }
}
		


void DBTX::WriteSet::Print()
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

DBTX::DBTX(MemStoreSkipList* store)
{
  txdb_ = store;
  count = 0;
}

DBTX::~DBTX()

{
  //clear all the data
}	

void DBTX::Begin()
{
//reset the local read set and write set
  txdb_->ThreadLocalInit();
  ThreadLocalInit();
  readset->Reset();
  writeset->Reset();
  
  
}

bool DBTX::Abort()
{
  //FIXME: clear all the garbage data
  readset->Reset();
  writeset->Reset();
  
  return false;
}

bool DBTX::End()
{
#if GLOBALOCK
  MutexLock lock(&storemutex);
#else
  RTMScope rtm(&rtmProf);
#endif
  
  if( !readset->Validate()) {
  	  return false;
   }
 
  //step 2.  update the the seq set 
  //can't use the iterator because the cur node may be deleted 
  writeset->Write(txdb_->snapshot);
  return true;
}

void DBTX::Add(uint64_t key, uint64_t* val)
{
  MemStoreSkipList::Node* node;
  //Get the seq addr from the hashtable

  node = txdb_->GetLatestNodeWithInsert(key);
  
  
  //write the key value into local buffer
  writeset->Add(key, val, node);
}


void DBTX::Delete(uint64_t key)
{
	//For delete, just insert a null value
	Add(key, NULL);
}

bool DBTX::Get(uint64_t key, uint64_t** val)
{
  //step 1. First check if the <k,v> is in the write set
  if(writeset->Lookup(key, val)) {
      	return true;
  }


  //step 2.  Read the <k,v> from the in memory store
  MemStoreSkipList::Node* node = txdb_->GetLatestNodeWithInsert(key);

  //Guarantee   
#if GLOBALOCK
	MutexLock lock(&storemutex);
#else
	RTMScope rtm(&rtmProf);
#endif


  readset->Add(&node->seq);
  
  if ( node->value == NULL ) {

 	*val = NULL;
	return false;
	
  } else {
  
	assert(node->value != NULL);
	*val = node->value;
	return true;
	
  }

  return true;
}



}  // namespace leveldb

#endif  // STORAGE_LEVELDB_DB_MEMTABLE_H_

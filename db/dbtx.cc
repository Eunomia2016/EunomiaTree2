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
#include "util/mutexlock.h"



namespace leveldb {

port::Mutex DBTX::storemutex;

SpinLock DBTX::slock;

__thread DBTX::WriteSet* DBTX::writeset = NULL;
__thread DBTX::ReadSet* DBTX::readset = NULL;

#define MAXSIZE 4*1024*1024 // the initial read/write set size

void DBTX::ThreadLocalInit()
{

	if(readset == NULL)
	  readset = new ReadSet();
	
	if(writeset == NULL)
	  writeset = new WriteSet();
	
}


DBTX::ReadSet::ReadSet() 
{
	max_length = MAXSIZE;
	elems = 0;	
	seqs = new RSSeqPair[max_length];

	rangeElems = 0;
	nexts = new RSSuccPair[max_length];

	//pretouch to avoid page fault in the rtm
	for(int i = 0; i < max_length; i++) {
		seqs[i].seq = 0;
		seqs[i].seqptr = NULL;
		nexts[i].next = 0;
		nexts[i].nextptr = NULL;
	}
		
}

DBTX::ReadSet::~ReadSet() 
{
	delete[] seqs;
	delete[] nexts;
}

inline void DBTX::ReadSet::Reset() 
{
	elems = 0;
	rangeElems = 0;
}

void DBTX::ReadSet::Resize() 
{
  printf("Read Set Resize\n");
  max_length = max_length * 2;

  RSSeqPair *ns = new RSSeqPair[max_length];
  for(int i = 0; i < elems; i++) {
	ns[i] = seqs[i];
  }
  delete[] seqs;
  seqs = ns;

  
  RSSuccPair* nts = new RSSuccPair[max_length];
  for(int i = 0; i < rangeElems; i++) {
	nts[i] = nexts[i];
  }
  delete[] nexts;
  nexts = nts;
  
}


inline void DBTX::ReadSet::AddNext(uint64_t *ptr, uint64_t value)
{
//if (max_length < rangeElems) printf("ELEMS %d MAX %d\n", rangeElems, max_length);
  assert(rangeElems <= max_length);

  if(rangeElems == max_length) {
    Resize();
  }

  int cur = rangeElems;
  rangeElems++;

  nexts[cur].next = value;
  nexts[cur].nextptr = ptr;
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
  //Check if any tuple read has been modified
  for(int i = 0; i < elems; i++) {
  	assert(seqs[i].seqptr != NULL);
	if(seqs[i].seq != *seqs[i].seqptr) {
		return false;
	}
  }

  //Check if any tuple has been inserted in the range
  for(int i = 0; i < rangeElems; i++) {
  	assert(nexts[i].nextptr != NULL);
	if(nexts[i].next != *nexts[i].nextptr) {

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
  max_length = MAXSIZE; //first allocate 1024 numbers
  elems = 0;
  cursindex = 0;
  
  kvs = new WSKV[max_length];
  sindexes = new WSSEC[max_length];

  for(int i = 0; i < max_length; i++) {
		kvs[i].key = 0;
		kvs[i].val = NULL;
		kvs[i].node = NULL;
		kvs[i].dummy = NULL;
  }
  
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
  
  printf("Write Set Resize\n");
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


void DBTX::WriteSet::Add(int tableid, uint64_t key, uint64_t* val, Memstore::MemNode* node)
{
  assert(elems <= max_length);

  if(elems == max_length) {
	Resize();
  }

  int cur = elems;
  elems++;

  kvs[cur].tableid = tableid;
  kvs[cur].key = key;
  kvs[cur].val = val;
  kvs[cur].node = node;

  //Allocate the dummy node
  //FIXME: Just allocate the dummy node as 1 height
  kvs[cur].dummy = Memstore::GetMemNode();
  
}

inline void DBTX::WriteSet::Add(uint64_t *seq, SecondIndex::MemNodeWrapper* mnw)
{
	if(cursindex == max_length) {
	  //FIXME: No resize support!
	  printf("Error: sindex overflow\n");
    }

	sindexes[cursindex].seq = seq;
	sindexes[cursindex].sindex = mnw;

	cursindex++;
}


inline bool DBTX::WriteSet::Lookup(int tableid, uint64_t key, uint64_t** val)
{
  for(int i = 0; i < elems; i++) {
    if(kvs[i].tableid == tableid && kvs[i].key == key) {
	   *val = kvs[i].val;
	   return true;
    }
  }
  
  return false;
  
}


inline void DBTX::WriteSet::UpdateSecondaryIndex()
{
	for(int i = 0; i < cursindex; i++) {
		//1. logically delete the old secondary index
		if(sindexes[cursindex].sindex->memnode->secIndexValidateAddr != NULL)
			*sindexes[cursindex].sindex->memnode->secIndexValidateAddr != false;

		//2. update the new secondary index
		sindexes[cursindex].sindex->valid = true;
		sindexes[cursindex].sindex->memnode->secIndexValidateAddr 
					= &sindexes[cursindex].sindex->valid;
		*sindexes[cursindex].seq += 1;
	}
}

//gcounter should be added into the rtm readset
inline void DBTX::WriteSet::Write(uint64_t gcounter)
{

  for(int i = 0; i < elems; i++) {

#if GLOBALOCK
	assert(kvs[i].node->counter <= gcounter);
#endif

    if(kvs[i].node->counter == gcounter) {
		
	  //If counter of the node is equal to the global counter, then just change the value pointer
	 
	  //FIXME: the old value should be deleted eventually
	  kvs[i].node->value = kvs[i].val;

	  //Should first update the value, then the seq, to guarantee the seq is always older than the value
	  kvs[i].node->seq++;
		
	} else if(kvs[i].node->counter < gcounter){

	  //If global counter is updated, just update the counter and store a old copy into the dummy node
	  //Clone the current value into dummy node
	  kvs[i].dummy->value = kvs[i].node->value;
	  kvs[i].dummy->counter = kvs[i].node->counter;
	  kvs[i].dummy->seq = kvs[i].node->seq; //need this ?
	  kvs[i].dummy->oldVersions = kvs[i].node->oldVersions;

	  //update the current node
	  kvs[i].node->oldVersions = kvs[i].dummy;
	  kvs[i].node->counter = gcounter;
	  	  
	  kvs[i].node->value = kvs[i].val;
	  kvs[i].node->seq++;
	  
//	  printf("[WS] write key %ld counter %d on snapshot %d\n", cur->key, cur->counter, gcounter);
	   
	} else {
	  //Error Check: Shouldn't arrive here
	  while(_xtest()) _xend();
	  assert(0);
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

DBTX::DBTX(DBTables* store)
{
  txdb_ = store;
  count = 0;
  abort = false;
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
  abort = false;
  return abort;
}

bool DBTX::End()
{
  if (abort) return false;
#if GLOBALOCK
  SpinLockScope spinlock(&slock);
#else
  RTMScope rtm(&rtmProf);
#endif

  if(!readset->Validate()) {
  	  return false;
  }
  
  
 
  //step 2.  update the the seq set 
  //can't use the iterator because the cur node may be deleted 
  writeset->Write(txdb_->snapshot);

  //step 3. update the sencondary index
  writeset->UpdateSecondaryIndex();
  
  return true;
}


void DBTX::Add(int tableid, uint64_t key, uint64_t* val)

{
  Memstore::MemNode* node;
  //Get the seq addr from the hashtable
  node = txdb_->tables[tableid]->GetWithInsert(key);
  //printf("[DBTX] insert key %lx\n", key);
  //write the key value into local buffer
  if(node == 0)
  	printf("get zero value!!!\n");
  
  writeset->Add(tableid, key, val, node);
}

//Update a column which has a secondary key
void DBTX::Add(int tableid, int indextableid, uint64_t key, uint64_t seckey, uint64_t* val)
{
	uint64_t *seq;
	//1. get the memnode wrapper of the secondary key
	SecondIndex::MemNodeWrapper* mw =  
		txdb_->secondIndexes[indextableid]->GetWithInsert(seckey, key, &seq);
	
	//2. add the record seq number into write set
	writeset->Add(tableid, key, val, mw->memnode);
	
	//3. add the seq number of the second node and the validation flag address into the write set
	writeset->Add(seq, mw);
}


void DBTX::Delete(int tableid, uint64_t key)
{
	//For delete, just insert a null value
	Add(tableid, key, NULL);
}

DBTX::KeyValues* DBTX::GetByIndex(int indextableid, uint64_t seckey)
{
	assert(txdb_->secondIndexes[indextableid] != NULL);
	SecondIndex::SecondNode* sn = txdb_->secondIndexes[indextableid]->Get(seckey);
	assert(sn != NULL);
	
	//FIXME: the seq number maybe much larger than the real number of nodes
	uint64_t knum = sn->seq;

	if(knum == 0)
		return NULL;
	
	KeyValues* kvs = new KeyValues(knum);

	
#if GLOBALOCK
	SpinLockScope spinlock(&slock);
#else
	RTMScope rtm(&rtmProf);
#endif

	//FIXME: the number of node may be changed before the RTM acquired
	if(knum != sn->seq) {
		while(_xtest())
			_xend();
		printf("[GetByIndex] Error OCCURRED\n");
	}

	//1.  put the seq into the readset
	readset->Add(&sn->seq);

	//2. get every record and put the record seq into the readset
	int i = 0;
	SecondIndex::MemNodeWrapper* mnw = sn->head;
	while(mnw != NULL) {
		kvs->keys[i] = mnw->key;
		kvs->values[i] = mnw->memnode->value;
		i++;
	}

	if(i > knum) {
		while(_xtest())
			_xend();
		printf("[GetByIndex] Error OCCURRED\n");
	}
	
	kvs->num = i;

	return kvs;
	
}


bool DBTX::Get(int tableid, uint64_t key, uint64_t** val)
{
  //step 1. First check if the <k,v> is in the write set
  if(writeset->Lookup(tableid, key, val)) {
      	return true;
  }

	
  //step 2.  Read the <k,v> from the in memory store
  Memstore::MemNode* node = txdb_->tables[tableid]->GetWithInsert(key);

	
	//Guarantee	
#if GLOBALOCK
	SpinLockScope spinlock(&slock);
#else
	RTMScope rtm(&rtmProf);
#endif

//	if(*val != NULL && **val == 1)
	assert(node != NULL);
    readset->Add(&node->seq);

	if (node->value == NULL) {

		*val = NULL;
//		txdb_->tables[tableid]->PrintStore();
	//    printf("Not Found %d\n", node->seq);
		return false;

	} else {

		*val = node->value;
		return true;
	}


}

DBTX::Iterator::Iterator(DBTX* tx, int tableid)
{
	tx_ = tx;
	table_ = tx->txdb_->tables[tableid];
	iter_ = table_->GetIterator();
	cur_ = NULL;
	prev_link = NULL;
}
	
bool DBTX::Iterator::Valid()
{
	return cur_ != NULL;
}
	

uint64_t DBTX::Iterator::Key()
{
	return iter_->Key();
}

uint64_t* DBTX::Iterator::Value()
{
	return val_;
}
	
void DBTX::Iterator::Next()
{
	bool r = iter_->Next();

#if AGGRESSIVEDETECT
	if (!r) {
		tx_->abort = true;
		cur_ = NULL;
		return;
	}
#endif

	while(iter_->Valid()) {
	  
	  cur_ = iter_->CurNode();
	  
	  {
	  	
#if GLOBALOCK
		SpinLockScope spinlock(&slock);
#else
		RTMScope rtm(&tx_->rtmProf);
#endif
	  	val_ = cur_->value;

		if(prev_link != iter_->GetLink()) {
			tx_->readset->AddNext(iter_->GetLink(), iter_->GetLinkTarget());
			prev_link = iter_->GetLink();
		}
		tx_->readset->Add(&cur_->seq);
		
	  	if(val_ != NULL) {	
			return;
		
		}

	  }
	  iter_->Next();
	}

	cur_ = NULL;
	
}

void DBTX::Iterator::Prev()
{
	bool b = iter_->Prev();
	if (!b) {
		tx_->abort = true;
		cur_ = NULL;
		return;
	}

	
	while(iter_->Valid()) {
	  
	  cur_ = iter_->CurNode();
	  
	  {
	  	
#if GLOBALOCK
		SpinLockScope spinlock(&slock);
#else
		RTMScope rtm(&tx_->rtmProf);
#endif
	  	val_ = cur_->value;

		tx_->readset->AddNext(iter_->GetLink(), iter_->GetLinkTarget());
		
		tx_->readset->Add(&cur_->seq);
		
	  	if(val_ != NULL) {
			return;
		
		}

	  }
	  iter_->Prev();
	}
	
	cur_ = NULL;
}

void DBTX::Iterator::Seek(uint64_t key)
{
	//Should seek from the previous node and put it into the readset
	iter_->Seek(key);
	
	cur_ = iter_->CurNode();
	//First, find the first node which is not less than the key
	//Iterate the list to avoid concurrent insertion
	while(iter_->Valid() && iter_->Key() < key) {	
	  iter_->Next();
	  cur_ = iter_->CurNode();
	}

	//No keys is equal or larger than key
	if(!iter_->Valid()){
		assert(cur_ == NULL);
#if GLOBALOCK
		SpinLockScope spinlock(&slock);
#else
		RTMScope rtm(&tx_->rtmProf);
#endif
		//put the previous node's next field into the readset
		
		tx_->readset->AddNext(iter_->GetLink(), iter_->GetLinkTarget());
		return;
	}
	
	//Second, find the first key which value is not NULL
	while(iter_->Valid()) {

	   {
#if GLOBALOCK
		  SpinLockScope spinlock(&slock);
#else
		  RTMScope rtm(&tx_->rtmProf);
#endif
//		  printf("before\n");
	  	  //Avoid concurrently insertion
		  tx_->readset->AddNext(iter_->GetLink(), iter_->GetLinkTarget());
	//	  printf("end\n");
	  	  //Avoid concurrently modification
	  	  tx_->readset->Add(&cur_->seq);
	  
	  	  val_ = cur_->value;
	  
  	  	  if(val_ != NULL) {
	    	return;
	  	  }

	    }
	  
		iter_->Next();
	    cur_ = iter_->CurNode();
	}

	  
	cur_ = NULL;
}
	
// Position at the first entry in list.
// Final state of iterator is Valid() iff list is not empty.
void DBTX::Iterator::SeekToFirst()
{
	//Put the head into the read set first
	
	iter_->SeekToFirst();

	cur_ = iter_->CurNode();
	
	if(!iter_->Valid()) {
		assert(cur_ == NULL);
#if GLOBALOCK
		SpinLockScope spinlock(&slock);
#else
		RTMScope rtm(&tx_->rtmProf);
#endif
		//put the previous node's next field into the readset
	    
		tx_->readset->AddNext(iter_->GetLink(), iter_->GetLinkTarget());
	    
		
		return;
	}
	
	while(iter_->Valid()) {
	    
	  {
	  	
#if GLOBALOCK
		SpinLockScope spinlock(&slock);
#else
		RTMScope rtm(&tx_->rtmProf);
#endif
	  	val_ = cur_->value;


		tx_->readset->AddNext(iter_->GetLink(), iter_->GetLinkTarget());
		
		
		tx_->readset->Add(&cur_->seq);
		
	  	if(val_ != NULL) {
			return;
		
		}

	  }

	  iter_->Next();
	  cur_ = iter_->CurNode();
	}
	cur_ = NULL;

}
	
// Position at the last entry in list.
// Final state of iterator is Valid() iff list is not empty.
void DBTX::Iterator::SeekToLast()
{
	//TODO
	assert(0);
}


}  // namespace leveldb

#endif  // STORAGE_LEVELDB_DB_MEMTABLE_H_

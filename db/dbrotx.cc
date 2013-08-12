// Copyright (c) 2011 The LevelDB Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file. See the AUTHORS file for names of contributors.

#include <string>
#include "leveldb/db.h"
#include "util/rtm.h"

#include "db/dbformat.h"
#include "db/dbrotx.h"
#include "db/dbtx.h"
#include "port/port_posix.h"
#include "port/atomic.h"

#include "util/txprofile.h"
#include "util/spinlock.h"
#include "util/mutexlock.h"
#include "db/txmemstore_template.h"

namespace leveldb {

DBROTX::DBROTX(DBTables* store)
{
  txdb_ = store;
  oldsnapshot = 0;
}

DBROTX::~DBROTX()

{
  //clear all the data
}	

void DBROTX::Begin()
{
//fetch and increase the global snapshot counter
#if GLOBALOCK
  SpinLockScope slock(&DBTX::slock);
#endif

  oldsnapshot = atomic_fetch_and_add64(&txdb_->snapshot, 1);

  //printf("snapshot %ld\n", txdb_->snapshot);
}

bool DBROTX::Abort()
{
  return false;
}

bool DBROTX::End()
{
  return true;
}

bool DBROTX::ScanMemNode(Memstore::MemNode* n, uint64_t** val)
{
	
  if(n == NULL)
	return false;
  if(n->counter <= oldsnapshot) {
    if(n->value == NULL) {
      return false;
    }else {
      *val = n->value;
      return true;
    }
  }
   
  n = n->oldVersions;
  while(n != NULL && n->counter > oldsnapshot) {
   	n = n->oldVersions;
  }
   
  if(n != NULL && n->counter <= oldsnapshot) {
    if(n->value == NULL) {
       return false;
     } else {
   	   *val = n->value;
   	   return true;
     }
  }	
  return false;
}


//This function should be executed atomically
inline bool DBROTX::GetValueOnSnapshot(Memstore::MemNode* n, uint64_t** val)
{

#if GLOBALOCK
	SpinLockScope slock(&DBTX::slock);
#else
	RTMScope rtm(NULL);
#endif

	return ScanMemNode(n, val);
  
}


bool DBROTX::GetValueOnSnapshotByIndex(SecondIndex::SecondNode* sn, KeyValues* kvs)
{

#if GLOBALOCK
	SpinLockScope slock(&DBTX::slock);
#else
	RTMScope rtm(NULL);
#endif


	SecondIndex::MemNodeWrapper* mnw = sn->head;
	int i = 0;
	while(mnw != NULL) {
		uint64_t *val = NULL;
		if(ScanMemNode(mnw->memnode, &val))
		{
			kvs->keys[i] = mnw->key;
			kvs->values[i] = val;
			i++;
		}
		mnw = mnw->next;
	}

	if(i > 0) {
		kvs->num = i;
		return true;
	}

	return false;

}

bool DBROTX::Get(int tableid, uint64_t key, uint64_t** val)
{  
  Memstore::MemNode* n = txdb_->tables[tableid]->Get(key);

  return GetValueOnSnapshot(n, val);

}


DBROTX::Iterator::Iterator(DBROTX* rotx, int tableid)
{
	rotx_ = rotx;
	iter_ = rotx->txdb_->tables[tableid]->GetIterator();
	cur_ = NULL;
	val_ = NULL;
}
	
bool DBROTX::Iterator::Valid()
{
	return cur_ != NULL;
}
	

uint64_t DBROTX::Iterator::Key()
{
	return iter_->Key();
}

uint64_t* DBROTX::Iterator::Value()
{
	//return cur_->value;
	return val_;
}
	
void DBROTX::Iterator::Next()
{
	iter_->Next();
	while(iter_->Valid()) {
	  
	  if(rotx_->GetValueOnSnapshot(iter_->CurNode(), &val_)) {
		cur_ = iter_->CurNode();
		return;
	  }
	  iter_->Next();
	}
	
	cur_ = NULL;
	
}

void DBROTX::Iterator::Prev()
{
	while(iter_->Valid()) {
		iter_->Prev();
		if(rotx_->GetValueOnSnapshot(iter_->CurNode(), &val_)) {
			cur_ = iter_->CurNode();
			return;
		}
	}
	cur_ = NULL;
}

void DBROTX::Iterator::Seek(uint64_t key)
{
	iter_->Seek(key);
	while(iter_->Valid()) {		
	  if(rotx_->GetValueOnSnapshot(iter_->CurNode(), &val_)) {
	    cur_ = iter_->CurNode();
	    return;
	  }
	  iter_->Next();
	}
}
	
// Position at the first entry in list.
// Final state of iterator is Valid() iff list is not empty.
void DBROTX::Iterator::SeekToFirst()
{
	iter_->SeekToFirst();
	while(iter_->Valid()) {
		
	  if(rotx_->GetValueOnSnapshot(iter_->CurNode(), &val_)) {
        cur_ = iter_->CurNode();
		return;
	  }
	  iter_->Next();
	}

}
	
// Position at the last entry in list.
// Final state of iterator is Valid() iff list is not empty.
void DBROTX::Iterator::SeekToLast()
{
	//TODO
	assert(0);
}

DBROTX::SecondaryIndexIterator::SecondaryIndexIterator(DBROTX* rotx, int tableid)
{
	rotx_ = rotx;
	index_ = rotx_->txdb_->secondIndexes[tableid];
	iter_ = index_->GetIterator();
	cur_ = NULL;
	val_ =  NULL;
}
	
bool DBROTX::SecondaryIndexIterator::Valid()
{
	return cur_ != NULL;
}
	

uint64_t DBROTX::SecondaryIndexIterator::Key()
{
	return iter_->Key();
}

DBROTX::KeyValues* DBROTX::SecondaryIndexIterator::Value()
{
	return val_;
}
	
void DBROTX::SecondaryIndexIterator::Next()
{
	iter_->Next();
	while(iter_->Valid()) {

	  uint64_t knum = iter_->CurNode()->seq;
	
	  if(knum == 0) {
		iter_->Next();
		continue;
	  }

	  KeyValues* kvs = new KeyValues(knum);
	  
	  if(rotx_->GetValueOnSnapshotByIndex(iter_->CurNode(), kvs)) {
		cur_ = iter_->CurNode();
		val_ = kvs;
		return;
	  }
	  
	  delete kvs;
	  iter_->Next();
	}
	
	cur_ = NULL;
	
}

void DBROTX::SecondaryIndexIterator::Prev()
{
	iter_->Prev();
	while(iter_->Valid()) {

	  uint64_t knum = iter_->CurNode()->seq;
	
	  if(knum == 0) {
		iter_->Next();
		continue;
	  }

	  KeyValues* kvs = new KeyValues(knum);
	  
	  if(rotx_->GetValueOnSnapshotByIndex(iter_->CurNode(), kvs)) {
		cur_ = iter_->CurNode();
		val_ = kvs;
		return;
	  }
	  
	  delete kvs;
	  iter_->Prev();
	}
	
	cur_ = NULL;
	
}


void DBROTX::SecondaryIndexIterator::Seek(uint64_t key)
{
	iter_->Seek(key);
	while(iter_->Valid()) {

	  uint64_t knum = iter_->CurNode()->seq;
	
	  if(knum == 0) {
		iter_->Next();
		continue;
	  }

	  KeyValues* kvs = new KeyValues(knum);
	  
	  if(rotx_->GetValueOnSnapshotByIndex(iter_->CurNode(), kvs)) {
		cur_ = iter_->CurNode();
		val_ = kvs;
		return;
	  }
	  
	  delete kvs;
	  iter_->Next();
	}
}
	
// Position at the first entry in list.
// Final state of iterator is Valid() iff list is not empty.
void DBROTX::SecondaryIndexIterator::SeekToFirst()
{
	iter_->SeekToFirst();
	while(iter_->Valid()) {

	  uint64_t knum = iter_->CurNode()->seq;
	
	  if(knum == 0) {
		iter_->Next();
		continue;
	  }

	  KeyValues* kvs = new KeyValues(knum);
	  
	  if(rotx_->GetValueOnSnapshotByIndex(iter_->CurNode(), kvs)) {
		cur_ = iter_->CurNode();
		val_ = kvs;
		return;
	  }
	  
	  delete kvs;
	  iter_->Next();
	}

}
	
// Position at the last entry in list.
// Final state of iterator is Valid() iff list is not empty.
void DBROTX::SecondaryIndexIterator::SeekToLast()
{
	//TODO
	assert(0);
}


}  // namespace leveldb


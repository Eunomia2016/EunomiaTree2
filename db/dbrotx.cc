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
#include "db/txmemstore_template.h"

namespace leveldb {

DBROTX::DBROTX(MemStoreSkipList* store)
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
  DBTX::slock.Lock();
  oldsnapshot = atomic_fetch_and_add64(&txdb_->snapshot, 1);
  DBTX::slock.Unlock();
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


inline bool DBROTX::GetValueOnSnapshot(MemStoreSkipList::Node* n, uint64_t** val)
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
     n = n->next_[0];  
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

bool DBROTX::Get(uint64_t key, uint64_t** val)
{  
  MemStoreSkipList::Node* n = txdb_->GetLatestNode(key);
  
#if GLOBALOCK
  DBTX::slock.Lock();
#else
  RTMScope rtm(NULL);
#endif

  bool res =  GetValueOnSnapshot(n, val);

#if GLOBALOCK
  DBTX::slock.Unlock();
#endif

  return res;

}


DBROTX::Iterator::Iterator(DBROTX* rotx)
{
	rotx_ = rotx;
	iter_ = new MemStoreSkipList::Iterator(rotx->txdb_);
	cur_ = NULL;
}
	
bool DBROTX::Iterator::Valid()
{
	return cur_ != NULL;
}
	

uint64_t DBROTX::Iterator::Key()
{
	return cur_->key;
}

uint64_t* DBROTX::Iterator::Value()
{
	//return cur_->value;
	return value;
}
	
void DBROTX::Iterator::Next()
{
	uint64_t* val;
/*
	while(iter_->Valid()) {
		iter_->Next();
		if(rotx_->GetValueOnSnapshot(iter_->CurNode(), &val)) {
			cur_ = iter_->CurNode();
			break;
		}
	}*/
	iter_->Next(); //qh
	if (!iter_->Valid()) 
		cur_ = NULL;
	while(iter_->Valid()) {		

#if GLOBALOCK
  		DBTX::slock.Lock();
#else
 		RTMScope rtm(NULL);
#endif
		
		bool b = rotx_->GetValueOnSnapshot(iter_->CurNode(), &val);

#if GLOBALOCK
  		DBTX::slock.Unlock();
#endif
		if(b) {
			cur_ = iter_->CurNode();
			value = val; 
			break;
		}
		iter_->Next();	
	}
}

void DBROTX::Iterator::Prev()
{
	uint64_t* val;

	while(iter_->Valid()) {
		iter_->Prev();
		if(rotx_->GetValueOnSnapshot(iter_->CurNode(), &val)) {
			cur_ = iter_->CurNode();
			break;
		}
	}
}

void DBROTX::Iterator::Seek(uint64_t key)
{
	uint64_t* val;
	iter_->Seek(key);
	while(iter_->Valid()) {		
#if GLOBALOCK
		DBTX::slock.Lock();
#else
		RTMScope rtm(NULL);
#endif
				
		bool b = rotx_->GetValueOnSnapshot(iter_->CurNode(), &val);
		
#if GLOBALOCK
		DBTX::slock.Unlock();
#endif
		if(b) {
			cur_ = iter_->CurNode();
			value = val; //qh
			break;
		}
		iter_->Next();	//qh
	}
}
	
// Position at the first entry in list.
// Final state of iterator is Valid() iff list is not empty.
void DBROTX::Iterator::SeekToFirst()
{
	uint64_t* val;
	iter_->SeekToFirst();
/*	while(iter_->Valid()) {
		iter_->Next();
		if(rotx_->GetValueOnSnapshot(iter_->CurNode(), &val)) {
			cur_ = iter_->CurNode();
			break;
		}
	}*/
	while(iter_->Valid()) {		
#if GLOBALOCK
		DBTX::slock.Lock();
#else
		RTMScope rtm(NULL);
#endif
				
		bool b = rotx_->GetValueOnSnapshot(iter_->CurNode(), &val);
		
#if GLOBALOCK
		DBTX::slock.Unlock();
#endif
		if(b) {

			cur_ = iter_->CurNode();
			value = val; //qh
			break;
		}
		iter_->Next();	//qh
	}
}
	
// Position at the last entry in list.
// Final state of iterator is Valid() iff list is not empty.
void DBROTX::Iterator::SeekToLast()
{
	//TODO
	assert(0);
}



}  // namespace leveldb


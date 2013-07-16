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

bool DBROTX::Get(uint64_t key, uint64_t** val)
{  
  MemStoreSkipList::Node* n = txdb_->GetLatestNode(key);

  if(n == NULL)
  	return false;
  
  if(n->counter <= oldsnapshot) {
	if(n->value == NULL) {
		return false;
	} else {
		*val = n->value;
		return true;
	}
  }

  n = n->oldVersions;
  while(n != NULL && n->counter > oldsnapshot) {
	n = n->next_[0];
  }

  if(n->counter <= oldsnapshot) {
	if(n->value == NULL) {
		return false;
	} else {
		*val = n->value;
		return true;
	}
  }	

  return false;
}



}  // namespace leveldb


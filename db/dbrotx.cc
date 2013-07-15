// Copyright (c) 2011 The LevelDB Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file. See the AUTHORS file for names of contributors.

#include <string>
#include "leveldb/db.h"
#include "util/rtm.h"

#include "db/dbformat.h"
#include "db/dbrotx.h"
#include "port/port_posix.h"
#include "port/atomic.h"

#include "util/txprofile.h"
#include "util/spinlock.h"
#include "db/txmemstore_template.h"

namespace leveldb {

port::Mutex DBROTX::storemutex;

SpinLock DBROTX::slock;

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

bool DBROTX::Get(uint64_t key, uint64_t** val)
{  
  return txdb_->GetValueWithSnapshot(key, val, oldsnapshot);
}



}  // namespace leveldb


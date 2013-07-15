// Copyright (c) 2011 The LevelDB Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file. See the AUTHORS file for names of contributors.

#ifndef DBROTX_H
#define DBROTX_H

#include <string>
#include "leveldb/db.h"
#include "util/rtm.h"

#include "db/dbformat.h"
#include "port/port_posix.h"
#include "util/txprofile.h"
#include "util/spinlock.h"
#include "util/mutexlock.h"
#include "db/memstore_skiplist.h"



namespace leveldb {


class DBROTX {
 public:


	DBROTX (MemStoreSkipList* store);
	~DBROTX();

	void Begin();
	bool Abort();
	bool End();
	
	bool Get(uint64_t key, uint64_t** val);
	
	
private:

	static port::Mutex storemutex;
	static SpinLock slock;

	
	uint64_t oldsnapshot;
	MemStoreSkipList *txdb_ ;

};

}  // namespace leveldb

#endif  // STORAGE_LEVELDB_DB_MEMTABLE_H_

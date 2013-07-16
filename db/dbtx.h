// Copyright (c) 2011 The LevelDB Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file. See the AUTHORS file for names of contributors.

#ifndef DBTX_H
#define DBTX_H

#include <string>
#include "leveldb/db.h"
#include "util/rtm.h"

#include "db/dbformat.h"
#include "port/port_posix.h"
#include "util/txprofile.h"
#include "util/spinlock.h"
#include "util/mutexlock.h"
#include "db/memstore_skiplist.h"



#define CACHESIM 0
#define GLOBALOCK 1

namespace leveldb {


class DBTX {
 public:

	RTMProfile rtmProf;
	int count;

	DBTX (MemStoreSkipList* store);
	
	~DBTX();

	void Begin();
	bool Abort();
	bool End();
	void Add(uint64_t key, uint64_t* val);	
	bool Get(uint64_t key, uint64_t** val);
	void Delete(uint64_t key);
	void ThreadLocalInit();
	
public:

	
	
 	class ReadSet {

	  struct RSSeqPair {
	    uint64_t seq; //seq got when read the value
	    uint64_t *seqptr; //pointer to the global memory location
	  };
		
	  private:

	    int max_length;
	    int elems;

	    RSSeqPair *seqs;

	    void Resize();
			
	  public:
	    ReadSet();
	    ~ReadSet();
		inline void Reset();
	    inline void Add(uint64_t *ptr);
	    inline bool Validate();
	    void Print();
	};


	class WriteSet {
		
		struct WSKV{
			uint64_t key; //pointer to the written key 
			uint64_t *val;
			MemStoreSkipList::Node* node;
			MemStoreSkipList::Node* dummy;
		};
		
		
		
	  public:

		int cacheset[64];
		uint64_t cacheaddr[64][8];
		uint64_t cachetypes[64][8];
		int max_length;
		int elems;

		WSKV *kvs;

		void Resize();
			
	  public:
		WriteSet();
		~WriteSet();	
		void TouchAddr(uint64_t addr, int type);
		
		inline void Add(uint64_t key, uint64_t* val, MemStoreSkipList::Node* node);
		inline bool Lookup(uint64_t key, uint64_t** val);
		inline void Write(uint64_t gcounter);
		
		void Print();
		void Reset();
	};

	static __thread ReadSet* readset;
	static __thread WriteSet *writeset;
	
	static port::Mutex storemutex;
	static SpinLock slock;

	MemStoreSkipList *txdb_ ;

};

}  // namespace leveldb

#endif  // STORAGE_LEVELDB_DB_MEMTABLE_H_

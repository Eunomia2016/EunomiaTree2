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
#define GLOBALOCK 0

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

	//FIXME: This iterator doesn't provide any isolation
	class Iterator {
	 public:
	  // Initialize an iterator over the specified list.
	  // The returned iterator is not valid.
	  explicit Iterator(DBTX* tx);
	
	  // Returns true iff the iterator is positioned at a valid node.
	  bool Valid();
	
	  // Returns the key at the current position.
	  // REQUIRES: Valid()
	  uint64_t Key();

	  uint64_t* Value();
	
	  // Advances to the next position.
	  // REQUIRES: Valid()
	  void Next();
	
	  // Advances to the previous position.
	  // REQUIRES: Valid()
	  void Prev();
	
	  // Advance to the first entry with a key >= target
	  void Seek(uint64_t key);
	
	  // Position at the first entry in list.
	  // Final state of iterator is Valid() iff list is not empty.
	  void SeekToFirst();
	
	  // Position at the last entry in list.
	  // Final state of iterator is Valid() iff list is not empty.
	  void SeekToLast();
	
	 private:
	  DBTX* tx_;
	  MemStoreSkipList::Node* cur_;
	  MemStoreSkipList::Iterator *iter_;
	  uint64_t *val_;
	};
	
 	class ReadSet {

	  struct RSSeqPair {
	    uint64_t seq; //seq got when read the value
	    uint64_t *seqptr; //pointer to the global memory location
	  };

	  //This is used to check the insertion problem in range query
	  //FIXME: Still have the ABA problem
	  struct RSSuccPair {
	    uint64_t next; //next addr 
	    uint64_t *nextptr; //pointer to next[0]
	  };
		
	  private:

	    int max_length;
	    int elems;
	    RSSeqPair *seqs;

		int rangeElems;
		RSSuccPair *nexts;

	    void Resize();
			
	  public:
	    ReadSet();
	    ~ReadSet();
		inline void Reset();
	    inline void Add(uint64_t *ptr);
		inline void AddNext(uint64_t *ptr, uint64_t value);
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

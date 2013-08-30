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
#include "db/dbtables.h"


#define CACHESIM 0
#define GLOBALOCK 0
#define AGGRESSIVEDETECT 0
#define BUFFERNODE 0
#define PROFILEBUFFERNODE 0
#define CLEANUPPHASE 0

namespace leveldb {


class DBTX {
 public:

	RTMProfile rtmProf;
	int count;

	struct KeyValues {

	  int num;
	  uint64_t *keys;
	  uint64_t **values;
	  
	  KeyValues(int num)
	  {
	  	keys = new uint64_t[num];
		values = new uint64_t* [num];
	  }
	  ~KeyValues()
	  {
	  	delete[] keys;
		delete[] values;
	  }
	};

	struct BufferNode {

	  uint64_t key;
	  Memstore::MemNode* node;
	  
	  BufferNode()
	  {
	  	key = -1;
		node = NULL;
	  }
	};
	
	DBTX(DBTables* tables);


	~DBTX();

	void Begin();
	bool Abort();
	bool End();
	void Cleanup();

	void Add(int tableid, uint64_t key, uint64_t* val);
	void Add(int tableid, int indextableid, uint64_t key, uint64_t seckey, uint64_t* val);
	bool Get(int tableid, uint64_t key, uint64_t** val);
	void Delete(int tableid, uint64_t key);
	int ScanSecondNode(SecondIndex::SecondNode* sn, KeyValues* kvs);
	KeyValues* GetByIndex(int indextableid, uint64_t seckey);
	void PrintKVS(KeyValues* kvs);
	inline bool hasConflict() {return abort;}
	
	void ThreadLocalInit();
	
public:

	class Iterator {
	 public:
	  // Initialize an iterator over the specified list.
	  // The returned iterator is not valid.
	  explicit Iterator(DBTX* tx, int tableid);
	
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

	  // Just for profile
	  void SeekProfiled(uint64_t key);
	
	  // Position at the first entry in list.
	  // Final state of iterator is Valid() iff list is not empty.
	  void SeekToFirst();
	
	  // Position at the last entry in list.
	  // Final state of iterator is Valid() iff list is not empty.
	  void SeekToLast();
	
	 private:
	  DBTX* tx_;
	  Memstore *table_;
	  uint64_t tableid_;
	  Memstore::MemNode* cur_;
	  Memstore::Iterator *iter_;
	  uint64_t *val_;
	  uint64_t *prev_link;
	};


	class SecondaryIndexIterator {
	 public:
	  // Initialize an iterator over the specified list.
	  // The returned iterator is not valid.
	  explicit SecondaryIndexIterator(DBTX* tx, int tableid);
	
	  // Returns true iff the iterator is positioned at a valid node.
	  bool Valid();
	
	  // Returns the key at the current position.
	  // REQUIRES: Valid()
	  uint64_t Key();

	  KeyValues* Value();
	
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
	  SecondIndex* index_;
	  SecondIndex::SecondNode* cur_;
	  SecondIndex::Iterator *iter_;
	  KeyValues* val_;
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
	    
	    RSSeqPair *seqs;

		int rangeElems;
		RSSuccPair *nexts;

	    void Resize();
			
	  public:
	  	int elems;	
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
			int tableid;
			uint64_t key; //pointer to the written key 
			uint64_t *val;
			Memstore::MemNode* node;
			Memstore::MemNode* dummy;
		};


		struct WSSEC{
			uint64_t *seq;
			SecondIndex::MemNodeWrapper* sindex;
			Memstore::MemNode* memnode;
		};
		
		
		
	  public:

		int cacheset[64];
		uint64_t cacheaddr[64][8];
		uint64_t cachetypes[64][8];
		int max_length;
		int elems;
		int cursindex;
		
		WSKV *kvs;
		WSSEC *sindexes;

		DBTX* dbtx_;
		
		void Resize();
			
	  public:
		WriteSet();
		~WriteSet();	
		void TouchAddr(uint64_t addr, int type);
		
		inline void Add(int tableid, uint64_t key, uint64_t* val, Memstore::MemNode* node);
		inline void Add(uint64_t *seq, SecondIndex::MemNodeWrapper* mnw, Memstore::MemNode* node);
		inline bool Lookup(int tableid, uint64_t key, uint64_t** val);
		inline void UpdateSecondaryIndex();
		inline void SetDBTX(DBTX* dbtx);
		inline void Write(uint64_t gcounter);
		inline bool CheckWriteSet();
		inline void Cleanup(DBTables* tables);
		
		void Print();
		void Reset();
	};

	static __thread ReadSet* readset;
	static __thread WriteSet *writeset;
	char padding[64];
	
	static __thread bool localinit;
	static __thread BufferNode* buffer;
	char padding2[64];

	int bufferGet;
	int bufferHit;
	int bufferMiss;


       inline unsigned long rdtsc(void)
      {
        unsigned a, d;
        __asm __volatile("rdtsc":"=a"(a), "=d"(d));
        return ((unsigned long)a) | (((unsigned long) d) << 32);
      }

	uint64_t searchTime;
	uint64_t traverseTime;
	uint64_t traverseCount;	
	static port::Mutex storemutex;
	static SpinLock slock;

	bool abort;
	DBTables *txdb_;


};

}  // namespace leveldb

#endif  // STORAGE_LEVELDB_DB_MEMTABLE_H_

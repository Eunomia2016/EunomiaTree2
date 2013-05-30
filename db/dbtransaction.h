// Copyright (c) 2011 The LevelDB Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file. See the AUTHORS file for names of contributors.

#ifndef STORAGE_LEVELDB_DB_DBTRANSACTION_H_
#define STORAGE_LEVELDB_DB_DBTRANSACTION_H_

#include <string>
#include "leveldb/db.h"
#include "db/dbformat.h"
#include "db/hashtable.h"
#include "db/memtable.h"
#include "port/port_posix.h"
#include "util/txprofile.h"



namespace leveldb {

class DBTransaction {
 public:

	RTMProfile rtmProf;
	
	explicit DBTransaction(HashTable* ht, MemTable* store, port::Mutex* mutex);
	~DBTransaction();

	void Begin();
	bool End();
	void Add(ValueType type, Slice& key, Slice& value);
	bool Get(const Slice& key, std::string* value, Status* s);

	
public:

	struct Data {
		
		uint32_t length;
		char contents[1]; // Beginning of key
				
		Slice Getslice() const {
			return Slice(contents, length);
		}
	};

 	class ReadSet {

		struct RSSeqPair {
			uint64_t oldseq; //seq got when read the value
			uint64_t *seq; //pointer to the global memory location 
		};
		
		private:
			int max_length;
			int elems;

			RSSeqPair *seqs;
			uint64_t *hashes;
			Data **keys;

			void Resize();
			
		public:
			ReadSet();
			~ReadSet();			
			void Add(const Slice& key, uint64_t hash, uint64_t oldeseq, uint64_t seq_addr);
			bool Validate(HashTable* ht);
			void Print();
	};


	class WriteSet {
		
		struct WSValue {
			ValueType type; //seq got when read the value
			Data *val; //pointer to the global memory location 
		};
		
		private:
			int max_length;
			int elems;

			HashTable::Node *keys;
						
			uint64_t *seqs;
			uint64_t *hashes;
			WSValue *values;

			void Resize();
			
		public:
			WriteSet();
			~WriteSet();			
			void Add(ValueType type, const Slice& key, uint64_t hash, const Slice& val);
			void UpdateGlobalSeqs(HashTable* ht);
			bool Lookup(const Slice& key, ValueType* type, Slice* val);
			
			void Commit(MemTable *memstore);
			void Print();
	};

//	HashTable *readset;
	ReadSet* readset;
	WriteSet *writeset;
	
	port::Mutex* storemutex;
	HashTable *latestseq_ ;
	MemTable *memstore_ ;
	
  	bool Validation();
	void GlobalCommit();
};

}  // namespace leveldb

#endif  // STORAGE_LEVELDB_DB_MEMTABLE_H_

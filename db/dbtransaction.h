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
  
	struct WSNode {
		ValueType type;
		SequenceNumber seq;
		HashTable::Node* knode;
		WSNode* next;
		uint32_t refs;
		
		size_t value_length;
		char value_data[1];	// Beginning of key
		
		Slice value() const {
			// For cheaper lookups, we allow a temporary Handle object
			// to store a pointer to a key in "value".
			return Slice(value_data, value_length);
		}

		void Unref() {
		  assert(refs > 0);
		  refs--;
		  if (refs <= 0) {		
			if(knode != NULL)
				knode->Unref();
			free(this);
		  }
		}

		void Ref() {
			refs++;
		}
	};

	
public:

	
	struct RSSeqPair {
		uint64_t oldseq; //seq got when read the value
		uint64_t *seq; //pointer to the global memory location 
	};

	struct Key {
		
		uint32_t key_length;
		char key_data[1]; // Beginning of key
				
		Slice keySlice() const {
			return Slice(key_data, key_length);
		}
	};

 	class ReadSet {
		private:
			int max_length;
			int elems;

			RSSeqPair *seqs;
			uint64_t *hashes;
			Key **keys;

			void Resize();
			
		public:
			ReadSet();
			~ReadSet();			
			void Add(const Slice& key, uint64_t hash, uint64_t seq_addr);
			bool Validate(HashTable* ht);
			void Print();
	};

//	HashTable *readset;
	ReadSet* readset;
	HashTable *writeset;

	WSNode* committedValues;
	
	port::Mutex* storemutex;
	HashTable *latestseq_ ;
	MemTable *memstore_ ;
	
  	bool Validation();
	void GlobalCommit();
};

}  // namespace leveldb

#endif  // STORAGE_LEVELDB_DB_MEMTABLE_H_

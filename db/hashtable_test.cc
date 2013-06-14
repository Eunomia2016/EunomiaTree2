// Copyright (c) 2011 The LevelDB Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file. See the AUTHORS file for names of contributors.

#include "db/hashtable_template.h"
#include "db/dbtransaction_template.h"
#include "db/txmemstore_template.h"
#include <set>
#include "leveldb/env.h"
#include "leveldb/comparator.h"
#include "util/arena.h"
#include "util/hash.h"
#include "util/random.h"

namespace leveldb {

typedef uint64_t Key;


class KeyComparator : public leveldb::Comparator {
    public:
	int operator()(Key& a, Key& b) const {
		if (a < b) {
	      return -1;
	    } else if (a > b) {
	      return +1;
	    } else {
	      return 0;
	    }
	}

	virtual int Compare(const Slice& a, const Slice& b) const {
		assert(0);
		return 0;
	}

	virtual const char* Name()  const {
		assert(0);
		return 0;
	};

   virtual void FindShortestSeparator(
      std::string* start,
      const Slice& limit)  const {
		assert(0);

	}
  
   virtual void FindShortSuccessor(std::string* key)  const {
		assert(0);

	}
  
  };

  class KeyHash : public leveldb::HashFunction
  {
  public:
	
	virtual uint64_t hash(uint64_t& k)
	{
		return k;
	}

  };

}

int main(int argc, char** argv) {

  /*
  for(int i = 0; i < 10; i++) {
	k = new uint64_t();
	*k = i;
	memstore.Put(k, k, i);
  }

  for(int i = 0; i < 10; i++) {
	k = new uint64_t();
	*k = i;
	uint64_t* v;
	leveldb::Status s = memstore.Get(k, &v, i*i);
	if(s.IsNotFound())
		printf(" Not Found\n");
//	printf("Get %ld\n", *v);
  }
  
  uint64_t seq;
  memstore.GetMaxSeq(k, &seq);
  printf("Seq %d\n", seq);
  memstore.DumpTXMemStore();
  */

  leveldb::KeyHash kh;
  leveldb::KeyComparator cmp;
  leveldb::TXMemStore<leveldb::Key, leveldb::Key, leveldb::KeyComparator> memstore(cmp);

  uint64_t *k;
  leveldb::HashTable<leveldb::Key, leveldb::KeyHash, leveldb::KeyComparator> ht(kh, cmp);
  
  leveldb::DBTransaction
  	<leveldb::Key, leveldb::Key, 
  	leveldb::KeyHash, leveldb::KeyComparator> tx(&ht, &memstore, cmp);

  leveldb::ValueType t = leveldb::kTypeValue;

  tx.Begin();
  
  for(int i = 0; i < 10; i++) {
	uint64_t *k = new uint64_t();
	*k = i;
	tx.Add(t, k, k);
	uint64_t *v;
	leveldb::Status s;
	tx.Get(k, &v, &s);
	printf("Get %ld %s\n", *v, s.ToString().c_str());
  }

  tx.End();


  tx.Begin();
  
  for(int i = 0; i < 10; i++) {
	uint64_t *k = new uint64_t();
	*k = i;
	uint64_t *v;
	leveldb::Status s;
	tx.Get(k, &v, &s);
	printf("Get %ld %s\n", *v, s.ToString().c_str());
  }

  tx.End();
  

  ht.PrintHashTable();
  
  return 1;
}
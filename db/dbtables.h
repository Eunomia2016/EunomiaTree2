#ifndef DBTABLES_H
#define DBTABLES_H

#include "memstore_skiplist.h"
#include "memstore/memstore_bplustree.h"
#include "memstore/memstore_cuckoohash.h"

namespace leveldb{

class DBTables {
  public:
	
	uint64_t snapshot; // the counter for current snapshot
	int number;
	Memstore **tables;

	DBTables(int n);
	~DBTables();
	void ThreadLocalInit();
};

}
#endif


#ifndef DBTABLES_H
#define DBTABLES_H

#include "memstore_skiplist.h"
#include "memstore/memstore_bplustree.h"
#include "memstore/memstore_cuckoohash.h"

namespace leveldb{
#define NONE 0
#define BTREE 1
#define HASH 2
#define SKIPLIST 3
class DBTables {
  public:
	
	uint64_t snapshot; // the counter for current snapshot
	int number;
	Memstore **tables;
	int *types;
	int next;
	DBTables();
	DBTables(int n);
	~DBTables();
	void ThreadLocalInit();
	int AddTable(int index_type, int secondary_index_type);
};

}
#endif


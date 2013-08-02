#ifndef DBTABLES_H
#define DBTABLES_H

#include "memstore_skiplist.h"

namespace leveldb{

class DBTables {
  public:
	
	uint64_t snapshot; // the counter for current snapshot
	int number;
	MemStoreSkipList **tables;

	DBTables(int n);
	~DBTables();
	void ThreadLocalInit();
};

}
#endif


#ifndef DBTABLES_H
#define DBTABLES_H

#include "memstore_skiplist.h"
#include "memstore/memstore_bplustree.h"
#include "memstore/memstore_cuckoohash.h"
#include "memstore/secondindex.h"
#include "memstore/memstore_stringbplustree.h"
#include "memstore/memstore_uint64bplustree.h"
#include "db/epoch.h"
#include "db/gcqueue.h"
#include "db/rmqueue.h"


namespace leveldb{

class RMQueue;

#define NONE 0
#define BTREE 1
#define HASH 2
#define SKIPLIST 3
#define IBTREE 4
#define SBTREE 5

class DBTables {

  

  public:
	static __thread GCQueue* nodeGCQueue;
	static __thread RMQueue* rmqueue;
	
	uint64_t snapshot; // the counter for current snapshot
	int number;
	Memstore **tables;
	SecondIndex ** secondIndexes;
	int *types;
	int *indextypes;
	int next;
	int nextindex;
	Epoch* epoch;
	
	DBTables();
	DBTables(int n);
	~DBTables();

	void ThreadLocalInit(int tid);
	int AddTable(int tableid, int index_type, int secondary_index_type);

	//For GC
	void InitEpoch(int thr_num);
	void EpochTXBegin();
	void EpochTXEnd();
	
	void AddDeletedNodes(uint64_t **nodes, int len);
	void GCDeletedNodes();

	void AddRemoveNodes(uint64_t **nodes, int len);
	void RemoveNodes();
	
};

}
#endif


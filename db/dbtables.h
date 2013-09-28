#ifndef DBTABLES_H
#define DBTABLES_H

#include "memstore_skiplist.h"
#include "memstore/memstore_bplustree.h"
#include "memstore/memstore_cuckoohash.h"
#include "memstore/memstore_hash.h"
#include "memstore/secondindex.h"
#include "memstore/memstore_stringbplustree.h"
#include "memstore/memstore_uint64bplustree.h"
#include "db/epoch.h"
#include "db/gcqueue.h"
#include "db/rmqueue.h"
#include "db/nodebuf.h"
#include "db/objpool.h"

namespace leveldb{

class RMQueue;

#define NONE 0
#define BTREE 1
#define HASH 2
#define CUCKOO 6
#define SKIPLIST 3
#define IBTREE 4
#define SBTREE 5

class DBTables {

  

  public:
	static __thread GCQueue* nodeGCQueue;
	static __thread GCQueue* valueGCQueue;
	static __thread RMQueue* rmqueue;
	static __thread NodeBuf* nodebuffer;


	static __thread OBJPool* valuesPool;
	static __thread OBJPool* memnodesPool;
	
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

	void AddDeletedValues(uint64_t **nodes, int len);
	void GCDeletedValues();
	Memstore::MemNode* GetMemNode();
	
	void AddRemoveNodes(uint64_t **nodes, int len);
	void RemoveNodes();

	void AddDeletedValue(int tableid, uint64_t* value);
	uint64_t*GetEmptyValue(int tableid);
	
	void AddRemoveNode(uint64_t *node);
	uint64_t* GetEmptyNode();
	
};

}
#endif


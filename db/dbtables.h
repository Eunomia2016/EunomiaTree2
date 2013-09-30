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
#include "db/rcu.h"
#include "db/rmpool.h"

namespace leveldb{

class RMQueue;
class RMPool;


#define NONE 0
#define BTREE 1
#define HASH 2
#define CUCKOO 6
#define SKIPLIST 3
#define IBTREE 4
#define SBTREE 5

//GC when the number of gc objects reach GCThreshold
#define GCThreshold 100000

//GC when the number of rm objects reach RMThreshold
#define RMThreshold 100

class DBTables {

  

  public:
	static __thread GCQueue* nodeGCQueue;
	static __thread GCQueue* valueGCQueue;
	static __thread RMQueue* rmqueue;
	static __thread NodeBuf* nodebuffer;


	static __thread OBJPool* valuesPool;
	static __thread OBJPool* memnodesPool;
	static __thread uint64_t gcnum;

	static __thread RMPool* rmPool;
	
	uint64_t snapshot; // the counter for current snapshot
	int number;
	Memstore **tables;
	SecondIndex ** secondIndexes;
	int *types;
	int *indextypes;
	int next;
	int nextindex;
	Epoch* epoch;
	RCU* rcu;
	
	DBTables();
	DBTables(int n);
	~DBTables();

	void ThreadLocalInit(int tid);
	int AddTable(int tableid, int index_type, int secondary_index_type);

	//For Epoch
	void InitEpoch(int thr_num);	
	void EpochTXBegin();
	void EpochTXEnd();
	
	void AddDeletedNodes(uint64_t **nodes, int len);
	void GCDeletedNodes();
	void AddDeletedValues(uint64_t **nodes, int len);
	void GCDeletedValues();
	void AddRemoveNodes(uint64_t **nodes, int len);
	void RemoveNodes();


	//For RCU
	void RCUInit(int thr_num);
	void RCUTXBegin();
	void RCUTXEnd();
	
	
	void AddDeletedValue(int tableid, uint64_t* value);
	uint64_t*GetEmptyValue(int tableid);
	
	void AddDeletedNode(uint64_t *node);

	void AddRemoveNode(int tableid, uint64_t key, uint64_t seq, Memstore::MemNode* value);

	Memstore::MemNode* GetMemNode();	
	void GC();
	
};

}
#endif


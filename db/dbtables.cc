#include "dbtables.h"

namespace leveldb {


__thread GCQueue* DBTables::nodeGCQueue = NULL;
__thread GCQueue* DBTables::valueGCQueue = NULL;
__thread RMQueue* DBTables::rmqueue = NULL;
__thread NodeBuf* DBTables::nodebuffer = NULL;


__thread OBJPool* DBTables::valuesPool = NULL;
__thread OBJPool* DBTables::memnodesPool = NULL;
__thread uint64_t DBTables::gcnum = 0;

__thread RMPool*  DBTables::rmPool = NULL;


//FOR TEST
DBTables::DBTables() {
	number = 1;
	tables = new Memstore*[1];
//	tables[0] = new MemstoreStringBPlusTree(8);
	tables[0] = new MemstoreBPlusTree();
//	tables[0] = new MemstoreHashTable();
//	tables[0] = new MemStoreSkipList();
	types = new int[1];
	types[0] = BTREE;
	snapshot = 1;
	epoch = NULL;
}



DBTables::DBTables(int n) {
	number = n;
	next = 0;
	nextindex = 0;
	tables = new Memstore*[n];
	schemas = new TableSchema[n];
	secondIndexes = new SecondIndex*[n];
	types = new int[n];	
	indextypes = new int[n];
	snapshot = 1;
	epoch = NULL;

}

DBTables::~DBTables() {

	for (int i=0; i<next; i++) {
		if (types[i] == HASH) delete (MemstoreHashTable *)tables[i];
		else if (types[i] == BTREE) delete (MemstoreBPlusTree *)tables[i];
		else if (types[i] == SKIPLIST) delete (MemStoreSkipList *)tables[i];
		else if (types[i] == CUCKOO) delete (MemstoreCuckooHashTable *)tables[i];
#if !USESECONDINDEX
//		else if (types[i] == SBTREE) delete (MemstoreStringBPlusTree *)tables[i];
		else if (types[i] == SBTREE) delete (MemstoreUint64BPlusTree *)tables[i];
#endif			
	}
	
	delete[] tables;
	delete[] types;
	delete[] schemas;
	
#if USESECONDINDEX
	for (int i=0; i<nextindex; i++) {
		if (indextypes[i] == SBTREE) delete (SecondIndexStringBPlusTree *)secondIndexes[i];
		else if (indextypes[i] == IBTREE) delete (SecondIndexUint64BPlusTree *)secondIndexes[i];
	}
	delete secondIndexes;
	delete indextypes;
#endif	

	RMQueue::rtmProf->reportAbortStatus();

}


void DBTables::InitEpoch(int thr_num)
{
	epoch = new Epoch(thr_num);
}

void DBTables::EpochTXBegin()
{
	epoch->beginTX();
}

void DBTables::EpochTXEnd()
{
	epoch->endTX();
}

void DBTables::AddDeletedValues(uint64_t **values, int len)
{
	assert(values != NULL);
	assert(valueGCQueue != NULL);
	
	valueGCQueue->AddGCElement(epoch->getCurrentEpoch(), values, len);
}

void DBTables::GCDeletedValues()
{
	if(valueGCQueue != NULL)
		valueGCQueue->GC(epoch);
}


void DBTables::AddDeletedNodes(uint64_t **nodes, int len)
{
	assert(nodes != NULL);
	assert(nodeGCQueue != NULL);
#if 0	
	if (nodeGCQueue->elems > 1000) {
		printf("Warning : \n");
		epoch->Print();
	}
#endif	
	nodeGCQueue->AddGCElement(epoch->getCurrentEpoch(), nodes, len);
}


void DBTables::GCDeletedNodes()
{
	if(nodeGCQueue != NULL)
		nodeGCQueue->GC(epoch, nodebuffer);
}


void DBTables::AddRemoveNodes(uint64_t **nodes, int len)
{
	assert(nodes != NULL);
	assert(rmqueue != NULL);
	rmqueue->AddRMArray(epoch->getCurrentEpoch(), nodes, len);
	
}


void DBTables::RemoveNodes()
{
	if (rmqueue != NULL)
		rmqueue->Remove(epoch);
}


void DBTables::RCUInit(int thr_num)
{
	rcu = new RCU(thr_num);
}

void DBTables::RCUTXBegin()
{
	rcu->BeginTX();
}

void DBTables::RCUTXEnd()
{
	rcu->EndTX();
}

void DBTables::AddDeletedValue(int tableid, uint64_t* value)
{
	gcnum++;
	valuesPool[tableid].AddGCObj(value);
}

Memstore::MemNode* DBTables::GetMemNode()
{
	uint64_t* mn = memnodesPool->GetFreeObj();

	if(mn == NULL)
		return new Memstore::MemNode();
	
	return new (mn) Memstore::MemNode();
}


uint64_t* DBTables::GetEmptyValue(int tableid)
{
	return valuesPool[tableid].GetFreeObj();
}

void DBTables::AddDeletedNode(uint64_t *node)
{
	gcnum++;
	memnodesPool->AddGCObj(node); 
}

void DBTables::AddRemoveNode(int tableid, uint64_t key, 
										uint64_t seq, Memstore::MemNode* node)
{
	rmPool->AddRMObj(tableid, key, seq, node);
}

void DBTables::GC()
{
	if(gcnum < GCThreshold && rmPool->GCElems() < RMThreshold)
		return;

	rcu->WaitForGracePeriod();
	
	//Delete all values 
	for(int i = 0; i < number; i++) {
		valuesPool[i].GC();
	}

	memnodesPool->GC();
	gcnum = 0;

	rmPool->RemoveAll();
}

void DBTables::WriteRecord(int tableid, uint64_t key, Memstore::MemNode* node)
{
	
}

void DBTables::WriteSnapshot(int tableid, uint64_t sn)
{

}

void DBTables::ThreadLocalInit(int tid)
{

	assert(number != 0);
	
	valuesPool = new OBJPool[number];
	memnodesPool = new OBJPool();

	memnodesPool->debug = true;
	
	rmPool = new RMPool(this);
	
	gcnum = 0;
	
    if(epoch != NULL)
		epoch->setTID(tid);

	if(rcu != NULL)
		rcu->RegisterThread(tid);

	nodeGCQueue = new GCQueue();

	valueGCQueue = new GCQueue();
	
	rmqueue = new RMQueue(this);

	nodebuffer = new NodeBuf();
	
	for (int i=0; i<next; i++)
		tables[i]->ThreadLocalInit();
}


void DBTables::AddSchema(int tableid, int kl, int vl)
{
	schemas[tableid].klen = kl;
	schemas[tableid].vlen = vl;
}

int DBTables::AddTable(int tableid, int index_type,int secondary_index_type)
{
	assert(tableid == next);
	assert(next < number);
	if (index_type == BTREE) tables[next] = new MemstoreBPlusTree();	
	else if (index_type == HASH) tables[next] = new MemstoreHashTable();
	else if (index_type == SKIPLIST) tables[next] = new MemStoreSkipList();
	else if (index_type == CUCKOO) tables[next] = new MemstoreCuckooHashTable();
#if !USESECONDINDEX
//	else if (index_type == SBTREE) tables[next] = new MemstoreStringBPlusTree();
	else if (index_type == SBTREE) tables[next] = new MemstoreUint64BPlusTree();
#endif
	types[next] = index_type;
	next++;
#if USESECONDINDEX
	if (secondary_index_type == IBTREE) secondIndexes[nextindex] = new SecondIndexUint64BPlusTree();	
	else if (secondary_index_type == SBTREE) secondIndexes[nextindex] = new SecondIndexStringBPlusTree();	
	
	if (secondary_index_type != NONE) {
		indextypes[nextindex] = secondary_index_type;
		nextindex++;
	}
#endif	
	return nextindex - 1;
}

}

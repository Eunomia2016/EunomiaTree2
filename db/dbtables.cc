#include "dbtables.h"

namespace leveldb {


__thread GCQueue* DBTables::nodeGCQueue = NULL;
__thread GCQueue* DBTables::valueGCQueue = NULL;
__thread RMQueue* DBTables::rmqueue = NULL;
__thread NodeBuf* DBTables::nodebuffer = NULL;

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
	}
	delete tables;
	delete types;
	for (int i=0; i<nextindex; i++) {
		if (indextypes[i] == SBTREE) delete (MemstoreStringBPlusTree *)secondIndexes[i];
		else if (indextypes[i] == IBTREE) delete (MemstoreUint64BPlusTree *)secondIndexes[i];
	}
	delete secondIndexes;
	delete indextypes;
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


Memstore::MemNode* DBTables::GetMemNode()
{
	return nodebuffer->GetMemNode();
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


void DBTables::ThreadLocalInit(int tid)
{
	if(epoch != NULL)
		epoch->setTID(tid);

	nodeGCQueue = new GCQueue();

	valueGCQueue = new GCQueue();
	
	rmqueue = new RMQueue(this);

	nodebuffer = new NodeBuf();
	
	for (int i=0; i<next; i++)
		tables[i]->ThreadLocalInit();
}

int DBTables::AddTable(int tableid, int index_type,int secondary_index_type)
{
	assert(tableid == next);
	assert(next < number);
	if (index_type == BTREE) tables[next] = new MemstoreBPlusTree();	
	else if (index_type == HASH) tables[next] = new MemstoreHashTable();
	else if (index_type == SKIPLIST) tables[next] = new MemStoreSkipList();
	else if (index_type == CUCKOO) tables[next] = new MemstoreCuckooHashTable();
	types[next] = index_type;
	next++;
	
	if (secondary_index_type == IBTREE) secondIndexes[nextindex] = new MemstoreUint64BPlusTree();	
	else if (secondary_index_type == SBTREE) secondIndexes[nextindex] = new MemstoreStringBPlusTree();	
	if (secondary_index_type != NONE) {
		indextypes[nextindex] = secondary_index_type;
		nextindex++;
	}
	return nextindex - 1;
}

}

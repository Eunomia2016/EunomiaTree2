#include "dbtables.h"
#include "dbtx.h"

namespace leveldb {

// number of nanoseconds in 1 second (1e9)
#define ONE_SECOND_NS 1000000000

//40ms
#define UPDATEPOCH  ONE_SECOND_NS / 1000 * 40


__thread GCQueue* DBTables::nodeGCQueue = NULL;
__thread GCQueue* DBTables::valueGCQueue = NULL;
__thread RMQueue* DBTables::rmqueue = NULL;
__thread NodeBuf* DBTables::nodebuffer = NULL;


__thread OBJPool* DBTables::valuesPool = NULL;
__thread OBJPool* DBTables::memnodesPool = NULL;
__thread uint64_t DBTables::gcnum = 0;

__thread RMPool*  DBTables::rmPool = NULL;


Memstore::MemNode* DBTables::bugnode = NULL;

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

#if PERSISTENT
	pthread_t tid;
	pthread_create(&tid, NULL, SnapshotUpdateThread, (void *)this);
#endif
}

//n: tables number, thr: threads number
DBTables::DBTables(int n, int thrs)
{
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

	RCUInit(thrs);
	PBufInit(thrs);

#if PERSISTENT
	pthread_t tid;
	pthread_create(&tid, NULL, SnapshotUpdateThread, (void *)this);
#endif
	
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

//This interface is just used during initialization
void DBTables::TupleInsert(int tabid, uint64_t key, uint64_t *val, int len)
{


  	char *value = (char *)malloc(sizeof(OBJPool::Obj) + len);


  	value += sizeof(OBJPool::Obj);
  	memcpy(value, val, len);

	//printf("TupleInsert Alloc %lx\n", value);  
	
	Memstore::MemNode* mn = tables[tabid]->Put(key, (uint64_t *)value);

#if 0
	if(tabid == 1 && key == 0x13) {
		printf("TupleInsert key %lx  Alloc Value %lx MN %lx Value Ptr Addr %lx\n", 
			key, value, mn, &mn->value);
		bugnode = mn;
		DEBUGGC();
	}
#endif
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

void DBTables::AddDeletedValue(int tableid, uint64_t* value, uint64_t sn)
{	
	gcnum++;
	valuesPool[tableid].AddGCObj((char *)value, sn);
}

Memstore::MemNode* DBTables::GetMemNode(int tableid)
{
	char* mn =  memnodesPool[tableid].GetFreeObj();

	if(mn == NULL) {
		mn = (char *)malloc(sizeof(OBJPool::Obj) + sizeof(Memstore::MemNode));
		mn += sizeof(OBJPool::Obj);
	}

	
	return new (mn) Memstore::MemNode();
}


uint64_t* DBTables::GetEmptyValue(int tableid)
{
	return (uint64_t *)valuesPool[tableid].GetFreeObj();
}

void DBTables::AddDeletedNode(int tableid, uint64_t *node)
{
	gcnum++;
	
	//XXX: we set the safe sn of memnode to be 0
	memnodesPool[tableid].AddGCObj((char *)node, 0); 
}

void DBTables::AddRemoveNode(int tableid, uint64_t key, 
										uint64_t seq, Memstore::MemNode* node)
{
	rmPool->AddRMObj(tableid, key, seq, node);
}

void DBTables::GC()
{
	
	if(gcnum < GCThreshold)
		return;
	rcu->WaitForGracePeriod();
	
	for(int i = 0; i < number; i++) {
		valuesPool[i].GC(pbuf_->GetSafeSN());
		memnodesPool[i].GC(pbuf_->GetSafeSN());
	}

	
	gcnum = 0;

}

void DBTables::DelayRemove()
{
	if(rmPool->GCElems() < RMThreshold)
		return;

	rcu->WaitForGracePeriod();
	
	rmPool->RemoveAll();
}



void DBTables::PBufInit(int thrs)
{
	pbuf_ = new PBuf(thrs);
}

void DBTables::Sync()
{
	pbuf_->Sync();
}

void DBTables::DEBUGGC()
{
	if(bugnode == NULL)
		printf("Bug Node is NULL\n");
	else
		printf("Bug Node Addr %lx Value Addr %lx\n", bugnode, bugnode->value);
}

void* DBTables::SnapshotUpdateThread(void * arg)
{
	DBTables* store = (DBTables*)arg;
	
	while(true) {

		struct timespec t;
		t.tv_sec  = UPDATEPOCH / ONE_SECOND_NS;
     	t.tv_nsec = UPDATEPOCH % ONE_SECOND_NS;
      	nanosleep(&t, NULL);

		//Other snapshot updates (RO TX) are protected by RTM
		store->snapshot++;
	}
}


void DBTables::WriteUpdateRecords()
{
	int capacity = 0;
	int recnum = DBTX::writeset->elems;

	if(recnum == 0)
		return;
	
	uint64_t sn = DBTX::writeset->commitSN;

	pbuf_->RecordTX(sn, recnum);
	
	for(int i = 0; i < recnum; i++)
	{
		//FIXME: shouldn't directly use memnode
		pbuf_->WriteRecord(DBTX::writeset->kvs[i].tableid, DBTX::writeset->kvs[i].key,
			DBTX::writeset->kvs[i].commitseq, DBTX::writeset->kvs[i].commitval, 
			schemas[DBTX::writeset->kvs[i].tableid].vlen);
		
	}
}


	

void DBTables::ThreadLocalInit(int tid)
{

	assert(number != 0);
	
	valuesPool = new OBJPool[number];
	memnodesPool = new OBJPool[number];

	//memnodesPool->debug = false;
	
	rmPool = new RMPool(this);
	
	gcnum = 0;
	
    if(epoch != NULL)
		epoch->setTID(tid);

	if(rcu != NULL)
		rcu->RegisterThread(tid);

	if(pbuf_ != NULL)
		pbuf_->RegisterThread(tid);
	
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

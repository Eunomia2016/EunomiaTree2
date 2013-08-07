#include "dbtables.h"

namespace leveldb {

DBTables::DBTables(int n) {
	number = n;
	next = 0;
	tables = new Memstore*[n];
	for (int i=0; i<number; i++)
		//tables[i] = new MemstoreCuckooHashTable();
		tables[i] = new MemstoreBPlusTree();
		//tables[i] = new MemStoreSkipList();
	snapshot = 1;
}

DBTables::~DBTables() {
//	uint64_t reads = 0;
//	uint64_t writes = 0;
	for (int i=0; i<number; i++) {
//		delete (MemstoreCuckooHashTable *)tables[i];
		MemstoreBPlusTree *t = (MemstoreBPlusTree *)tables[i];
//		reads += t->reads;
//		writes += t->writes;
		delete t;
	}
	delete tables;
//	printf("reads %ld writes %ld\n", reads, writes);
}

void DBTables::ThreadLocalInit()
{
	for (int i=0; i<number; i++)
		tables[i]->ThreadLocalInit();
}

int DBTables::AddTable(int index_type,int secondary_index_type)
{
	if (index_type == BTREE) tables[next] = new MemstoreBPlusTree();
	else if (index_type == HASH) tables[next] = new MemstoreCuckooHashTable();
	else if (index_type == SKIPLIST) tables[next] = new MemStoreSkipList();
	next++;
	return next - 1;
}

}

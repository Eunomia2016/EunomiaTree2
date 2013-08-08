#include "dbtables.h"

namespace leveldb {

//FOR TEST
DBTables::DBTables() {
	number = 1;
	tables = new Memstore*[1];
	tables[0] = new MemstoreBPlusTree();
//	tables[0] = new MemstoreCuckooHashTable();
//	tables[0] = new MemStoreSkipList();
	types = new int[1];
	types[0] = BTREE;
	snapshot = 1;
}



DBTables::DBTables(int n) {
	number = n;
	next = 0;
	tables = new Memstore*[n];
	types = new int[n];	
	snapshot = 1;
}

DBTables::~DBTables() {

	for (int i=0; i<next; i++) {
		if (types[i] == HASH) delete (MemstoreCuckooHashTable *)tables[i];
		else if (types[i] == BTREE) delete (MemstoreBPlusTree *)tables[i];
		else if (types[i] == SKIPLIST) delete (MemStoreSkipList *)tables[i];
	}
	delete tables;
	delete types;
}

void DBTables::ThreadLocalInit()
{
	for (int i=0; i<next; i++)
		tables[i]->ThreadLocalInit();
}

int DBTables::AddTable(int tableid, int index_type,int secondary_index_type)
{
	assert(tableid == next);
	assert(next < number);
	if (index_type == BTREE) tables[next] = new MemstoreBPlusTree();	
	else if (index_type == HASH) tables[next] = new MemstoreCuckooHashTable();
	else if (index_type == SKIPLIST) tables[next] = new MemStoreSkipList();
	types[next] = index_type;
	next++;
	return next - 1;
}

}

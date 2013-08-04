#include "dbtables.h"

namespace leveldb {

DBTables::DBTables(int n) {
	number = n;
	tables = new Memstore*[n];
	for (int i=0; i<number; i++)
		tables[i] = new MemStoreSkipList();
	snapshot = 1;
}

DBTables::~DBTables() {
	for (int i=0; i<number; i++)
		delete tables[i];
	delete tables;
}

void DBTables::ThreadLocalInit()
{
	for (int i=0; i<number; i++)
		tables[i]->ThreadLocalInit();
}


}

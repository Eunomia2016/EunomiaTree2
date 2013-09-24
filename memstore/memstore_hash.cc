#include "memstore/memstore_hash.h"

namespace leveldb {
	__thread RTMArena* MemstoreHashTable::arena_ = NULL;
	__thread bool MemstoreHashTable::localinit_ = false;
	__thread Memstore::MemNode *MemstoreHashTable::dummyval_ = NULL;
	__thread MemstoreHashTable::HashNode *MemstoreHashTable::dummynode_ = NULL;
}

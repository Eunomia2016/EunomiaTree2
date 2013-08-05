
#include <assert.h>
#include "memstore_cuckoohash.h"


MemstoreCuckooHashTable::MemstoreCuckooHashTable()
{
	size_ = DEFAULT_SIZE;
	table_ = new Entry[size_];
}
  
  
MemstoreCuckooHashTable::~MemstoreCuckooHashTable()
{
	delete[] table_;
}

  //Only for initialization

Memstore::Iterator* MemstoreCuckooHashTable::GetIterator()
{
	assert(0);
	
}

bool MemstoreCuckooHashTable::Put(uint64_t key, MemNode* val)
{
	
	uint64_t h1, h2;

	ComputeHash(key, &h1, &h2);
	
	//Step 1. check the first slot
	int slot = GetFreeSlot(table_[h1 % size_]);
	if(slot < ASSOCIATIVITY) {
		WriteAtSlot(table_[h1 % size_], slot, key, h1, h2, val);
		return true;
	}


	//Step 2. check the second slot and evict the victim if there is none
	uint32_t undo_index[MAX_CUCKOO_COUNT];
    uint32_t undo_slot[MAX_CUCKOO_COUNT];
	
	uint32_t victim_index = 0;
	uint32_t victim_slot = 0;

	Element victim;
	uint32_t index;
	Entry *cur;
	
	for(int l = 0; l < MAX_CUCKOO_COUNT; l++) {

		index = h2 % size_;

		cur = &table_[index];
		
		slot = GetFreeSlot(*cur);

		if(slot < ASSOCIATIVITY) {
			WriteAtSlot(*cur, slot, key, h1, h2, val);
			return true;
	    }

		//generate the victim element
		victim_index = index;
		victim_slot = l % ASSOCIATIVITY;
		victim = table_[victim_index].elems[victim_slot];

		//save the victim infomation
		undo_index[l] = victim_index;
		undo_slot[l] = victim_slot;

		//replace the victim element with current element
		WriteAtSlot(*cur, victim_slot, key, h1, h2, val);

		h1 = victim.hash2;
		h2 = victim.hash1;
		key = victim.key;
		val = victim.value;
		
	}

	//Step 3. Failed to put, just roll back
	for (int n = 0; n < MAX_CUCKOO_COUNT; n++) {
		victim_index  = undo_index[MAX_CUCKOO_COUNT - 1 - n];
		victim_slot	  = undo_slot[MAX_CUCKOO_COUNT - 1 - n];
		victim = table_[victim_index].elems[victim_slot];
	
		WriteAtSlot(*cur, victim_slot, key, h1, h2, val);

		h1 = victim.hash2;
		h2 = victim.hash1;
		key = victim.key;
		val = victim.value;
	}

	return false;
	
}

Memstore::MemNode* MemstoreCuckooHashTable::Get(uint64_t key)
{
	uint64_t h1, h2;

	ComputeHash(key, &h1, &h2);
	
	
	return NULL;
}

  
Memstore::MemNode* MemstoreCuckooHashTable::GetWithInsert(uint64_t key)
{

}
  
void MemstoreCuckooHashTable::PrintStore()
{

}
  
void MemstoreCuckooHashTable::ThreadLocalInit() { assert(0); }



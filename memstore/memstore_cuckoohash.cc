#include <stdio.h>
#include <assert.h>
#include "memstore_cuckoohash.h"
#include "util/rtmScope.h"


__thread bool MemstoreCuckooHashTable::localinit_ = false;
__thread Memstore::MemNode *MemstoreCuckooHashTable::dummyval_ = NULL;
__thread uint32_t *MemstoreCuckooHashTable::undo_index = NULL;
__thread uint32_t *MemstoreCuckooHashTable::undo_slot = NULL;
  

MemstoreCuckooHashTable::MemstoreCuckooHashTable()
{
	size_ = DEFAULT_SIZE;
	table_ = new Entry[size_];
	ThreadLocalInit();
}
  
  
MemstoreCuckooHashTable::~MemstoreCuckooHashTable()
{
	delete[] table_;
}



inline void MemstoreCuckooHashTable::ThreadLocalInit()
{
	if(false == localinit_) {
		dummyval_ = new MemNode();
		dummyval_->value = NULL;
		
		undo_index = new uint32_t[MAX_CUCKOO_COUNT];
		undo_slot = new uint32_t[MAX_CUCKOO_COUNT];

		localinit_ = true;
	}
			
}



Memstore::Iterator* MemstoreCuckooHashTable::GetIterator()
{
	assert(0);
	
}

bool MemstoreCuckooHashTable::Insert(uint64_t key, MemNode **mn)
{
	
	uint64_t h1, h2;

	ComputeHash(key, &h1, &h2);

	//Step 0. Check if it already exist
	int slot = GetSlot(table_[h1 % size_], key);
	if (slot < ASSOCIATIVITY) {
		*mn = table_[h1 % size_].elems[slot].value;
		return true;
	}

	slot = GetSlot(table_[h2 % size_], key);
	if (slot < ASSOCIATIVITY) {
		*mn = table_[h2 % size_].elems[slot].value;
		return true;
	}

	MemNode *mnode = dummyval_;
	
	dummyval_ = NULL;
	*mn = mnode;
	
	//Step 1. check the first slot
	slot = GetFreeSlot(table_[h1 % size_]);
	if(slot < ASSOCIATIVITY) {
		WriteAtSlot(table_[h1 % size_], slot, key, h1, h2, mnode);
		return true;
	}


	//Step 2. check the second slot and evict the victim if there is none
	
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
			WriteAtSlot(*cur, slot, key, h1, h2, mnode);
			return true;
	    }
		
		//generate the victim infomation
		victim_index = index;
		victim_slot = l % ASSOCIATIVITY;
		victim = table_[victim_index].elems[victim_slot];

		//save the victim infomation
		if (!_xtest()) {
			undo_index[l] = victim_index;
			undo_slot[l] = victim_slot;
		}
		
		//replace the victim element with current element
		WriteAtSlot(*cur, victim_slot, key, h1, h2, mnode);

		h1 = victim.hash2;
		h2 = victim.hash1;
		key = victim.key;
		mnode = victim.value;
		
	}

	//Step 3. Failed to put, just roll back (Only need to do if it is not in the RTM protect region)
	if (_xtest()) {
		return false;
	} else {
		//Rollback manually
		for (int n = 0; n < MAX_CUCKOO_COUNT; n++) {
			victim_index  = undo_index[MAX_CUCKOO_COUNT - 1 - n];
			victim_slot	  = undo_slot[MAX_CUCKOO_COUNT - 1 - n];
			victim = table_[victim_index].elems[victim_slot];
			WriteAtSlot(table_[victim_index], victim_slot, key, h1, h2, mnode);

			h1 = victim.hash2;
			h2 = victim.hash1;
			key = victim.key;
			mnode = victim.value;
		}
		
		//Need to restore the mem node
		dummyval_ = mnode;
	}
	return false;
	
}

Memstore::MemNode* MemstoreCuckooHashTable::Get(uint64_t key)
{
	uint64_t h1, h2;

	ComputeHash(key, &h1, &h2);

	RTMArenaScope begtx(&rtmlock, &prof, NULL);
	
	int slot = GetSlot(table_[h1 % size_], key);
	if (slot < ASSOCIATIVITY)
		return table_[h1 % size_].elems[slot].value;

	slot = GetSlot(table_[h2 % size_], key);
	if (slot < ASSOCIATIVITY)
		return table_[h2 % size_].elems[slot].value;
	
	return NULL;
}

void MemstoreCuckooHashTable::Put(uint64_t k, uint64_t* val)
{

	ThreadLocalInit();
	
	bool succ = false;
	MemNode* mnode = NULL;
	
	{
		RTMArenaScope begtx(&rtmlock, &prof, NULL);
		succ = Insert(k, &mnode);
		if(succ)
			mnode->value = val;
	}

	if(dummyval_ == NULL)
		dummyval_ = new MemNode();
	
	if(!succ) {
		//TODO: need rehash the table, then retry
		printf("Alert Failed to insert %ld\n", k);
	} 
}

Memstore::MemNode* MemstoreCuckooHashTable::GetWithInsert(uint64_t key)
{

	ThreadLocalInit();
	
	bool succ = false;
	MemNode* mnode = NULL;
	
	{
		RTMArenaScope begtx(&rtmlock, &prof, NULL);
		succ = Insert(key, &mnode);
		if(succ)
			return mnode;
	}

	if(dummyval_ == NULL)
		dummyval_ = new MemNode();
	
	if(!succ) {
		//TODO: need rehash the table, then retry
		printf("Alert Failed to insert %ld\n", key);
		return NULL;
	} 
}

  
void MemstoreCuckooHashTable::PrintStore()
{
	printf("========================STORE==========================\n");
	int count = 0;
	for(int i = 0; i < size_ ; i++) {
		bool empty = true;
		//printf("Entry [%d] ", i);
		for(int j = 0; j < ASSOCIATIVITY; j++) {
			if(table_[i].elems[j].key != -1) {
//				printf(" [%d] %ld hash1 [%lx] hash2 [%lx]\t", 
	//				j, table_[i].elems[j].key, table_[i].elems[j].hash1, table_[i].elems[j].hash2);
				printf(" [%d] Key %ld H1 %d H2 %d\t", 
				j, table_[i].elems[j].key, 
				table_[i].elems[j].hash1 % size_ , table_[i].elems[j].hash2 % size_);
	
				empty = false;
				count++;
			}
        }
		if(!empty)
			printf(" Entry [%d]\n", i);
	}
	printf("Total Count %d\n", count);
}
  

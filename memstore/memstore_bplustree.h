#ifndef MEMSTOREBPLUSTREE_H
#define MEMSTOREBPLUSTREE_H

#include <stdlib.h>
#include <iostream>
#include <unordered_map>
#include <algorithm>
#include <utmpx.h>
#include "util/rtmScope.h"
#include "util/rtm.h"
#include "util/rtm_arena.h"
#include "util/mutexlock.h"
#include "util/numa_util.h"
#include "util/statistics.h"
#include "port/port_posix.h"
#include "memstore.h"
#define M  15
#define N  15

#define BTREE_PROF 0
#define BTREE_LOCK 0
#define BTPREFETCH 0
#define DUMMY 1

#define NODEMAP  0
#define NODEDUMP 0
#define KEYDUMP  0
#define KEYMAP   0
#define NUMADUMP 0

#define REMOTEACCESS 0

#define BUFFER_TEST 0

#define BUFFER_LEN 15

#define CONFLICT_BUFFER_LEN 100

//static uint64_t writes = 0;
//static uint64_t reads = 0;

/*static int total_key = 0;
static int total_nodes = 0;
static uint64_t rconflict = 0;
static uint64_t wconflict = 0;
*/
using namespace std;

/*void atomic_inc32(uint32_t *p) {
	__asm__ __volatile__("lock; incl %0"
						 : "+m"(*p)
						 :
						 : "cc");
}*/

namespace leveldb {

struct access_log {
	uint64_t gets;
	uint64_t writes;
	uint64_t splits;
};

struct key_log {
	uint64_t gets;
	uint64_t writes;
	uint64_t dels;
};

static uint32_t leaf_id = 0;
static int32_t inner_id = 0;
static uint32_t table_id = 0;

class MemstoreBPlusTree: public Memstore {
//Test purpose
public:

#if BUFFER_TEST
	struct buffer_entry {
		int valid: 1;
		uint64_t key;
		MemNode* val;
	};
	class NUMA_Buffer {
	private:
		int head;
		int hits;
		int reads;
		int writes;
		int invalids;
		buffer_entry entries[BUFFER_LEN];
		SpinLock* bf_lock;
	public:
		NUMA_Buffer(SpinLock* _bf_lock): head(0), hits(0), reads(0), writes(0), invalids(0), bf_lock(_bf_lock) {
			for(int i = 0; i < BUFFER_LEN; i++) {
				entries[i] = {0, 0, NULL};
			}
		}

		MemNode* get(uint64_t key) {
			//reads ++;
			MemNode* res = NULL;
			bf_lock->Lock();
			for(int i = 0; i < BUFFER_LEN; i++) {
				buffer_entry entry = entries[i];
				if(entry.key == key && entry.valid) {
					//hits++;
					res =  entry.val;
					break;
				}
			}
			bf_lock->Unlock();
			return res;
		}

		void push(uint64_t key, MemNode* val) {
			//writes ++;
			int index = __sync_fetch_and_add(&head, 1) % BUFFER_LEN;
			entries[index] = {1, key, val};
		}

		void inv(uint64_t key) {
			int index = -1;
			bf_lock->Lock();
			for(int i = 0; i < BUFFER_LEN; i++) {
				buffer_entry entry = entries[i];
				if(entry.key == key && entry.valid) {
					index = i;
					break;
				}
			}
			bf_lock->Unlock();
			if(index != -1) {
				//invalids++;
				entries[index].valid = 0;
			}
		}

		~NUMA_Buffer() {
			//printf("%d, %d, %d, %d\n", reads, writes, hits, invalids);
		}
	};
	NUMA_Buffer** buffers;
#endif
	
/*
	unordered_map<uint32_t, access_log> node_map;
	unordered_map<uint64_t, key_log> key_map;

	access_log level_logs[10];
*/

#if REMOTEACCESS
	uint64_t inner_local_access;
	uint64_t inner_remote_access;
	uint64_t leaf_local_access;
	uint64_t leaf_remote_access;
	uint64_t buffer_local_access;
#endif

	int tableid;
	int num_of_nodes;
	int num_insert_rtm;

	uint64_t rconflict = 0;
	uint64_t wconflict = 0;

	struct LeafNode {
		LeafNode() : num_keys(0) {
			signature = __sync_fetch_and_add(&leaf_id, 1);
		} //, writes(0), reads(0) {}
//		uint64_t padding[4];
		unsigned signature;
		unsigned num_keys;
		uint64_t keys[M];
		MemNode *values[M];
		LeafNode *left;
		LeafNode *right;
		uint64_t seq;
//		uint64_t writes;
//		uint64_t reads;
//		uint64_t padding1[4];
	};

	struct InnerNode {
		InnerNode() : num_keys(0) {
			signature = __sync_fetch_and_add(&inner_id, -1);
		}//, writes(0), reads(0) {}
//		uint64_t padding[8];
//		unsigned padding;
		int signature;
		unsigned num_keys;
		uint64_t 	 keys[N];
		void*	 children[N + 1];
//		uint64_t writes;
//		uint64_t reads;
//		uint64_t padding1[8];
	};

	//The result object of the delete function
	struct DeleteResult {
		DeleteResult(): value(0), freeNode(false), upKey(-1) {}
		Memstore::MemNode* value;  //The value of the record deleted
		bool freeNode;	//if the children node need to be free
		uint64_t upKey; //the key need to be updated -1: default value
	};

	class Iterator: public Memstore::Iterator {
	public:
		// Initialize an iterator over the specified list.
		// The returned iterator is not valid.
		Iterator() {};
		Iterator(MemstoreBPlusTree* tree);

		// Returns true iff the iterator is positioned at a valid node.
		bool Valid();

		// Returns the key at the current position.
		// REQUIRES: Valid()
		MemNode* CurNode();

		uint64_t Key();

		// Advances to the next position.
		// REQUIRES: Valid()
		bool Next();

		// Advances to the previous position.
		// REQUIRES: Valid()
		bool Prev();

		// Advance to the first entry with a key >= target
		void Seek(uint64_t key);

		void SeekPrev(uint64_t key);

		// Position at the first entry in list.
		// Final state of iterator is Valid() iff list is not empty.
		void SeekToFirst();

		// Position at the last entry in list.
		// Final state of iterator is Valid() iff list is not empty.
		void SeekToLast();

		uint64_t* GetLink();

		uint64_t GetLinkTarget();

	private:
		MemstoreBPlusTree* tree_;
		LeafNode* node_;
		uint64_t seq_;
		int leaf_index;
		uint64_t *link_;
		uint64_t target_;
		uint64_t key_;
		MemNode* value_;
		uint64_t snapshot_;

		// Intentionally copyable
	};

public:
	MemstoreBPlusTree() {
		printf("MemstoreBPlusTree()\n");
		//leaf_id = 0;
		//tableid = __sync_fetch_and_add(&table_id,1);
		//int num_of_nodes = numa_num_configured_nodes();
		//buffers = new NUMA_Buffer[num_of_nodes]();

		root = new LeafNode();
		reinterpret_cast<LeafNode*>(root)->left = NULL;
		reinterpret_cast<LeafNode*>(root)->right = NULL;
		reinterpret_cast<LeafNode*>(root)->seq = 0;
		depth = 0;

#if BTREE_PROF
		writes = 0;
		reads = 0;
		calls = 0;
#endif
//		printf("root addr %lx\n", &root);
//		printf("depth addr %lx\n", &depth);
		/*		for (int i=0; i<4; i++) {
					windex[i] = 0;
					rindex[i] = 0;
				}*/
	}
	MemstoreBPlusTree(int _tablid) {
		tableid = _tablid;
		root = new LeafNode();
		reinterpret_cast<LeafNode*>(root)->left = NULL;
		reinterpret_cast<LeafNode*>(root)->right = NULL;
		reinterpret_cast<LeafNode*>(root)->seq = 0;
		depth = 0;
/*
		for(int i = 0; i < 10; i++) {
			level_logs[i].gets = 0;
			level_logs[i].writes = 0;
			level_logs[i].splits = 0;
		}
*/
		num_of_nodes = numa_num_configured_nodes();
#if BUFFER_TEST
		buffers = (NUMA_Buffer**)calloc(num_of_nodes, sizeof(NUMA_Buffer*));
		SpinLock *bf_lock = (SpinLock*)calloc(1, sizeof(SpinLock));
		for(int i = 0; i < num_of_nodes; i++){
			NUMA_Buffer* local_buffer = 
				(NUMA_Buffer*)Numa_alloc_onnode(sizeof(NUMA_Buffer),i);
			buffers[i] = new (local_buffer) NUMA_Buffer(bf_lock);
		}
#endif

#if REMOTEACCESS
		inner_local_access = inner_remote_access
							 = leaf_local_access = leaf_remote_access = buffer_local_access = 0;
#endif

		num_insert_rtm = 0;

#if BTREE_PROF
		writes = 0;
		reads = 0;
		calls = 0;
#endif
		/*
			for (int i=0; i<4; i++) {
				windex[i] = 0;
				rindex[i] = 0;
			}
			current_tid = sched_getcpu();
			*/

	}

	~MemstoreBPlusTree() {
		//printf("[Alex]~MemstoreBPlusTree tableid = %d\n", tableid);
		//printf("[Alex]~MemstoreBPlusTree\n");
		//prof.reportAbortStatus();
		//delprof.reportAbortStatus();
		//PrintList();
		//PrintStore();
		//printf("rwconflict %ld\n", rconflict);
		//printf("wwconflict %ld\n", wconflict);
		//printf("depth %d\n",depth);
		//printf("reads %ld\n",reads);
		//printf("writes %ld\n", writes);
		//printf("calls %ld touch %ld avg %f\n", calls, reads + writes,  (float)(reads + writes)/(float)calls );
#if NODEMAP

		printf("=========Tableid = %d=========\n", tableid);
		printf("Insert_rtm = %d\n", num_insert_rtm);
		for(int i = 0; i < 6; i++) {
			printf("%d: %d, %d, %d\n", i, level_logs[i].gets, level_logs[i].writes, level_logs[i].splits);
		}

#endif
#if BUFFER_TEST
		for(int i = 0; i < num_of_nodes; i++){
			Numa_free(buffers[i], sizeof(NUMA_Buffer));
		}
		free(buffers);
#endif
#if REMOTEACCESS
		printf("tableid = %2d, inner_local_access = %10d, inner_remote_access = %10d, leaf_local_access = %10d, leaf_remote_access = %10d, buffer_local_access = %10d\n",
			   tableid, inner_local_access, inner_remote_access, leaf_local_access, leaf_remote_access, buffer_local_access);
		table_prof.inner_local_access += inner_local_access;
		table_prof.inner_remote_access += inner_remote_access;
		table_prof.leaf_local_access += leaf_local_access;
		table_prof.leaf_remote_access += leaf_remote_access;
		table_prof.buffer_local_access += buffer_local_access;
#endif

#if BTREE_PROF
		printf("calls %ld avg %f writes %f\n", calls, (float)(reads + writes) / (float)calls, (float)(writes) / (float)calls);
#endif

#if NODEDUMP
		for(auto iter : node_map) {
			//if(iter.second.gets + iter.second.writes + iter.second.splits > 10)
			printf("[%ld][%ld]: {%ld, %ld, %ld}\n", tableid, iter.first, iter.second.gets, iter.second.writes, iter.second.splits);
		}
		printf("Total Nodes: %ld\n", node_map.size());
#endif

#if KEYDUMP
		for(auto iter : key_map) {
			if(iter.second.gets + iter.second.writes + iter.second.dels > 10)
				printf("[%ld]: {%ld, %ld, %ld}\n", iter.first, iter.second.gets, iter.second.writes, iter.second.dels);
		}
		printf("Total Keys: %ld\n", key_map.size());
#endif
		//printTree();
		//top();
	}
	void transfer_para(RTMPara& para) {
		prof.transfer_para(para);
	}
	inline void ThreadLocalInit() {
		if(false == localinit_) {
			arena_ = new RTMArena();
			dummyval_ = GetMemNode();
			dummyval_->value = NULL;
			dummyleaf_ = new LeafNode();
			localinit_ = true;
		}
	}

	inline LeafNode* new_leaf_node() {
#if DUMMY
		LeafNode* result = dummyleaf_;
		dummyleaf_ = NULL;
#else
		LeafNode* result = new LeafNode();
#endif
		//LeafNode* result = (LeafNode *)(arena_->AllocateAligned(sizeof(LeafNode)));
		return result;
	}

	inline InnerNode* new_inner_node() {
		InnerNode* result = new InnerNode();
		//InnerNode* result = (InnerNode *)(arena_->AllocateAligned(sizeof(InnerNode)));
		return result;
	}

	inline LeafNode* FindLeaf(uint64_t key) {
		InnerNode* inner;
		register void* node = root;
		register unsigned d = depth;
		unsigned index = 0;
		while(d-- != 0) {
			index = 0;
			inner = reinterpret_cast<InnerNode*>(node);
			while((index < inner->num_keys) && (key >= inner->keys[index])) {
				++index;
			}
			node = inner->children[index];
		}
		return reinterpret_cast<LeafNode*>(node);
	}

	inline MemNode* Get(uint64_t key) {
		//RTMArenaScope begtx(&rtmlock, &prof, arena_);
		struct timespec begin, end;
		clock_gettime(CLOCK_MONOTONIC, &begin);
#if BTREE_LOCK
		MutexSpinLock lock(&slock);
#else
		//RTMArenaScope begtx(&rtmlock, &delprof, arena_);
		RTMScope begtx(&prof, depth * 2, 1, &rtmlock, GET_TYPE);
#endif
		InnerNode* inner;
		register void* node = root;
		register unsigned d = depth;
		unsigned index = 0;
		while(d-- != 0) {
			index = 0;
			inner = reinterpret_cast<InnerNode*>(node);
#if REMOTEACCESS
			if(get_current_node() == get_numa_node(inner)) {
				inner_local_access++;
			} else {
				inner_remote_access++;
			}
#endif

#if NODEMAP
			//printf("[%2d][GET] node = %10d, key = %20ld, d = %2d\n",
			//	sched_getcpu(), inner->signature, key, d+1);
			level_logs[d + 1].gets++;
#endif

//			reads++;
			while((index < inner->num_keys) && (key >= inner->keys[index])) {
				++index;
			}
			//get down to the corresponding child
			node = inner->children[index];
		}

		//it is a defacto leaf node, reinterpret_cast
		LeafNode* leaf = reinterpret_cast<LeafNode*>(node);

#if REMOTEACCESS
		if(get_current_node() == get_numa_node(inner)) {
			leaf_local_access++;
		} else {
			leaf_remote_access++;
		}
#endif

#if NODEMAP
		//printf("[%2d][GET] node = %10d, key = %20ld, d = %2d\n",
		//			sched_getcpu(), leaf->signature, key, 0);
		level_logs[0].gets++;
#endif

		if(leaf->num_keys == 0) return NULL;
		unsigned k = 0;
		while((k < leaf->num_keys)&&(leaf->keys[k]<key)) {
			++k;
		}

		if(k==leaf->num_keys){return NULL;}
		if(leaf->keys[k]==key){
			return leaf->values[k];
		}else{
			return NULL;
		}
	}

	inline MemNode* Put(uint64_t k, uint64_t* val) {
		ThreadLocalInit();
		MemNode *node = GetWithInsert(k).node;
		node->value = val;
#if BTREE_PROF
		reads = 0;
		writes = 0;
		calls = 0;
#endif
		return node;
	}

	inline int slotAtLeaf(uint64_t key, LeafNode* cur) {
		int slot = 0;
		while((slot < cur->num_keys) && cur->keys[slot] != key) {
			slot++;
		}
		return slot;
	}

	inline Memstore::MemNode* removeLeafEntry(LeafNode* cur, int slot) {
		assert(slot < cur->num_keys);
		cur->seq = cur->seq + 1;
		Memstore::MemNode* value = cur->values[slot];
		cur->num_keys--; //num_keys subtracts one
		//The key deleted is the last one
		if(slot == cur->num_keys)
			return value;
		//Re-arrange the entries in the leaf
		for(int i = slot + 1; i <= cur->num_keys; i++) {
			cur->keys[i - 1] = cur->keys[i];
			cur->values[i - 1] = cur->values[i];
		}
		return value;
	}

	inline DeleteResult* LeafDelete(uint64_t key, LeafNode* cur) {
		struct timespec begin, end;
		clock_gettime(CLOCK_MONOTONIC, &begin);

		//step 1. find the slot of the key
		int slot = slotAtLeaf(key, cur);
		//the record of the key doesn't exist, just return
		if(slot == cur->num_keys) {
			return NULL;
		}
		//	assert(cur->values[slot]->value == (uint64_t *)2);
		//	printf("delete node\n");
		DeleteResult *res = new DeleteResult();

		//step 2. remove the entry of the key, and get the deleted value
		res->value = removeLeafEntry(cur, slot);

#if NODEMAP
		//	printf("[%2d][DEL] node = %10d, key = %20ld, d = %2d\n",
		//		   sched_getcpu(), cur->signature, key, 0);
		level_logs[0].writes++;
#endif

		//step 3. if node is empty, remove the node from the list
		if(cur->num_keys == 0) {
			if(cur->left != NULL)
				cur->left->right = cur->right;
			if(cur->right != NULL)
				cur->right->left = cur->left;

			//Parent is responsible for the node deletion
			res->freeNode = true;
			return res;
		}
		//The smallest key in the leaf node has been changed, update the parent key
		if(slot == 0) {
			res->upKey = cur->keys[0];
		}
		return res;
	}

	inline int slotAtInner(uint64_t key, InnerNode* cur) {
		int slot = 0;
		while((slot < cur->num_keys) && (cur->keys[slot] <= key)) {
			slot++;
		}
		return slot;
	}

	inline void removeInnerEntry(InnerNode* cur, int slot, DeleteResult* res) {
		assert(slot <= cur->num_keys);
		//If there is only one available entry
		if(cur->num_keys == 0) {
			assert(slot == 0);
			res->freeNode = true;
			return;
		}
		//The key deleted is the last one
		if(slot == cur->num_keys) {
			cur->num_keys--;
			return;
		}
		//rearrange the children slot
		for(int i = slot + 1; i <= cur->num_keys; i++)
			cur->children[i - 1] = cur->children[i];
		//delete the first entry, upkey is needed
		if(slot == 0) {
			//record the first key as the upkey
			res->upKey = cur->keys[slot];
			//delete the first key
			for(int i = slot; i < cur->num_keys - 1; i++) {
				cur->keys[i] = cur->keys[i + 1];
			}
		} else {
			//delete the previous key
			for(int i = slot; i < cur->num_keys; i++) {
				cur->keys[i - 1] = cur->keys[i];
			}
		}
		cur->num_keys--;
	}

	inline DeleteResult* InnerDelete(uint64_t key, InnerNode* cur , int depth) {
		DeleteResult* res = NULL;
		//step 1. find the slot of the key
		int slot = slotAtInner(key, cur);

		//step 2. remove the record recursively
		//This is the last level of the inner nodes
		if(depth == 1) {
			res = LeafDelete(key, (LeafNode *)cur->children[slot]);
		} else {
			//printf("Delete Inner Node  %d\n", depth);
			//printInner((InnerNode *)cur->children[slot], depth - 1);
			res = InnerDelete(key, (InnerNode *)cur->children[slot], (depth - 1));
		}
		//The record is not found
		if(res == NULL) {
			return res;
		}
		//step 3. Remove the entry if the TOTAL children nodes have been removed
		if(res->freeNode) {
			//FIXME: Should free the children node here
			//remove the node from the parent node
			res->freeNode = false;
			removeInnerEntry(cur, slot, res);
			return res;
		}
		//step 4. update the key if needed
		if(res->upKey != -1) {
			if(slot != 0) {
				cur->keys[slot - 1] = res->upKey; //the upkey should be updated
				res->upKey = -1;
			}
		}
		return res;
	}

	inline Memstore::MemNode* Delete_rtm(uint64_t key) {
#if BTREE_LOCK
		MutexSpinLock lock(&slock);
#else
		//RTMArenaScope begtx(&rtmlock, &delprof, arena_);
		RTMScope begtx(&prof, depth * 2, 1, &rtmlock, DEL_TYPE);
#endif
		DeleteResult* res = NULL;
		if(depth == 0) {
			//Just delete the record from the root
			res = LeafDelete(key, (LeafNode*)root);
		} else {
			res = InnerDelete(key, (InnerNode*)root, depth);
		}
		//printf("[%ld] DEL: %lx\n", pthread_self(), root);

		if(res == NULL)
			return NULL;

		if(res->freeNode)
			root = NULL;

		return res->value;
	}

	inline Memstore::MemNode* GetWithDelete(uint64_t key) {
		ThreadLocalInit();
		//timespec begin, end;
		//clock_gettime(CLOCK_MONOTONIC, &begin);
		//printf("[%ld] BeginTime = %ld\n", pthread_self(), begin.tv_sec * BILLION + begin.tv_nsec);
		MemNode* value = Delete_rtm(key);
		//clock_gettime(CLOCK_MONOTONIC, &end);
		//printf("[%ld] EndTime = %ld\n", pthread_self(), end.tv_sec * BILLION + end.tv_nsec);
#if DUMMY
		if(dummyval_ == NULL) {
			dummyval_ = GetMemNode();
		}
		if(dummyleaf_ == NULL) {
			dummyleaf_ = new LeafNode();
		}
#endif
		return value;
	}

	inline Memstore::InsertResult GetWithInsert(uint64_t key) {
#if BUFFER_TEST
		int current_node = get_current_node();
		for(int i = 0; i < num_of_nodes; i++) {
			buffers[i]->inv(key);
		}
#endif
		//printf("[BEGIN] key = %ld, type = %d\n", key, type);
#if 0
		auto key_iter = key_map.find(key);
		if(key_iter != key_map.end()) {
			key_iter->second++;
		} else {
			key_map.insert(make_pair(key, 0));
		}
#endif
		//printf("[END] key = %ld, type = %d\n", key, type);

		ThreadLocalInit();
		//timespec begin, end;
		//clock_gettime(CLOCK_MONOTONIC, &begin);
		//printf("[%ld] BeginTime = %ld\n", pthread_self(), begin.tv_sec * BILLION + begin.tv_nsec);
		InsertResult res = Insert_rtm(key);

		//MemNode* value = res.node;
		//bool newNode = res.newNode;
		//clock_gettime(CLOCK_MONOTONIC, &end);
		//printf("[%ld] EndTime = %ld\n", pthread_self(), end.tv_sec * BILLION + end.tv_nsec);
#if DUMMY
		if(dummyval_ == NULL) {
			dummyval_ = GetMemNode();
		}
		if(dummyleaf_ == NULL) {
			dummyleaf_ = new LeafNode();
		}
#endif
#if BUFFER_TEST
		buffers[current_node]->push(key, res.node);
#endif
		return res;
	}

#if BUFFER_TEST
	inline MemNode* checkBuffer(uint64_t key) {
		int current_node = get_current_node();
		MemNode* res_node = buffers[current_node]->get(key);
		if(res_node != NULL) {
			return res_node;
		}
		for(int i = 0 ; i < num_of_nodes; i++) {
			if(i == current_node) {
				continue;
			}
			res_node = buffers[i]->get(key);
			if(res_node != NULL) {
				return res_node;
			}
		}
		return NULL;
	}
#endif

	inline Memstore::MemNode* GetForRead(uint64_t key) {
		//printf("[BEGIN] key = %ld, type = %d\n", key, type);
#if BUFFER_TEST
		MemNode* node = checkBuffer(key);

		if(node != NULL) {
			//buffer_local_access++;
			return node;
		}
#endif
#if 0
		auto key_iter = key_map.find(key);
		if(key_iter != key_map.end()) {
			key_iter->second++;
		} else {
			key_map.insert(make_pair(key, 0));
		}
#endif
		//printf("[END] key = %ld, type = %d\n", key, type);
		ThreadLocalInit();
		//timespec begin, end;
		//clock_gettime(CLOCK_MONOTONIC, &begin);
		//printf("[%ld] BeginTime = %ld\n", pthread_self(), begin.tv_sec * BILLION + begin.tv_nsec);
		MemNode* value = Get(key);
		//clock_gettime(CLOCK_MONOTONIC, &end);
		//printf("[%ld] EndTime = %ld\n", pthread_self(), end.tv_sec * BILLION + end.tv_nsec);
#if DUMMY
		if(dummyval_ == NULL) {
			dummyval_ = GetMemNode();
		}
		if(dummyleaf_ == NULL) {
			dummyleaf_ = new LeafNode();
		}
#endif

#if BUFFER_TEST
		int current_node = get_current_node();
		buffers[current_node]->push(key, value);
#endif
		return value;
	}

	inline Memstore::InsertResult Insert_rtm(uint64_t key) {
#if NODEMAP
		num_insert_rtm ++;
		if(tableid == 8 && num_insert_rtm % 10000 == 0) {
			printf("Insert_rtm = %d\n", num_insert_rtm);
			for(int i = 0; i < 6; i++) {
				printf("%d: %d, %d, %d\n", i, level_logs[i].gets, level_logs[i].writes, level_logs[i].splits);
			}
		}
#endif
		bool newKey = true;
#if BTREE_LOCK
		MutexSpinLock lock(&slock);
#else
		//RTMArenaScope begtx(&rtmlock, &prof, arena_);
		RTMScope begtx(&prof, depth * 2, 1, &rtmlock, ADD_TYPE);
#endif

#if BTREE_PROF
		calls++;
#endif
		if(root == NULL) {
			root = new_leaf_node();
			reinterpret_cast<LeafNode*>(root)->left = NULL;
			reinterpret_cast<LeafNode*>(root)->right = NULL;
			reinterpret_cast<LeafNode*>(root)->seq = 0;
			depth = 0;
		}

		MemNode* val = NULL;
		if(depth == 0) {
			LeafNode *new_leaf = LeafInsert(key, reinterpret_cast<LeafNode*>(root), &val, &newKey);
			if(new_leaf != NULL) { //a new leaf node is created, therefore adding a new inner node to hold
				InnerNode *inner = new_inner_node();
				inner->num_keys = 1;
				inner->keys[0] = new_leaf->keys[0];
				inner->children[0] = root;
				inner->children[1] = new_leaf;
				depth++;
				root = inner;
//				checkConflict(inner->signature, 1);
//				checkConflict(reinterpret_cast<LeafNode*>(root)->signature, 1);
#if BTREE_PROF
				writes++;
#endif
//				inner->writes++;
			}
		} else {
#if BTPREFETCH
			for(int i = 0; i <= 64; i += 64)
				prefetch(reinterpret_cast<char*>(root) + i);
#endif
			InnerInsert(key, reinterpret_cast<InnerNode*>(root), depth, &val, &newKey);
		}
		//printf("[%ld] ADD: %lx\n", sched_getcpu(), root);
		return {val, newKey};
	}

	inline InnerNode* InnerInsert(uint64_t key, InnerNode *inner, int d, MemNode** val, bool* newKey) {
#if REMOTEACCESS
		if(get_current_node() == get_numa_node(inner)) {
			inner_local_access++;
		} else {
			inner_remote_access++;
		}
#endif
		unsigned k = 0;
		uint64_t upKey;
		InnerNode *new_sibling = NULL;
		//find the appropriate position of the new key
		while((k < inner->num_keys) && (key >= inner->keys[k])) {
			k++;
		}

#if NODEMAP
		//printf("[%2d][GET] node = %10d, key = %20ld, d = %2d\n", sched_getcpu(), inner->signature, key, d);
		level_logs[d].gets++;
#endif

		void *child = inner->children[k]; //search the descendent layer
#if BTPREFETCH
		for(int i = 0; i <= 64; i += 64) {
			prefetch(reinterpret_cast<char*>(child) + i);
		}
#endif
		//inserting at the lowest inner level
		if(d == 1) {
			LeafNode *new_leaf = LeafInsert(key, reinterpret_cast<LeafNode*>(child), val, newKey);
			if(new_leaf != NULL) {  //if a new leaf node is created
				InnerNode *toInsert = inner;
				//the inner node is full -> split it
				if(inner->num_keys == N) {
#if NODEMAP
					//printf("[%2d][SPT] node = %10d, key = %20ld, d = %2d\n",
					//				sched_getcpu(), inner->signature, key, d);
					level_logs[d].splits++;
#endif
					new_sibling = new_inner_node();
					if(new_leaf->num_keys == 1) {
						new_sibling->num_keys = 0;
						upKey = new_leaf->keys[0];
						toInsert = new_sibling;
						k = -1;
					} else {
						unsigned threshold = (N + 1) / 2;
						//num_keys(new inner node) = num_keys(old inner node) - threshold
						new_sibling->num_keys = inner->num_keys - threshold;
						//moving the excessive keys to the new inner node
						for(unsigned i = 0; i < new_sibling->num_keys; ++i) {
							new_sibling->keys[i] = inner->keys[threshold + i];
							new_sibling->children[i] = inner->children[threshold + i];
						}
#if NODEMAP
						//printf("[%2d][ADD] node = %10d, key = %20ld, d = %2d\n",
						//				sched_getcpu(), new_sibling->signature, key, d);
						level_logs[d].writes++;
#endif

						//the last child
						new_sibling->children[new_sibling->num_keys] = inner->children[inner->num_keys];
						//the num_key of the original node should be below the threshold
						inner->num_keys = threshold - 1;
						//upkey should be the delimiter of the old/new node in their common parent
						upKey = inner->keys[threshold - 1];
						//the new leaf node could be the child of old/new inner node
						if(new_leaf->keys[0] >= upKey) {
							toInsert = new_sibling;
							//if the new inner node is to be inserted, the index to insert should subtract threshold
							if(k >= threshold) k = k - threshold;
							else k = 0;
						}
					}
//					inner->keys[N-1] = upKey;
					new_sibling->keys[N - 1] = upKey; //???
#if 0
					printf("==============\n");
					for(int i = 0; i < inner->num_keys; i++) {
						printf("key[%2d] = %ld\n", i, inner->keys[i]);
					}
					printf("--------------\n");
					for(int i = 0; i < new_sibling->num_keys; i++) {
						printf("key[%2d] = %ld\n", i, new_sibling->keys[i]);
					}
					printf("==============\n");
#endif
//					if(d == 0){
//						checkConflict(new_sibling->signature, 1);
//					}
#if BTREE_PROF
					writes++;
#endif
//					new_sibling->writes++;
				}

				//insert the new key at the (k)th slot of the parent node (old or new)
				if(k != -1) {
					for(int i = toInsert->num_keys; i > k; i--) {
						toInsert->keys[i] = toInsert->keys[i - 1];
						toInsert->children[i + 1] = toInsert->children[i];
					}
					toInsert->num_keys++; //add a new key
					toInsert->keys[k] = new_leaf->keys[0];
				}

				toInsert->children[k + 1] = new_leaf;

#if NODEMAP
				//printf("[%2d][ADD] node = %10d, key = %20ld, d = %2d\n",
				//			sched_getcpu(), toInsert->signature, key, d);
				level_logs[d].writes++;

#endif
//				if(d == 0){
//					checkConflict(toInsert->signature, 1);
//				}
#if BTREE_PROF
				writes++;
#endif
//				inner->writes++;
			} else { // no new leaf node is created
#if BTREE_PROF
				reads++;
#endif
//				inner->reads++;
//				if(d == 0){
//					checkConflict(inner->signature, 0);
//				}
			}
//			if (new_sibling!=NULL && new_sibling->num_keys == 0) printf("sibling\n");
		} else { //not inserting at the lowest inner level
			//recursively insert at the lower levels
			InnerNode *new_inner =
				InnerInsert(key, reinterpret_cast<InnerNode*>(child), d - 1, val, newKey);

			if(new_inner != NULL) {
				InnerNode *toInsert = inner;
				InnerNode *child_sibling = new_inner;

				unsigned treshold = (N + 1) / 2; //split equally
				//the current node is full, creating a new node to hold the inserted key
				if(inner->num_keys == N) {
					new_sibling = new_inner_node();

#if NODEMAP
					//printf("[%2d][SPT] node = %10d, key = %20ld, d = %2d\n",
					//		sched_getcpu(), inner->signature, key, d);
					level_logs[d].splits++;
#endif

					if(child_sibling->num_keys == 0) {
						new_sibling->num_keys = 0;
						upKey = child_sibling->keys[N - 1];
						toInsert = new_sibling;
						k = -1;
					} else {
						//new_sibling should hold the excessive (>=threshold) keys
						new_sibling->num_keys = inner->num_keys - treshold;

						for(unsigned i = 0; i < new_sibling->num_keys; ++i) {
							new_sibling->keys[i] = inner->keys[treshold + i];
							new_sibling->children[i] = inner->children[treshold + i];
						}

#if NODEMAP
						//printf("[%2d][ADD] node = %10d, key = %20ld, d = %2d\n",
						//	sched_getcpu(), new_sibling->signature, key, d);
						level_logs[d].writes++;
#endif
//						if(d == 0){
//							checkConflict(new_sibling->signature, 1);
//						}
						new_sibling->children[new_sibling->num_keys] =
							inner->children[inner->num_keys];

						//XXX: should threshold ???
						inner->num_keys = treshold - 1;

						upKey = inner->keys[treshold - 1];
						//after split, the new key could be inserted into the old node or the new node
						if(key >= upKey) {
							toInsert = new_sibling;
							if(k >= treshold) k = k - treshold;
							else k = 0;
						}
					}
					//XXX: what is this used for???
					//inner->keys[N-1] = upKey;
					new_sibling->keys[N - 1] = upKey;

#if BTREE_PROF
					writes++;
#endif
//					new_sibling->writes++;
				}
				//inserting the new key to appropriate position
				if(k != -1) {
					for(int i = toInsert->num_keys; i > k; i--) {
						toInsert->keys[i] = toInsert->keys[i - 1];
						toInsert->children[i + 1] = toInsert->children[i];
					}
					toInsert->num_keys++;
					toInsert->keys[k] = reinterpret_cast<InnerNode*>(child_sibling)->keys[N - 1]; //??
				}
				toInsert->children[k + 1] = child_sibling;

#if NODEMAP
				//printf("[%2d][ADD] node = %10d, key = %20ld, d = %2d\n",
				//			sched_getcpu(), toInsert->signature, key, d);
				level_logs[d].writes++;

#endif

#if BTREE_PROF
				writes++;
#endif
//				inner->writes++;
//				if(d == 0){
//					checkConflict(toInsert->signature, 1);
//				}
			} else {
#if BTREE_PROF
				reads++;
#endif
//				inner->reads++;
//				if(d == 0){
//					checkConflict(inner->signature, 0);
//				}
			}
		}

		if(d == depth && new_sibling != NULL) {
			//printf("tableid = %d, depth = %d, num_insert_rtm = %d\n", tableid, depth, num_insert_rtm);
			InnerNode *new_root = new_inner_node();
			new_root->num_keys = 1;
			new_root->keys[0] = upKey;
			new_root->children[0] = root;
			new_root->children[1] = new_sibling;

#if NODEMAP
			//printf("[%2d][ADD] node = %10d, key = %20ld, d = %2d\n",
			//	sched_getcpu(), new_root->signature, key, d);
			level_logs[d].writes++;
			//if(tableid == 8)
			//	printf("new depth = %d, num_insert_rtm = %d\n", depth+1, num_insert_rtm);
#endif

			root = new_root;
			depth++;

#if BTREE_PROF
			writes++;
#endif
//			new_root->writes++;
//			if(d == 0){
//				checkConflict(new_root->signature, 1);
//				checkConflict(reinterpret_cast<InnerNode*>(root)->signature, 1);
//			}
		} else if(d == depth) {
//			if(d == 0){
//				checkConflict(reinterpret_cast<InnerNode*>(root)->signature, 0);
//			}
		}
//		if (inner->num_keys == 0) printf("inner\n");
		//if (new_sibling->num_keys == 0) printf("sibling\n");
		return new_sibling; //return the newly-created node (if exists)
	}

//Insert a key at the leaf level
//Return: the new node where the new key resides, NULL if no new node is created
//@val: storing the pointer to new value in val
	inline LeafNode* LeafInsert(uint64_t key, LeafNode *leaf, MemNode** val, bool * newKey) {
		//printf("[%ld] ADD: %lx\n", pthread_self(), (LeafNode*)root);
#if REMOTEACCESS
		if(get_current_node() == get_numa_node(leaf)) {
			leaf_local_access++;
		} else {
			leaf_remote_access++;
		}
#endif
		LeafNode *new_sibling = NULL;
		unsigned k = 0;
		while((k < leaf->num_keys)&&(leaf->keys[k]<key)) {
			++k;
		}

		if((k < leaf->num_keys) && (leaf->keys[k] == key)) {
			*newKey = false;
		#if BTPREFETCH
			prefetch(reinterpret_cast<char*>(leaf->values[k]));
		#endif
			*val = leaf->values[k];
		#if NODEMAP
			//printf("[%2d][GET] node = %10d, key = %20ld, d = %2d\n",
			//		   sched_getcpu(), leaf->signature, key,0);
			level_logs[0].gets++;

		#endif
		//			checkConflict(leaf->signature, 0);

		#if BTREE_PROF
			reads++;
		#endif
			assert(*val != NULL);
			return NULL;
		}
		*newKey = true;
		//inserting a new key in the children
		LeafNode *toInsert = leaf;
		//create a new node to accommodate the new key if the leaf is full
		if(leaf->num_keys == M) {
			new_sibling = new_leaf_node();
#if NUMADUMP
			printf("Node = %ld NUMA ZONE = %d\n", new_sibling->signature, get_numa_node(new_sibling));
#endif
//			checkConflict(new_sibling->signature, 1);

			if(leaf->right == NULL && k == leaf->num_keys) {
				new_sibling->num_keys = 0;
				toInsert = new_sibling;
				k = 0;
			} else {
				//SPLITTING the current node
				unsigned threshold = (M + 1) / 2;
				new_sibling->num_keys = leaf->num_keys - threshold;

				//moving the keys above the threshold to the new sibling
				for(unsigned j = 0; j < new_sibling->num_keys; ++j) {
					//move the keys beyond threshold in old leaf to the new leaf
					new_sibling->keys[j] = leaf->keys[threshold + j];
					new_sibling->values[j] = leaf->values[threshold + j];
				}
				leaf->num_keys = threshold;

				if(k >= threshold) {
					k = k - threshold;
					toInsert = new_sibling;
				}
			}
			//inserting the newsibling at the right of the old leaf node
			if(leaf->right != NULL) leaf->right->left = new_sibling;
			new_sibling->right = leaf->right;
			new_sibling->left = leaf;
			leaf->right = new_sibling;
			new_sibling->seq = 0;

#if NODEMAP
			//printf("[%2d][ADD] node = %10d, key = %20ld, d = %2d\n",
			//	   sched_getcpu(), new_sibling->signature, key, 0);
			level_logs[0].writes++;
#endif

#if BTREE_PROF
			writes++;
#endif
//			new_sibling->writes++;

#if NODEMAP
			//printf("[%2d][SPT] node = %10d, key = %20ld, d = %2d\n",
			//	   sched_getcpu(), leaf->signature, key, 0);
			level_logs[0].splits++;
#endif

		}


		
		#if BTPREFETCH
				prefetch(reinterpret_cast<char*>(dummyval_));
		#endif

				for(int j = toInsert->num_keys; j > k; j--) {
					toInsert->keys[j] = toInsert->keys[j - 1];
					toInsert->values[j] = toInsert->values[j - 1];
				}

				toInsert->num_keys = toInsert->num_keys + 1;
				toInsert->keys[k] = key;
		

#if NODEMAP
		level_logs[0].writes++;
#endif

#if DUMMY
		toInsert->values[k] = dummyval_;
		*val = dummyval_;
#else
		toInsert->values[k] = GetMemNode();
		*val = toInsert->values[k];
#endif

		assert(*val != NULL);
		dummyval_ = NULL;

#if BTREE_PROF
		writes++;
#endif
//		leaf->writes++;

//		checkConflict(leaf->signature, 1);

		//printf("IN LEAF2");
		//printTree();
		leaf->seq = leaf->seq + 1;
		return new_sibling;
	}

	Memstore::Iterator* GetIterator() {
		return new MemstoreBPlusTree::Iterator(this);
	}
	void printLeaf(LeafNode *n);
	void printInner(InnerNode *n, unsigned depth);
	void PrintStore();
	void PrintList();
	void checkConflict(int sig, int mode) ;

//YCSB TREE COMPARE Test Purpose
	void TPut(uint64_t key, uint64_t *value) {
#if BTREE_LOCK
		MutexSpinLock lock(&slock);
#else
		//RTMArenaScope begtx(&rtmlock, &prof, arena_);
		RTMScope begtx(&prof, depth * 2, 1, &rtmlock);
#endif

		if(root == NULL) {
			root = new_leaf_node();
			reinterpret_cast<LeafNode*>(root)->left = NULL;
			reinterpret_cast<LeafNode*>(root)->right = NULL;
			reinterpret_cast<LeafNode*>(root)->seq = 0;
			depth = 0;
		}

		if(depth == 0) {
			LeafNode *new_leaf = TLeafInsert(key, reinterpret_cast<LeafNode*>(root), value);
			if(new_leaf != NULL) {
				InnerNode *inner = new_inner_node();
				inner->num_keys = 1;
				inner->keys[0] = new_leaf->keys[0];
				inner->children[0] = root;
				inner->children[1] = new_leaf;
				depth++;
				root = inner;
//								checkConflict(inner->signature, 1);
//								checkConflict(reinterpret_cast<LeafNode*>(root)->signature, 1);
#if BTREE_PROF
				writes++;
#endif
				//				inner->writes++;
			}
		} else {

#if BTPREFETCH
			for(int i = 0; i <= 64; i += 64)
				prefetch(reinterpret_cast<char*>(root) + i);
#endif

			TInnerInsert(key, reinterpret_cast<InnerNode*>(root), depth, value);
		}
	}

	inline LeafNode* TLeafInsert(uint64_t key, LeafNode *leaf, uint64_t *value) {
		LeafNode *new_sibling = NULL;
		unsigned k = 0;
		while((k < leaf->num_keys) && (leaf->keys[k] < key)) {
			++k;
		}

		if((k < leaf->num_keys) && (leaf->keys[k] == key)) {
			leaf->values[k] = (Memstore::MemNode *)value;
			return NULL;
		}

		LeafNode *toInsert = leaf;
		if(leaf->num_keys == M) {
			new_sibling = new_leaf_node();
			if(leaf->right == NULL && k == leaf->num_keys) {
				new_sibling->num_keys = 0;
				toInsert = new_sibling;
				k = 0;
			} else {
				unsigned threshold = (M + 1) / 2;
				new_sibling->num_keys = leaf->num_keys - threshold;
				for(unsigned j = 0; j < new_sibling->num_keys; ++j) {
					new_sibling->keys[j] = leaf->keys[threshold + j];
					new_sibling->values[j] = leaf->values[threshold + j];
				}
				leaf->num_keys = threshold;

				if(k >= threshold) {
					k = k - threshold;
					toInsert = new_sibling;
				}
			}
			if(leaf->right != NULL) leaf->right->left = new_sibling;
			new_sibling->right = leaf->right;
			new_sibling->left = leaf;
			leaf->right = new_sibling;
			new_sibling->seq = 0;
#if BTREE_PROF
			writes++;
#endif
			//			new_sibling->writes++;
//						checkConflict(new_sibling->signature, 1);
		}

		//printf("IN LEAF1 %d\n",toInsert->num_keys);
		//printTree();

		for(int j = toInsert->num_keys; j > k; j--) {
			toInsert->keys[j] = toInsert->keys[j - 1];
			toInsert->values[j] = toInsert->values[j - 1];
		}

		toInsert->num_keys = toInsert->num_keys + 1;
		toInsert->keys[k] = key;
		toInsert->values[k] = (Memstore::MemNode *)value;

		return new_sibling;
	}

	inline InnerNode* TInnerInsert(uint64_t key, InnerNode *inner, int d, uint64_t* value) {
		unsigned k = 0;
		uint64_t upKey;
		InnerNode *new_sibling = NULL;

		while((k < inner->num_keys) && (key >= inner->keys[k])) {
			++k;
		}

		void *child = inner->children[k];

#if BTPREFETCH
		for(int i = 0; i <= 64; i += 64)
			prefetch(reinterpret_cast<char*>(child) + i);
#endif

		if(d == 1) {
			LeafNode *new_leaf = TLeafInsert(key, reinterpret_cast<LeafNode*>(child), value);
			if(new_leaf != NULL) {
				InnerNode *toInsert = inner;
				if(inner->num_keys == N) {
					new_sibling = new_inner_node();
					if(new_leaf->num_keys == 1) {
						new_sibling->num_keys = 0;
						upKey = new_leaf->keys[0];
						toInsert = new_sibling;
						k = -1;
					} else {
						unsigned treshold = (N + 1) / 2;
						new_sibling->num_keys = inner->num_keys - treshold;

						for(unsigned i = 0; i < new_sibling->num_keys; ++i) {
							new_sibling->keys[i] = inner->keys[treshold + i];
							new_sibling->children[i] = inner->children[treshold + i];
						}

						new_sibling->children[new_sibling->num_keys] = inner->children[inner->num_keys];
						inner->num_keys = treshold - 1;

						upKey = inner->keys[treshold - 1];

						if(new_leaf->keys[0] >= upKey) {
							toInsert = new_sibling;
							if(k >= treshold) k = k - treshold;
							else k = 0;
						}
					}
//					inner->keys[N-1] = upKey;
					new_sibling->keys[N - 1] = upKey;
				}

				if(k != -1) {
					for(int i = toInsert->num_keys; i > k; i--) {
						toInsert->keys[i] = toInsert->keys[i - 1];
						toInsert->children[i + 1] = toInsert->children[i];
					}
					toInsert->num_keys++;
					toInsert->keys[k] = new_leaf->keys[0];
				}
				toInsert->children[k + 1] = new_leaf;
			}

//			if (new_sibling!=NULL && new_sibling->num_keys == 0) printf("sibling\n");
		} else {
			bool s = true;
			InnerNode *new_inner =
				TInnerInsert(key, reinterpret_cast<InnerNode*>(child), d - 1, value);

			if(new_inner != NULL) {
				InnerNode *toInsert = inner;
				InnerNode *child_sibling = new_inner;


				unsigned treshold = (N + 1) / 2;
				if(inner->num_keys == N) {
					new_sibling = new_inner_node();

					if(child_sibling->num_keys == 0) {
						new_sibling->num_keys = 0;
						upKey = child_sibling->keys[N - 1];
						toInsert = new_sibling;
						k = -1;
					}

					else  {
						new_sibling->num_keys = inner->num_keys - treshold;

						for(unsigned i = 0; i < new_sibling->num_keys; ++i) {
							new_sibling->keys[i] = inner->keys[treshold + i];
							new_sibling->children[i] = inner->children[treshold + i];
						}
						new_sibling->children[new_sibling->num_keys] =
							inner->children[inner->num_keys];


						//XXX: should threshold ???
						inner->num_keys = treshold - 1;

						upKey = inner->keys[treshold - 1];
						//printf("UP %lx\n",upKey);
						if(key >= upKey) {
							toInsert = new_sibling;
							if(k >= treshold) k = k - treshold;
							else k = 0;
						}
					}
					//XXX: what is this used for???
					//inner->keys[N-1] = upKey;
					new_sibling->keys[N - 1] = upKey;


				}
				if(k != -1) {
					for(int i = toInsert->num_keys; i > k; i--) {
						toInsert->keys[i] = toInsert->keys[i - 1];
						toInsert->children[i + 1] = toInsert->children[i];
					}

					toInsert->num_keys++;
					toInsert->keys[k] = reinterpret_cast<InnerNode*>(child_sibling)->keys[N - 1];
				}
				toInsert->children[k + 1] = child_sibling;
			}
		}

		if(d == depth && new_sibling != NULL) {
			InnerNode *new_root = new_inner_node();
			new_root->num_keys = 1;
			new_root->keys[0] = upKey;
			new_root->children[0] = root;
			new_root->children[1] = new_sibling;
			root = new_root;
			depth++;
		}
//		else if (d == depth) checkConflict(reinterpret_cast<InnerNode*>(root)->signature, 0);
//		if (inner->num_keys == 0) printf("inner\n");
		//if (new_sibling->num_keys == 0) printf("sibling\n");
		return new_sibling;
	}

public:
	static __thread RTMArena* arena_;	  // Arena used for allocations of nodes
	static __thread bool localinit_;
	static __thread MemNode *dummyval_;
	static __thread LeafNode *dummyleaf_;

	char padding1[64];
	void *root;
	int depth;

	char padding2[64];
	RTMProfile delprof;
	char padding3[64];

	RTMProfile prof;
	char padding6[64];
	port::SpinLock slock;
#if BTREE_PROF
public:
	uint64_t reads;
	uint64_t writes;
	uint64_t calls;
#endif
	char padding4[64];
	SpinLock rtmlock;
	char padding5[64];

	int current_tid;
	int waccess[4][CONFLICT_BUFFER_LEN];
	int raccess[4][CONFLICT_BUFFER_LEN];
	int windex[4];
	int rindex[4];
};
//__thread RTMArena* MemstoreBPlusTree::arena_ = NULL;
//__thread bool MemstoreBPlusTree::localinit_ = false;
}
#endif

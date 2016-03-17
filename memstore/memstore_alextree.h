#ifndef MEMSTOREALEXTREE_H
#define MEMSTOREALEXTREE_H

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
#include "util/bloomfilter.h"
#include "port/port_posix.h"
#include "memstore.h"
#include "silo_benchmark/util.h"
//#define LEAF_NUM 15
#define N  15

//#define SEG_NUM 2
//#define SEG_LEN 7
//#define LEAF_NUM (SEG_NUM*SEG_LEN)

#define LEAF_NUM 16

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
#define LEVEL_LOG 0

#define BUFFER_TEST 0
#define BUFFER_LEN (1<<8)
#define HASH_MASK (BUFFER_LEN-1)
#define OFFSET_BITS 4

#define BM_TEST 0
#define FLUSH_FREQUENCY 100
#define ERROR_RATE 0.1
#define BM_SIZE 100

#define SHUFFLE_KEYS 0

#define ORIGIN_INSERT 0
#define SHUFFLE_INSERT 1
#define UNSORTED_INSERT 0

#define SEGS 4
#define SEG_LEN 4
#define EMP_LEN  4
#define HAL_LEN  2

#define ADAPTIVE_LOCK 1
#define SPEC_PROF 0

//static uint64_t writes = 0;
//static uint64_t reads = 0;

/*static int total_key = 0;
static int total_nodes = 0;
static uint64_t rconflict = 0;
static uint64_t wconflict = 0;
*/

using namespace std;
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

//const int SEG_LENS[] = {4,4,4,3};
#define CACHE_LINE_SIZE 64

class MemstoreAlexTree: public Memstore {
//Test purpose
public:

	int tableid;

	uint64_t rconflict = 0;
	uint64_t wconflict = 0;

	uint64_t spec_hit;
	uint64_t spec_miss;

//	uint64_t inserts;
	uint64_t shifts;

	uint64_t half_born;
	uint64_t empty_born;

	uint64_t should_protect;
	uint64_t should_not_protect;

	uint64_t rightmost;
	uint64_t not_rightmost;

	bool first_leaf;

	uint64_t spec_time;
	uint64_t insert_time;

	uint64_t insert_times[SEGS+1];
	uint64_t inserts[SEGS+1];

	uint64_t split_ops;
	uint64_t duplicate_keys;

	struct LeafNode;
	unordered_map<LeafNode*, vector<uint64_t> > leaf_dup_log;
#if SHUFFLE_INSERT
	struct Leaf_Seg{
		uint64_t keys[EMP_LEN];
		//uint64_t paddings[4];
		unsigned max_room;
		//MemNode* vals[EMP_LEN];
		unsigned key_num;
		uint64_t paddings1[3]; //cacheline alignment
	};

	struct Insert_Log{
		Insert_Log():one_try(0),two_try(0),check_all(0),split(0){}
		uint64_t one_try;
		uint64_t two_try;
		uint64_t check_all;
		uint64_t split;
	};
	
	Insert_Log insert_log;
#endif

	struct LeafNode {
		LeafNode() : num_keys(0){
		//	signature = __sync_fetch_and_add(&leaf_id, 1);
		} //, writes(0), reads(0) {}
		//unsigned signature;
#if SHUFFLE_INSERT
		Leaf_Seg leaf_segs[SEGS];
		//int born_key_num;
		uint64_t paddings[8];//cacheline alignment
		SpinLock mlock;
#endif

		uint64_t keys[LEAF_NUM];
		MemNode *values[LEAF_NUM];
		
		unsigned num_keys;
		LeafNode *left;
		LeafNode *right;
		uint64_t seq;
	};

	struct InnerNode {
		InnerNode() : num_keys(0) {
			//signature = __sync_fetch_and_add(&inner_id, -1);
		}//, writes(0), reads(0) {}
		//SpinLock mlock;
		//int signature;
		unsigned num_keys;
		uint64_t 	 keys[N];
		void*	 children[N+1];
		//uint64_t padding1[7];
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
		Iterator(MemstoreAlexTree* tree);

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
		MemstoreAlexTree* tree_;
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
	MemstoreAlexTree() {
		//leaf_id = 0;
		//tableid = __sync_fetch_and_add(&table_id,1);
		assert(0);
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
	MemstoreAlexTree(int _tableid) {
		//printf("sizeof(Leaf_Seg) = %u\n", sizeof(Leaf_Seg));
		tableid = _tableid;
		root = new LeafNode();
		first_leaf = true;
		reinterpret_cast<LeafNode*>(root)->left = NULL;
		reinterpret_cast<LeafNode*>(root)->right = NULL;
		reinterpret_cast<LeafNode*>(root)->seq = 0;
		depth = 0;
		insert_time=spec_time=0;
		spec_hit=spec_miss=0;
		split_ops=0;
		duplicate_keys=0;
/*
		for(int i = 0; i < 20 ; i ++){
			last_leafs[i] = new LeafNode();
		}
*/		
		//half_born=empty_born=0;
		//should_protect=should_not_protect=0;
		//rightmost=not_rightmost=0;
#if BTREE_PROF
		writes = 0;
		reads = 0;
		calls = 0;
#endif

	}

	inline unsigned Shuffle(){
		return random() % SEGS;
	}
	
	~MemstoreAlexTree() {
		//printf("[Alex]~MemstoreAlexTree tableid = %d\n", tableid);
		//printf("[Alex]~MemstoreAlexTree\n");
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
		prof.reportAbortStatus();
		printf("duplicate_keys = %lu\n", duplicate_keys);
		//printf("split_ops = %lu\n", split_ops);
		/*
		printf("one_try = %lu\n", insert_log.one_try);
		printf("two_try = %lu\n", insert_log.two_try);
		printf("check_all = %lu\n", insert_log.check_all);
		printf("split = %lu\n", insert_log.split);
		*/
		/*
		for(int i = 0; i < SEGS+1; i++){
			printf("insert_times[%d] = %lu, inserts[%d] = %lu, single_insert_times[%d] = %lf\n", 
				i,insert_times[i],i,inserts[i],i,(double)insert_times[i]/inserts[i]);
		}
		*/
#if SPEC_PROF
		printf("spec_hit = %lu, spec_miss = %lu\n", spec_hit, spec_miss);
#endif
		//printf("spec_time = %lu, insert_time = %lu\n", spec_time, insert_time);

		printf("should_protect = %d, should_not_protect = %d\n", should_protect, should_not_protect);
		printf("rightmost = %d, not_rightmost = %d\n", rightmost, not_rightmost);
#if BTREE_PROF
		printf("calls %ld avg %f writes %f\n", calls, (float)(reads + writes) / (float)calls, (float)(writes) / (float)calls);
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

	inline InnerNode* SpecInner(uint64_t key){
		InnerNode* inner;
		register void* node = root;
		register unsigned d = depth;
		unsigned index = 0;
		if(d == 0){
			return reinterpret_cast<InnerNode*>(node);
		}
		while(d-- != 1) {
			index = 0;
			inner = reinterpret_cast<InnerNode*>(node);
			while((index < inner->num_keys) && (key >= inner->keys[index])) {
				++index;
			}
			node = inner->children[index];
		}
		return reinterpret_cast<InnerNode*>(node);
	}
	
	inline MemNode* Get(uint64_t key) {
		//RTMArenaScope begtx(&rtmlock, &prof, arena_);

		InnerNode* inner;
		register void* node ;
		register unsigned d ;
		unsigned index ;
		
#if BTREE_LOCK
		MutexSpinLock lock(&slock);
#else
		//RTMArenaScope begtx(&rtmlock, &delprof, arena_);
		RTMScope begtx(&prof, depth * 2, 1, &rtmlock, GET_TYPE);
#endif

		node = root;
		d = depth;
		index = 0;

		while(d-- != 0) {
			index = 0;
			inner = reinterpret_cast<InnerNode*>(node);
//			reads++;
			while((index < inner->num_keys) && (key >= inner->keys[index])) {
				++index;
			}
			//get down to the corresponding child
			node = inner->children[index];
		}

		//it is a defacto leaf node, reinterpret_cast
		LeafNode* leaf = reinterpret_cast<LeafNode*>(node);

		if(leaf->num_keys == 0) return NULL;
		unsigned k = 0;
		while((k < leaf->num_keys) && (leaf->keys[k] < key)) {
			++k;
		}

		if(k == leaf->num_keys) {
			return NULL;
		}
		if(leaf->keys[k] == key) {
			return leaf->values[k];
		} else {
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

#if LEVEL_LOG
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
		MemNode* value = Delete_rtm(key);
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

#if SHUFFLE_INSERT&&ADAPTIVE_LOCK
	inline bool ShouldLockLeaf(LeafNode* leaf){
/*
		int idx = -1;
		int seg_len = EMP_LEN;
		if(leaf->born_key_num==8){
			seg_len = HAL_LEN;
		}
*/		

		int full_segs = 0;
		for(int i = 0; i < SEGS; i++){
			if(leaf->leaf_segs[i].key_num >= leaf->leaf_segs[i].max_room){
				full_segs++;
			}
			if(full_segs == 2) break;
		}
		bool need_protect = full_segs == 2;
		//if(need_protect)should_protect++;
		//else should_not_protect++;
		return need_protect;
	}
#endif
/*
	inline int full_seg_num(LeafNode* leaf){
		int seg_len = EMP_LEN;
		if(leaf->born_key_num == 8){
			seg_len = HAL_LEN;
		}
		int full_segs = 0;
		for(int i = 0 ; i < SEGS; i ++){
			if(leaf->leaf_segs[i].key_num >= seg_len){
				full_segs++;
			}
		}
		//printf("full_segs = %d\n", full_segs);
		return full_segs;
	}
*/
	inline Memstore::InsertResult GetWithInsert(uint64_t key) {
		//int thread_id = sched_getcpu();
		ThreadLocalInit();
		LeafNode* spec_leaf = NULL;
		LeafNode* target_leaf = NULL;
		InnerNode* inner = NULL;
		InnerNode* target_inner = NULL;

		{
			RTMScope begtx(&prof, depth * 2, 1, &rtmlock, GET_TYPE);
			spec_leaf = FindLeaf(key);
		}

		bool locked = false;
		bool should_move = false;
		
		
#if ADAPTIVE_LOCK
		//int full_segs = ShouldLockLeaf(spec_leaf);
		// should_move = full_segs == 2;
		if(ShouldLockLeaf(spec_leaf))
		{
			locked = true;
#endif
			spec_leaf->mlock.Lock();
#if ADAPTIVE_LOCK
		}
#endif
		//t1.lap();

		MemNode* res = Insert_rtm(key, &target_leaf);

		/*
		insert_time=t1.lap();
		if(target_leaf){
			int full_segs = full_seg_num(target_leaf);
			//printf("full_segs = %d\n", full_segs);
			insert_times[full_segs]+=insert_time;
			inserts[full_segs]+=1;
		}
		*/
		//if(target_inner==inner){spec_hit++;}else{spec_miss++;}
		//insert_time+=t1.lap();

#if ADAPTIVE_LOCK
		if(locked)
#endif
		spec_leaf->mlock.Unlock();

		/*
		if(target_leaf == last_leafs[thread_id]){
			spec_hit++;
		}else{
			spec_miss++;
			last_leafs[thread_id]=target_leaf;
		}
		*/
		/*
		if(target_leaf!=last_leafs[thread_id]){
			last_leafs[thread_id]=target_leaf;
		}
		*/
#if SPEC_PROF
		if(spec_leaf == target_leaf){
			spec_hit++;
		}else{
			spec_miss++;
		}
#endif

#if DUMMY
		if(dummyval_ == NULL) {
			dummyval_ = GetMemNode();
		}
		if(dummyleaf_ == NULL) {
			dummyleaf_ = new LeafNode();
		}
#endif

		res = dummyval_;
		return {res, false};
	}

	inline Memstore::MemNode* GetForRead(uint64_t key) {
		ThreadLocalInit();
		MemNode* value = Get(key);
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

	inline Memstore::MemNode* Insert_rtm(uint64_t key, LeafNode** target_leaf) {
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
			//reinterpret_cast<LeafNode*>(root)->born_key_num = 0;
			//printf("root->born_key_num = %d\n", reinterpret_cast<LeafNode*>(root)->born_key_num );
			depth = 0;
		}

		MemNode* val = NULL;
		if(depth == 0) {
			LeafNode *new_leaf = LeafInsert(key, reinterpret_cast<LeafNode*>(root), &val, target_leaf);
			//printf("root->born_key_num = %d\n", reinterpret_cast<LeafNode*>(root)->born_key_num );
			if(new_leaf != NULL) { //a new leaf node is created, therefore adding a new inner node to hold
				InnerNode *inner = new_inner_node();
//for(int i = 0; i < LEAF_NUM; i++){
//	printf("new_leaf->keys[%d] = %lu\n",i,new_leaf->keys[i]);
//}
				inner->num_keys = 1;
				inner->keys[0] = new_leaf->keys[0];
//				printf("inner->keys[0] = %lu\n", inner->keys[0]);
				inner->children[0] = root;
				inner->children[1] = new_leaf;
				depth++; //depth=1
				root = inner;
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
			InnerInsert(key, reinterpret_cast<InnerNode*>(root), depth, &val, target_leaf);
		}
		return val;
	}

	inline InnerNode* InnerInsert(uint64_t key, InnerNode *inner, int d, MemNode** val, LeafNode** target_leaf) {
		unsigned k = 0;
		uint64_t upKey;
		InnerNode *new_sibling = NULL;
		//find the appropriate position of the new key
		while((k < inner->num_keys) && (key >= inner->keys[k])) {
			k++;
		}

		void *child = inner->children[k]; //search the descendent layer
#if BTPREFETCH
		for(int i = 0; i <= 64; i += 64) {
			prefetch(reinterpret_cast<char*>(child) + i);
		}
#endif
		//inserting at the lowest inner level
		if(d == 1) {
			//*target_inner = inner;
			LeafNode *new_leaf = LeafInsert(key, reinterpret_cast<LeafNode*>(child), val, target_leaf);
			if(new_leaf != NULL) {  //if a new leaf node is created
				InnerNode *toInsert = inner;
				//the inner node is full -> split it
				if(inner->num_keys == N) {
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
				InnerInsert(key, reinterpret_cast<InnerNode*>(child), d - 1, val, target_leaf);

			if(new_inner != NULL) {
				InnerNode *toInsert = inner;
				InnerNode *child_sibling = new_inner;

				unsigned treshold = (N + 1) / 2; //split equally
				//the current node is full, creating a new node to hold the inserted key
				if(inner->num_keys == N) {
					new_sibling = new_inner_node();
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
//				inner->writes++;
			} else {
//				inner->reads++;
//				if(d == 0){
//					checkConflict(inner->signature, 0);
//				}
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

#if BTREE_PROF
			writes++;
#endif
//			new_root->writes++;
		} 
		//else if(d == depth) {
//			if(d == 0){
//				checkConflict(reinterpret_cast<InnerNode*>(root)->signature, 0);
//			}
		//}
//		if (inner->num_keys == 0) printf("inner\n");
		//if (new_sibling->num_keys == 0) printf("sibling\n");
		return new_sibling; //return the newly-created node (if exists)
	}

	inline LeafNode* UnsortedLeafInsert(uint64_t key, LeafNode *leaf, MemNode** val) {
		unsigned leaf_key_num;
		unsigned toInsert_key_num;
		LeafNode *new_sibling = NULL;
		int k = 0;	

		for(k = LEAF_NUM - 1; k >= 0; k--){
			if(leaf->keys[k] != 0){
				break;
			}
		}
		
		leaf_key_num = k+1;
		if(leaf_key_num < LEAF_NUM){
			leaf->keys[leaf_key_num] = key;
			
#if DUMMY
			//leaf->values[leaf_key_num] = dummyval_;
			//*val = dummyval_;
#else
			//leaf->values[leaf_key_num] = GetMemNode();
			//*val = leaf->values[k];
#endif

			//printf("NO SPLIT\n");
			return NULL;
		}
		//this LeafNode is FULL
		//printf("SPLIT Begin\n");
		std::sort(leaf->keys, leaf->keys+LEAF_NUM);
		
		k = 0;
		while((k < leaf_key_num) && (leaf->keys[k] < key)) {
			++k;
		}
		//inserting a new key in the children
		LeafNode *toInsert = leaf;
		toInsert_key_num = leaf_key_num;
		new_sibling = new_leaf_node();
		if(leaf->right == NULL && k == leaf_key_num) {
			//new_sibling->num_keys = 0;
			for(int i = 0; i < LEAF_NUM; i++){
				new_sibling->keys[i]=0;
			}
			toInsert = new_sibling;
			toInsert_key_num=0;

			k = 0;
		} else {
			unsigned threshold = (LEAF_NUM + 1) / 2;
			//new_sibling->num_keys = leaf->num_keys - threshold;
			
			unsigned new_sibling_num_keys = leaf_key_num - threshold;

			//moving the keys above the threshold to the new sibling
			for(unsigned j = 0; j < new_sibling_num_keys; ++j) {
				//move the keys beyond threshold in old leaf to the new leaf
				new_sibling->keys[j] = leaf->keys[threshold + j];
				leaf->keys[threshold + j] = 0;
				//new_sibling->values[j] = leaf->values[threshold + j];
			}
			//leaf->num_keys = threshold;

			leaf_key_num = threshold;
			toInsert_key_num = threshold;
			if(k >= threshold) {
				k = k - threshold;
				toInsert = new_sibling;
				toInsert_key_num = new_sibling_num_keys;
			}
		}
		
		//inserting the newsibling at the right of the old leaf node

		if(leaf->right != NULL) leaf->right->left = new_sibling;
		new_sibling->right = leaf->right;
		new_sibling->left = leaf;
		leaf->right = new_sibling;
		//new_sibling->seq = 0;
		
		//printf("toInsert_key_num = %d\n", toInsert_key_num);
		toInsert->keys[toInsert_key_num] = key;
		/*
		for(int j = toInsert_key_num; j > k; j--) {
			toInsert->keys[j] = toInsert->keys[j - 1];
			toInsert->values[j] = toInsert->values[j - 1];
		}
		toInsert->keys[k] = key;
		*/
		//printf("[%2d] sig = %d, k = %u, key = %llu\n", sched_getcpu(), leaf->signature, k, key);

#if DUMMY
		//toInsert->values[toInsert_key_num] = dummyval_;
		//*val = dummyval_;
#else
		//toInsert->values[toInsert_key_num] = GetMemNode();
		//*val = toInsert->values[toInsert_key_num];
#endif
		//dummyval_ = NULL;
		//leaf->seq = leaf->seq + 1;
		//printf("SPLIT Here. new_sibling = %lu\n",new_sibling->signature);
		return new_sibling;
	}


	inline LeafNode* ShuffleLeafInsert(uint64_t key, LeafNode *leaf, MemNode** val, LeafNode** target_leaf) {
		//printf("I want to insert key = %d\n",key);
		LeafNode *new_sibling = NULL;
		//int idx = -1;
		//int seg_len = leaf->born_key_num == 8 ? HAL_LEN : EMP_LEN;
		if(leaf == root){
			if(first_leaf){//root node needs reinitialization
				for(int i = 0 ; i < SEGS; i++){
					for(int j = 0; j < EMP_LEN; j++){
						leaf->leaf_segs[i].keys[j] = 0;
					}
					leaf->leaf_segs[i].max_room = EMP_LEN;
				}
				first_leaf = false;
			}
		}
		//int retries = 0;
		//bool should_check_all = false;
		//while(true){
		//idx = key % SEGS;
		//if(leaf->leaf_segs[idx].key_num < seg_len){
				//break;
		//}else{
		//	should_check_all=true;
		//}
			//if(retries++ == 1){
				//should_check_all = true;
				//break;
			//}
		//}
		/*
		if(should_move){
			unsigned empty_idx;
			int total_room;
			for(int i = 0 ; i < SEGS; i++){
				total_room+=leaf->leaf_segs[i].max_room;
			}
			empty_idx = LEAF_NUM-total_room;
			for(int i = 0 ; i < SEGS; i++){
				
				if(leaf->leaf_segs[i].key_num == leaf->leaf_segs[i].max_room){
					leaf->keys[empty_idx]=
				}
			}
		}
		*/
		int idx = key % SEGS;
		//printf("leaf = %x, key = %lu, idx = %d\n", leaf, key, idx);
		bool should_check_all = false;
		//printf("key = %lu, idx = %d\n", key, idx);
		if(leaf->leaf_segs[idx].key_num >= leaf->leaf_segs[idx].max_room){
			idx = (idx + 1) % SEGS;
			if(leaf->leaf_segs[idx].key_num >= leaf->leaf_segs[idx].max_room){
				should_check_all = true;
			}
		}
		//bool should_check_all = leaf->leaf_segs[idx].key_num >= seg_len;
		if(!should_check_all){
			//[Case #1] Shuffle to an empty segment, insert immediately
			leaf->leaf_segs[idx].keys[leaf->leaf_segs[idx].key_num] = key;
			leaf->leaf_segs[idx].key_num++;
			
			/*
			if(idx == key % SEGS){
				insert_log.one_try++;
			}else{
				insert_log.two_try++;
			}
			*/

			*target_leaf = leaf;

			return NULL;
		} else{//should check if this node is full
			int idx2;
			for(idx2 = 0; idx2 < SEGS; idx2++){
				if(leaf->leaf_segs[idx2].key_num < leaf->leaf_segs[idx].max_room){
					//[Case #2] Failed shuffle twice, so scan the segments from the beginning
					leaf->leaf_segs[idx2].keys[leaf->leaf_segs[idx2].key_num] = key;
					leaf->leaf_segs[idx2].key_num++;
					//insert_log.check_all++;

					*target_leaf = leaf;

					return NULL;
				}
			}
			if(idx2 == SEGS){//REALLY FULL
				//split_ops++;
				//[Case #3] This LeafNode is really full
				//insert_log.split++;

				//printf("[%d] I should split(1)\n", key);
				/*
				if(leaf == root){
					for(int i = 0 ; i < SEGS; i++){
						for(int j = 0; j < seg_len; j++){	
							printf("leaf->leaf_segs[%d].keys[%d] = %lu\n",i,j,leaf->leaf_segs[i].keys[j]);
						}
					}
				}
				*/
				//int initial = 0;
				//if(leaf->born_key_num == 8){
				//	initial = 8;
				//}
				int temp_idx = 0;
				for(int i = 0; i < SEGS; i++){
					//printf("leaf->leaf_segs[i].max_room = %u\n", leaf->leaf_segs[i].max_room);
					for(int j = 0; j < leaf->leaf_segs[i].max_room; j++){	
						leaf->keys[LEAF_NUM - temp_idx] = leaf->leaf_segs[i].keys[j];
						temp_idx++;
					}
				}
				std::sort(leaf->keys, leaf->keys + LEAF_NUM);
				/*
				for(int i =0 ; i < LEAF_NUM-1; i++){
					if(leaf->keys[i]==leaf->keys[i+1]){
						duplicate_keys++;
					}
				}
				*/
				//for(int i = 0 ; i < LEAF_NUM; i++){
				//	printf("leaf->keys[%d] = %lu\n",i,leaf->keys[i]);
				//}
				
				unsigned k = 0;
				while((k < LEAF_NUM) && (leaf->keys[k] < key)) {
					++k;
				}
				/*
				for(int i = 0 ; i < LEAF_NUM; i++){
					printf("leaf->keys[%d] = %lu\n",i,leaf->keys[i]);
				}
				printf("inserted key = %lu, k = %u\n", key,k);
				*/
				LeafNode *toInsert = leaf;
				
				//for(int i = 0; i < SEGS ;i++){
				//	for(int j = 0; j < seg_len; j++){
				//		leaf->leaf_segs[i].keys[j] = 0;
				//	}
				//}
				//for(int i = 0; i < LEAF_NUM; i++){
				//	leaf->keys[i] = 0;	
				//}

				new_sibling = new_leaf_node();
				/*
				if(first_leaf){
					for(int i = 0 ; i < SEGS; i++){
						for(int j = 0; j < seg_len; j++){	
							printf("new_sibling->leaf_segs[%d].keys[%d] = %lu\n",i,j,new_sibling->leaf_segs[i].keys[j]);
						}
					}
					first_leaf = false;
				}
				*/
				if(leaf->right == NULL && k == LEAF_NUM) {//new leafnode at rightmost
					//rightmost++;
					//new_sibling->num_keys = 0;
					//new_sibling->born_key_num = 0;
					//for(int i = 0; i < LEAF_NUM; i++){
					//	new_sibling->keys[i]=0;
					//}
					toInsert = new_sibling;
					//toInsert_key_num = 0;
					//k = 0;
					//for(int i = 0; i < LEAF_NUM; i++){
					//	new_sibling->keys[i]=0;
					//}
					if(new_sibling->leaf_segs[0].key_num != 0){
						//printf("new_sibling = %x\n", new_sibling);
						for(int i = 0; i < SEGS; i++){
							//printf("new_sibling->leaf_segs[%d].key_num = %d\n", i, new_sibling->leaf_segs[i].key_num);
							new_sibling->leaf_segs[i].key_num = 0;
							//new_sibling->leaf_segs[i].max_room = EMP_LEN;
						}
					}
					for(int i = 0; i < SEGS; i++){
						new_sibling->leaf_segs[i].max_room = EMP_LEN;
					}
					/*
					for(int i = 0; i < SEGS; i++){
						printf("new_sibling->leaf_segs[i].key_num = %d\n", new_sibling->leaf_segs[i].key_num);
					}
					*/
					toInsert->leaf_segs[0].key_num = 1;
					toInsert->leaf_segs[0].keys[0] = key;
					toInsert->keys[0] = key; //keys[0] should be set here
				} else { //not at rightmost
					//not_rightmost++;
					/*
					if(k==LEAF_NUM){
						LeafNode* right = leaf->right;
						printf("right->sig = %ld\n", right->signature);
						for(int i = 0 ; i < LEAF_NUM; i++){
							printf("right->keys[%d] = %lu\n",i,right->keys[i]);
						}
					}
					*/
					unsigned threshold = (LEAF_NUM + 1) / 2; //8
				//new_sibling->num_keys = leaf->num_keys - threshold;
					unsigned new_sibling_num_keys = LEAF_NUM - threshold; //8
				
				//moving the keys above the threshold to the new sibling
				/*
				for(unsigned j = 0; j < new_sibling_num_keys; ++j) {
					//move the keys beyond threshold in old leaf to the new leaf
					new_sibling->keys[j] = leaf->keys[threshold + j];
					leaf->keys[threshold + j] = 0;
					//new_sibling->values[j] = leaf->values[threshold + j];
				}
				*/
					//new_sibling->born_key_num = new_sibling_num_keys; //new-born leaf

					//leaf->born_key_num = threshold; //old leaf

					for(int i = 0; i < SEGS; i++){
						leaf->leaf_segs[i].key_num = 0;
						new_sibling->leaf_segs[i].key_num = 0;
						leaf->leaf_segs[i].max_room = HAL_LEN;
						new_sibling->leaf_segs[i].max_room = HAL_LEN;
					}
					
					for(int i = 0; i < new_sibling_num_keys; i++){
						new_sibling->keys[i] = leaf->keys[threshold + i];
						//leaf->keys[threshold+i] = 0;
					}
					//leaf->num_keys = threshold;

					//leaf_key_num = threshold;
					//toInsert_key_num = threshold; 
					if(k >= threshold) {
						//k = k - threshold;
						toInsert = new_sibling;
						//toInsert_key_num = new_sibling_num_keys;
					}
					toInsert->leaf_segs[0].key_num = 1;
					toInsert->leaf_segs[0].keys[0] = key;
				}
				//inserting the newsibling at the right of the old leaf node
				if(leaf->right != NULL) {
					leaf->right->left = new_sibling;
				}
				new_sibling->right = leaf->right;
				new_sibling->left = leaf;
				leaf->right = new_sibling;

				*target_leaf = toInsert;
			}
		}
		return new_sibling;
	}

	inline LeafNode* SimpleLeafInsert(uint64_t key, LeafNode *leaf, MemNode** val) {
		//unsigned leaf_key_num;
		//unsigned toInsert_key_num;
		LeafNode *new_sibling = NULL;
		unsigned  k = 0;	
		/*
		for(k = LEAF_NUM - 1; k >= 0; k--){
			if(leaf->keys[k] != 0){
				break;
			}
		}
		leaf_key_num = k + 1;
		*/
		//printf("leaf_key_num = %d, leaf->num_keys = %d\n", leaf_key_num, leaf->num_keys);
		//k = 0;
		while((k < leaf->num_keys) && (leaf->keys[k] < key)) {
			++k;
		}

		if((k < leaf->num_keys) && (leaf->keys[k] == key)) {
			//*newKey = false;
#if BTPREFETCH
			prefetch(reinterpret_cast<char*>(leaf->values[k]));
#endif
			*val = leaf->values[k];

			return NULL;
		}

		//*newKey = true;
		//inserting a new key in the children
		LeafNode *toInsert = leaf;
		//toInsert_key_num = leaf_key_num;
		//SPLIT THE NODE WHEN FULL
		if(leaf->num_keys == LEAF_NUM) {
			//split_ops++;
			//printf("[%d] I should split(2)\n", key);
		/*	
		for(int i = 0 ; i < LEAF_NUM; i++){
			printf("leaf->keys[%d] = %lu\n",i,leaf->keys[i]);
		}
		printf("inserted key = %lu, k = %u\n", key,k);
		*/
			new_sibling = new_leaf_node();
			if(leaf->right == NULL && k == leaf->num_keys) {//new leafnode at rightmost
			//rightmost++;
				new_sibling->num_keys = 0;
				//new_sibling->born_key_num=0;
				//for(int i = 0; i < LEAF_NUM; i++){
				//	new_sibling->keys[i]=0;
				//}
				toInsert = new_sibling;
				//toInsert_key_num = 0;
				k = 0;
			} else { //not at rightmost
			//not_rightmost++;
				unsigned threshold = (LEAF_NUM + 1) / 2; //8
				new_sibling->num_keys = leaf->num_keys - threshold;
				//unsigned new_sibling_num_keys = leaf_key_num - threshold; //8

				//new_sibling->born_key_num = new_sibling_num_keys;
				//leaf->born_key_num = threshold;

				//moving the keys above the threshold to the new sibling
				for(unsigned j = 0; j < new_sibling->num_keys; ++j) {
					//move the keys beyond threshold in old leaf to the new leaf
					new_sibling->keys[j] = leaf->keys[threshold + j];
					leaf->keys[threshold + j] = 0;
					new_sibling->values[j] = leaf->values[threshold + j];
				}

				leaf->num_keys = threshold;

				//leaf_key_num = threshold;
				//toInsert_key_num = threshold; 
				if(k >= threshold) {
					k = k - threshold;
					toInsert = new_sibling;
					//toInsert_key_num = new_sibling_num_keys;
				}
			}
			//inserting the newsibling at the right of the old leaf node
			if(leaf->right != NULL) leaf->right->left = new_sibling;
			new_sibling->right = leaf->right;
			new_sibling->left = leaf;
			leaf->right = new_sibling;
			new_sibling->seq = 0;
		}

#if BTPREFETCH
		prefetch(reinterpret_cast<char*>(dummyval_));
#endif
		//printf("toInsert_key_num = %d, toInsert->num_keys = %d\n", toInsert_key_num, toInsert->num_keys);
		for(int j = toInsert->num_keys; j > k; j--) {
			toInsert->keys[j] = toInsert->keys[j - 1];
			toInsert->values[j] = toInsert->values[j - 1];
		}

		toInsert->num_keys = toInsert->num_keys + 1;
		toInsert->keys[k] = key;

		//printf("[%2d] sig = %d, k = %u, key = %llu\n", sched_getcpu(), leaf->signature, k, key);
#if DUMMY
		toInsert->values[k] = dummyval_;
		//printf("toInsert->values[k] = %x\n",dummyval_);
		*val = dummyval_;
#else
		toInsert->values[k] = GetMemNode();
		*val = toInsert->values[k];
#endif

		assert(*val != NULL);
		dummyval_ = NULL;

		leaf->seq = leaf->seq + 1;
		//printf("toInsert = %x\n",toInsert );
		return new_sibling;
	}

//Insert a key at the leaf level
//Return: the new node where the new key resides, NULL if no new node is created
//@val: storing the pointer to new value in val
	inline LeafNode* LeafInsert(uint64_t key, LeafNode *leaf, MemNode** val, LeafNode** target_leaf) {
		//printf("LeafInsert->born_key_num = %d\n",leaf->born_key_num);
	#if SHUFFLE_INSERT
		return ShuffleLeafInsert(key, leaf, val, target_leaf);
	#else
		return SimpleLeafInsert(key, leaf, val);
	#endif
	}

	Memstore::Iterator* GetIterator() {
		return new MemstoreAlexTree::Iterator(this);
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
		if(leaf->num_keys == LEAF_NUM) {
			new_sibling = new_leaf_node();
			if(leaf->right == NULL && k == leaf->num_keys) {
				new_sibling->num_keys = 0;
				toInsert = new_sibling;
				k = 0;
			} else {
				unsigned threshold = (LEAF_NUM + 1) / 2;
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
	//int waccess[4][CONFLICT_BUFFER_LEN];
	//int raccess[4][CONFLICT_BUFFER_LEN];
	int windex[4];
	int rindex[4];
};
//__thread RTMArena* MemstoreAlexTree::arena_ = NULL;
//__thread bool MemstoreAlexTree::localinit_ = false;
}
#endif

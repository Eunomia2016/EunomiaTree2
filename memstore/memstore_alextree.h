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

#define SHUFFLE_LOG 0
#define SEGS 4
#define SEG_LEN 4
#define EMP_LEN  4
#define HAL_LEN  2

#define UNSORTED_INSERT 0

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

	uint64_t inserts;
	uint64_t shifts;

#if SHUFFLE_LOG
	struct Leaf_Seg{
		uint64_t keys[EMP_LEN];
		MemNode* vals[EMP_LEN];
		unsigned key_num;
		uint64_t paddings[8];
	};
#endif

	struct LeafNode {
		LeafNode() : num_keys(0), born_key_num(-1){
			//signature = __sync_fetch_and_add(&leaf_id, 1);
#if SHUFFLE_KEYS
			Seg_lens = {4,4,4,3};
			All_Full = false;
#endif
		} //, writes(0), reads(0) {}
		int born_key_num;
		unsigned signature;
		unsigned num_keys;
		
#if SHUFFLE_LOG
		Leaf_Seg leaf_segs[SEGS];
#endif

#if SHUFFLE_KEYS
		bool All_Full;
		bool Lock[SEGS];
		bool Full[SEGS];
		int Num[SEGS];
		int Seg_lens[SEGS];
#endif

		uint64_t keys[LEAF_NUM];
		MemNode *values[LEAF_NUM];

		LeafNode *left;
		LeafNode *right;
		uint64_t seq;
	};

	struct InnerNode {
		InnerNode() : num_keys(0) {
			//signature = __sync_fetch_and_add(&inner_id, -1);
		}//, writes(0), reads(0) {}
//		uint64_t padding[8];
		int signature;
		unsigned num_keys;
		uint64_t 	 keys[N];
		void*	 children[N + 1];
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
		tableid = _tableid;
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

		//printf("inserts = %d, shifts = %d\n", inserts, shifts);
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

	inline Memstore::InsertResult GetWithInsert(uint64_t key) {
		ThreadLocalInit();

		InsertResult res = Insert_rtm(key);

#if DUMMY
		if(dummyval_ == NULL) {
			dummyval_ = GetMemNode();
		}
		if(dummyleaf_ == NULL) {
			dummyleaf_ = new LeafNode();
		}
#endif

		res.node=GetMemNode();
		return res;
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

	inline Memstore::InsertResult Insert_rtm(uint64_t key) {
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
			reinterpret_cast<LeafNode*>(root)->born_key_num = 0;
			//printf("root->born_key_num = %d\n", reinterpret_cast<LeafNode*>(root)->born_key_num );
			depth = 0;
		}

		MemNode* val = NULL;
		if(depth == 0) {
			LeafNode *new_leaf = LeafInsert(key, reinterpret_cast<LeafNode*>(root), &val, &newKey);
			//printf("root->born_key_num = %d\n", reinterpret_cast<LeafNode*>(root)->born_key_num );
			if(new_leaf != NULL) { //a new leaf node is created, therefore adding a new inner node to hold
				InnerNode *inner = new_inner_node();
				inner->num_keys = 1;
				inner->keys[0] = new_leaf->keys[0];
				inner->children[0] = root;
				inner->children[1] = new_leaf;
				depth++;
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
			InnerInsert(key, reinterpret_cast<InnerNode*>(root), depth, &val, &newKey);
		}
		return {val, newKey};
	}

	inline InnerNode* InnerInsert(uint64_t key, InnerNode *inner, int d, MemNode** val, bool* newKey) {
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
			LeafNode *new_leaf = LeafInsert(key, reinterpret_cast<LeafNode*>(child), val, newKey);
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
				InnerInsert(key, reinterpret_cast<InnerNode*>(child), d - 1, val, newKey);

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
		//printf("LeafInsert->born_key_num = %d\n",leaf->born_key_num);
#if SHUFFLE_KEYS
		bool should_split = leaf->All_Full;
		bool should_check_all_full = false;
		unsigned idx = 0 ;
		unsigned check_full = 0;
		if(!should_split){
			while(true){
				idx = Shuffle();
				if(!leaf->Lock[idx]){
					if(!leaf->Full[idx]){
						break;				
					}
					check_full++;
					if(check_full>=2){
						should_check_all_full = true;
						break;
					}
				}
			}
			if(!should_check_all_full){ //should not check all_full
				leaf->Lock[idx] = true;
				//leaf->keys[idx*SEG_LEN+leaf->Num[idx]]=key;
				leaf->Num[idx]++;
				if(leaf->Num[idx] >= leaf->Seg_lens[idx]){
					leaf->Full[idx]=true;
				}
				leaf->Lock[idx] = false;
			}else{ //should check all full
				int seg = 0;
				for(seg = 0; seg < SEGS; seg++){
					if(!leaf->Full[seg]){
						break;
					}
				}
				if(seg==SEGS){//all full
					leaf->All_Full=true;
					should_split=true;
				}else{//not all full
					int  i = 0;
					for(i = seg; i < SEGS; i++){
						while(leaf->Lock[i]){cpu_relax();}
						if(!leaf->Full[i]){
								leaf->Lock[idx] = true;
								//leaf->keys[idx*SEG_LEN+leaf->Num[idx]]=key;
								leaf->Num[idx]++;
								if(leaf->Num[idx] >= leaf->Seg_lens[idx]){
									leaf->Full[idx]=true;
								}
								leaf->Lock[idx] = false;
						}
					}
					if(i==SEGS){
						leaf->All_Full=true;
						should_split=true;
					}
				}
			}
		}
#endif

#if SHUFFLE_LOG
		int idx = -1;
		int seg_len = EMP_LEN;
		if(leaf->born_key_num==8){
			seg_len = HAL_LEN;
		}
		int retries = 0;
		bool should_check_all = false;
		do{
			if(retries++ == 2){
				break;
				should_check_all = true;
			}
			
			idx = Shuffle();
		}while(leaf->leaf_segs[idx].key_num == seg_len);
		if(!should_check_all){
			leaf->leaf_segs[idx].keys[leaf->leaf_segs[idx].key_num] = key;
			
#if DUMMY
			leaf->leaf_segs[idx].vals[leaf->leaf_segs[idx].key_num] = dummyval_;
			*val = dummyval_;
#else
			leaf->values[leaf_key_num] = GetMemNode();
			*val = leaf->values[k];
#endif
			leaf->leaf_segs[idx].key_num++;
		}
		
#endif

#if UNSORTED_INSERT
		unsigned leaf_key_num;
		unsigned toInsert_key_num;
		LeafNode *new_sibling = NULL;
		int k = 0;	

		for(k=LEAF_NUM-1;k>=0;k--){
			if(leaf->keys[k] != 0){
				break;
			}
		}
		leaf_key_num = k+1;
		if(leaf_key_num < LEAF_NUM){
			leaf->keys[leaf_key_num]=key;
			
#if DUMMY
			leaf->values[leaf_key_num] = dummyval_;
			*val = dummyval_;
#else
			leaf->values[leaf_key_num] = GetMemNode();
			*val = leaf->values[k];
#endif

			//printf("NO SPLIT\n");
			return leaf;
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
				new_sibling->values[j] = leaf->values[threshold + j];
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
		toInsert->values[toInsert_key_num] = dummyval_;
		*val = dummyval_;
	#else
		toInsert->values[toInsert_key_num] = GetMemNode();
		*val = toInsert->values[toInsert_key_num];
	#endif

		dummyval_ = NULL;
		//leaf->seq = leaf->seq + 1;
		
		//printf("SPLIT Here. new_sibling = %lu\n",new_sibling->signature);
		return new_sibling;
#endif

		unsigned leaf_key_num;
		unsigned toInsert_key_num;
		LeafNode *new_sibling = NULL;
		int k = 0;	

		for(k=LEAF_NUM-1;k>=0;k--){
			if(leaf->keys[k] != 0){
				break;
			}
		}
		leaf_key_num = k+1;
		//printf("leaf_key_num = %d, leaf->num_keys = %d\n", leaf_key_num, leaf->num_keys);
		k = 0;
		while((k < leaf_key_num) && (leaf->keys[k] < key)) {
			++k;
		}

		if((k < leaf_key_num) && (leaf->keys[k] == key)) {
			*newKey = false;
	#if BTPREFETCH
			prefetch(reinterpret_cast<char*>(leaf->values[k]));
	#endif
			//*val = leaf->values[k];
	#if BTREE_PROF
			reads++;
	#endif
			assert(*val != NULL);
			return NULL;
		}

		*newKey = true;
		//inserting a new key in the children
		LeafNode *toInsert = leaf;
		toInsert_key_num = leaf_key_num;
		//SPLIT THE NODE WHEN FULL
		if(leaf_key_num == LEAF_NUM) {
			new_sibling = new_leaf_node();
			if(leaf->right == NULL && k == leaf_key_num) {
				//new_sibling->num_keys = 0;
				new_sibling->born_key_num=0;
				toInsert = new_sibling;
				toInsert_key_num = 0;
				k = 0;
			} else {
				unsigned threshold = (LEAF_NUM + 1) / 2; //8
				//new_sibling->num_keys = leaf->num_keys - threshold;
				unsigned new_sibling_num_keys = leaf_key_num - threshold; //8
				new_sibling->born_key_num=new_sibling_num_keys;
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
		}

	#if BTPREFETCH
		prefetch(reinterpret_cast<char*>(dummyval_));
	#endif
		//printf("toInsert_key_num = %d, toInsert->num_keys = %d\n", toInsert_key_num, toInsert->num_keys);
		for(int j = toInsert_key_num; j > k; j--) {
			toInsert->keys[j] = toInsert->keys[j - 1];
			//toInsert->values[j] = toInsert->values[j - 1];
		}

		//toInsert->num_keys = toInsert->num_keys + 1;

		toInsert->keys[k] = key;

		//printf("[%2d] sig = %d, k = %u, key = %llu\n", sched_getcpu(), leaf->signature, k, key);
	#if DUMMY
		//toInsert->values[k] = dummyval_;
		//printf("toInsert->values[k] = %x\n",dummyval_);
		//*val = dummyval_;
	#else
		toInsert->values[k] = GetMemNode();
		*val = toInsert->values[k];
	#endif

		//assert(*val != NULL);
		dummyval_ = NULL;

		//leaf->seq = leaf->seq + 1;
		//printf("toInsert = %x\n",toInsert );
		return new_sibling;
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

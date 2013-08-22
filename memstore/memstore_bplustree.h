#ifndef MEMSTOREBPLUSTREE_H
#define MEMSTOREBPLUSTREE_H

#include <stdlib.h>
#include <iostream>
#include "util/rtmScope.h" 
#include "util/rtm_arena.h"
#include "util/mutexlock.h"
#include "port/port_posix.h"
#include "memstore.h"
#define M  5
#define N  5

#define BTREE_PROF 0
#define BTREE_LOCK 1

//static uint64_t writes = 0;
//static uint64_t reads = 0;
	
/*static int total_key = 0;
static int total_nodes = 0;
static uint64_t rconflict = 0;
static uint64_t wconflict = 0;
*/
namespace leveldb {
class MemstoreBPlusTree: public Memstore {

//Test purpose	
public:
	struct LeafNode {
		LeafNode() : num_keys(0){}//, writes(0), reads(0) {}
//		uint64_t padding[4];
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
    	InnerNode() : num_keys(0) {}//, writes(0), reads(0) {}
//		uint64_t padding[8];
//		unsigned padding;
		unsigned num_keys;
		uint64_t 	 keys[N];
		void*	 children[N+1];
//		uint64_t writes;
//		uint64_t reads;
//		uint64_t padding1[8];
	};

	//The result object of the delete function
	struct DeleteResult {
		DeleteResult(): value(0), freeNode(false), upKey(-1){}
		Memstore::MemNode* value;  //The value of the record deleted
		bool freeNode;	//if the children node need to be free
		uint64_t upKey; //the key need to be updated -1: default value
	};	

	class Iterator: public Memstore::Iterator {
	 public:
	  // Initialize an iterator over the specified list.
	  // The returned iterator is not valid.
	  Iterator(){};
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


	
	~MemstoreBPlusTree() {
		prof.reportAbortStatus();
		//PrintList();
		//PrintStore();
		//printf("rwconflict %ld\n", rconflict);
		//printf("wwconflict %ld\n", wconflict);
		//printf("depth %d\n",depth);
		//printf("reads %ld\n",reads);
		//printf("writes %ld\n", writes);
		//printf("calls %ld touch %ld avg %f\n", calls, reads + writes,  (float)(reads + writes)/(float)calls );
#if BTREE_PROF
		printf("calls %ld avg %f writes %f\n", calls, (float)(reads + writes)/(float)calls,(float)(writes)/(float)calls );
#endif
	
		//printTree();
		//top();
	}
	  	  
	inline void ThreadLocalInit(){
		if(false == localinit_) {
			arena_ = new RTMArena();

			dummyval_ = new MemNode();
			dummyval_->value = NULL;
			
			localinit_ = true;
		}
			
	}
	
	inline LeafNode* new_leaf_node() {
			LeafNode* result = (LeafNode *)(arena_->AllocateAligned(sizeof(LeafNode)));
			return result;
	}
		
	inline InnerNode* new_inner_node() {
			InnerNode* result = (InnerNode *)(arena_->AllocateAligned(sizeof(InnerNode)));
			return result;
	}

	inline LeafNode* FindLeaf(uint64_t key) {
		InnerNode* inner;
		register void* node= root;
		register unsigned d= depth;
		unsigned index = 0;
		while( d-- != 0 ) {
			index = 0;
			inner= reinterpret_cast<InnerNode*>(node);
			while((index < inner->num_keys) && (key >= inner->keys[index])) {
			   ++index;
			}				
			node= inner->children[index];
		}
		return reinterpret_cast<LeafNode*>(node);
	}

	inline MemNode* Get(uint64_t key)
	{
		RTMArenaScope begtx(&rtmlock, &prof, arena_);

		InnerNode* inner;
		register void* node= root;
		register unsigned d= depth;
		unsigned index = 0;
		while( d-- != 0 ) {
			index = 0;
			inner= reinterpret_cast<InnerNode*>(node);
//			reads++;
			while((index < inner->num_keys) && (key >= inner->keys[index])) {
			   ++index;
			}				
			node= inner->children[index];
		}
		LeafNode* leaf= reinterpret_cast<LeafNode*>(node);
//		reads++;
		
	    unsigned k = 0;
		while((k < leaf->num_keys) && (leaf->keys[k]<key)) {
		   ++k;
		}
		if( leaf->keys[k] == key ) {
			return leaf->values[k];
		} else {
			return NULL;
		}
	}
	

	inline MemNode* Put(uint64_t k, uint64_t* val) 
	{
		ThreadLocalInit();
		MemNode *node = GetWithInsert(k);
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
		
		while((slot < cur->num_keys) && (cur->keys[slot] < key)) {
			slot++;
		}
		
		return slot;
	}

	inline Memstore::MemNode* removeLeafEntry(LeafNode* cur, int slot) {
		
		assert(slot < cur->num_keys);
		
		Memstore::MemNode* value = cur->values[slot];

		cur->num_keys--;

		//The key deleted is the last one
		if (slot == cur->num_keys)
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

		//FIXME: check if the node has been logically delete here.
		if(cur->values[slot]->value != (uint64_t *)1 || cur->values[slot]->counter != 0)
			return NULL;
		else
		   cur->values[slot]->value = (uint64_t *)2;

		DeleteResult *res = new DeleteResult();
		
		//step 2. remove the entry of the key, and get the deleted value
		res->value = removeLeafEntry(cur, slot);

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
		if (slot == cur->num_keys) {
			cur->num_keys--;
			return;
		}

		//replace the children slot
		for(int i = slot + 1; i <= cur->num_keys; i++)
			cur->children[i - 1] = cur->children[i];
		
		//delete the first entry, upkey is needed
		if (slot == 0) {

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

	inline DeleteResult* InnerDelete(uint64_t key, InnerNode* cur ,int depth) 
	{
		
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

		//step 3. Remove the entry if the total children node has been removed
		if(res->freeNode) {
			//FIXME: Should free the children node here
		  	
			//remove the node from the parent node
			removeInnerEntry(cur, slot, res);
			res->freeNode = false;
			return res;	
		}

		//step 4. update the key if needed
		if(res->upKey != -1) {
			if (slot != 0) {
				 cur->keys[slot - 1] = res->upKey;
				 res->upKey = -1;
			}
		}

		return res;
	
	}


	
	inline Memstore::MemNode* Delete_rtm(uint64_t key) {
#if BTREE_LOCK
		MutexSpinLock lock(&slock);
#else
		RTMArenaScope begtx(&rtmlock, &prof, arena_);
#endif


		DeleteResult* res = NULL;
		if (depth == 0) {
			//Just delete the record from the root
			res = LeafDelete(key, (LeafNode*)root);
		}
		else {
			res = InnerDelete(key, (InnerNode*)root, depth);
		}

		if (res == NULL)
			return NULL;
	
		if(res->freeNode) 
			root = NULL;

		return res->value;
	}



	inline Memstore::MemNode* GetWithDelete(uint64_t key) {
	
		ThreadLocalInit();

		MemNode* value = Delete_rtm(key);
		
		if(dummyval_ == NULL)
			dummyval_ = new MemNode();

		return value;
		
	}


	inline Memstore::MemNode* GetWithInsert(uint64_t key) {

		ThreadLocalInit();
//		NewNodes *dummy= new NewNodes(depth);
//		NewNodes dummy(depth);

/*		MutexSpinLock lock(&slock);
		current_tid = tid;
		windex[tid] = 0;
		rindex[tid] = 0;
	*/	
		MemNode* value = Insert_rtm(key);
		
		if(dummyval_ == NULL)
			dummyval_ = new MemNode();

		return value;
		
//		Insert_rtm(key, &dummy);

/*		if (dummy->leaf->num_keys <=0) delete dummy->leaf;
		for (int i=dummy->used; i<dummy->d;i++) {
			delete dummy->inner[i];
			//if (dummy.inner[i]->num_keys > 0) printf("!!!\n");
		}*/
//		delete dummy;
		
	}
	
	inline Memstore::MemNode* Insert_rtm(uint64_t key) {
#if BTREE_LOCK
		MutexSpinLock lock(&slock);
#else
		RTMArenaScope begtx(&rtmlock, &prof, arena_);
#endif

#if BTREE_PROF
		calls++;
#endif

		MemNode* val = NULL;
		if (depth == 0) {
			LeafNode *new_leaf = LeafInsert(key, reinterpret_cast<LeafNode*>(root), &val);
			if (new_leaf != NULL) {
				InnerNode *inner = new_inner_node();
				inner->num_keys = 1;
				inner->keys[0] = new_leaf->keys[0];
				inner->children[0] = root;
				inner->children[1] = new_leaf;
				depth++;
				root = inner;
//				checkConflict(inner, 1);
//				checkConflict(&root, 1);
#if BTREE_PROF
				writes++;
#endif
//				inner->writes++;
			}
//			else checkConflict(&root, 0);
		}
		else {
			InnerInsert(key, reinterpret_cast<InnerNode*>(root), depth, &val);
			
		}

		return val;
	}

	inline InnerNode* InnerInsert(uint64_t key, InnerNode *inner, int d, MemNode** val) {
	
		unsigned k = 0;
		uint64_t upKey;
		InnerNode *new_sibling = NULL;
		//printf("key %lx\n",key);
		//printf("d %d\n",d);
		while((k < inner->num_keys) && (key >= inner->keys[k])) {
		   ++k;
		}
		void *child = inner->children[k];
/*		if (child == NULL) {
			printf("Key %lx\n");
			printInner(inner, d);
		}*/
		//printf("child %d\n",k);
		if (d == 1) {
			//printf("leafinsert\n");
			//printTree();
			
			LeafNode *new_leaf = LeafInsert(key, reinterpret_cast<LeafNode*>(child), val);
			//printTree();
			if (new_leaf != NULL) {
				InnerNode *toInsert = inner;
				if (inner->num_keys == N) {										
					unsigned treshold= (N+1)/2;
					new_sibling = new_inner_node();
					
					new_sibling->num_keys= inner->num_keys -treshold;
					//printf("sibling num %d\n",new_sibling->num_keys);
                    			for(unsigned i=0; i < new_sibling->num_keys; ++i) {
                    				new_sibling->keys[i]= inner->keys[treshold+i];
                        			new_sibling->children[i]= inner->children[treshold+i];
                    			}	
                    			new_sibling->children[new_sibling->num_keys] = inner->children[inner->num_keys];
                    			inner->num_keys= treshold-1;
					//printf("remain num %d\n",inner->num_keys);
					upKey = inner->keys[treshold-1];
					//printf("UP %lx\n",upKey);
					if (new_leaf->keys[0] >= upKey) {
						toInsert = new_sibling;
						if (k >= treshold) k = k - treshold; 
						else k = 0;
					}
					inner->keys[N-1] = upKey;
//					checkConflict(new_sibling, 1);
#if BTREE_PROF
					writes++;
#endif
//					new_sibling->writes++;
					
				}
				
				for (int i=toInsert->num_keys; i>k; i--) {
					toInsert->keys[i] = toInsert->keys[i-1];
					toInsert->children[i+1] = toInsert->children[i];					
				}
				toInsert->num_keys++;
				toInsert->keys[k] = new_leaf->keys[0];
				toInsert->children[k+1] = new_leaf;
//				checkConflict(inner, 1);
#if BTREE_PROF
				writes++;
#endif
//				inner->writes++;
			}
			else {
#if BTREE_PROF
				reads++;
#endif
//				inner->reads++;
//				checkConflict(inner, 0);
			}
			
//			if (new_sibling!=NULL && new_sibling->num_keys == 0) printf("sibling\n");
		}
		else {
			//printf("inner insert\n");
			bool s = true;
			InnerNode *new_inner = 
				InnerInsert(key, reinterpret_cast<InnerNode*>(child), d - 1, val);
			
			
			if (new_inner != NULL) {
				InnerNode *toInsert = inner;
				InnerNode *child_sibling = new_inner;
				unsigned treshold= (N+1)/2;
				if (inner->num_keys == N) {										
					
					new_sibling = new_inner_node();
					new_sibling->num_keys= inner->num_keys -treshold;
					
                    for(unsigned i=0; i < new_sibling->num_keys; ++i) {
                    	new_sibling->keys[i]= inner->keys[treshold+i];
                        new_sibling->children[i]= inner->children[treshold+i];
                    }
                    new_sibling->children[new_sibling->num_keys]=
                                inner->children[inner->num_keys];
                                
                    //XXX: should threshold ???
                    inner->num_keys= treshold-1;
					
					upKey = inner->keys[treshold-1];
					//printf("UP %lx\n",upKey);
					if (key >= upKey) {
						toInsert = new_sibling;
						if (k >= treshold) k = k - treshold; 
						else k = 0;
					}

					//XXX: what is this used for???
					inner->keys[N-1] = upKey;

#if BTREE_PROF
					writes++;
#endif
//					new_sibling->writes++;
//					checkConflict(new_sibling, 1);
				}	
				
				for (int i=toInsert->num_keys; i>k; i--) {
					toInsert->keys[i] = toInsert->keys[i-1];
					toInsert->children[i+1] = toInsert->children[i];					
				}
			
				toInsert->num_keys++;
				toInsert->keys[k] = reinterpret_cast<InnerNode*>(child)->keys[N-1];

				toInsert->children[k+1] = child_sibling;
														
#if BTREE_PROF
				writes++;
#endif
//				inner->writes++;
//				checkConflict(inner, 1);
			}
			else {
#if BTREE_PROF
				reads++;
#endif
//				inner->reads++;
//				checkConflict(inner, 0);
			}
	
			
		}
		
		if (d==depth && new_sibling != NULL) {
			InnerNode *new_root = new_inner_node();			
			new_root->num_keys = 1;
			new_root->keys[0]= upKey;
			new_root->children[0] = root;
			new_root->children[1] = new_sibling;
			root = new_root;
			depth++;	

#if BTREE_PROF
			writes++;
#endif
//			new_root->writes++;
//			checkConflict(new_root, 1);
//			checkConflict(&root, 1);
		}
//		else if (d == depth) checkConflict(&root, 0);
//		if (inner->num_keys == 0) printf("inner\n");
		//if (new_sibling->num_keys == 0) printf("sibling\n");
		return new_sibling;
	}

	inline LeafNode* LeafInsert(uint64_t key, LeafNode *leaf, MemNode** val) {
		LeafNode *new_sibling = NULL;
		unsigned k = 0;
		while((k < leaf->num_keys) && (leaf->keys[k]<key)) {
		   ++k;
		}

		if((k < leaf->num_keys) && (leaf->keys[k] == key)) {
			*val = leaf->values[k];
#if BTREE_PROF
			reads++;
#endif
			assert(*val != NULL);
			return NULL;
		}
			

		LeafNode *toInsert = leaf;
		if (leaf->num_keys == M) {
			new_sibling = new_leaf_node();
			unsigned threshold= (M+1)/2;
			new_sibling->num_keys= leaf->num_keys -threshold;
            for(unsigned j=0; j < new_sibling->num_keys; ++j) {
            	new_sibling->keys[j]= leaf->keys[threshold+j];
				new_sibling->values[j]= leaf->values[threshold+j];
            }
            leaf->num_keys= threshold;
			

			if (k>=threshold) {
				k = k - threshold;
				toInsert = new_sibling;
			}

			if (leaf->right != NULL) leaf->right->left = new_sibling;
			new_sibling->right = leaf->right;
			new_sibling->left = leaf;
			leaf->right = new_sibling;
			new_sibling->seq = 0;
#if BTREE_PROF
			writes++;
#endif
//			new_sibling->writes++;
//			checkConflict(new_sibling, 1);
		}
		
		
		//printf("IN LEAF1 %d\n",toInsert->num_keys);
		//printTree();

        for (int j=toInsert->num_keys; j>k; j--) {
			toInsert->keys[j] = toInsert->keys[j-1];
			toInsert->values[j] = toInsert->values[j-1];
        }
		
		toInsert->num_keys = toInsert->num_keys + 1;
		toInsert->keys[k] = key;
		toInsert->values[k] = dummyval_;
		*val = dummyval_;
		assert(*val != NULL);
		dummyval_ = NULL;
		
#if BTREE_PROF
		writes++;
#endif
//		leaf->writes++;
//		checkConflict(leaf, 1);
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

//Test Purpose	
public:
		
		static __thread RTMArena* arena_;	  // Arena used for allocations of nodes
		static __thread bool localinit_;
		static __thread MemNode *dummyval_;
		
		char padding1[64];
		void *root;
		int depth;

		char padding2[64];
		RTMProfile prof;
		char padding3[64];
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

/*		
		int current_tid;
		void *waccess[4][30];
		void *raccess[4][30];
		int windex[4];
		int rindex[4];*/
		
};

//__thread RTMArena* MemstoreBPlusTree::arena_ = NULL;
//__thread bool MemstoreBPlusTree::localinit_ = false;

}

#endif

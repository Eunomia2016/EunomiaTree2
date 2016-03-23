#include "memstore/memstore_alextree.h"

namespace leveldb {

__thread RTMArena* MemstoreAlexTree::arena_ = NULL;
__thread bool MemstoreAlexTree::localinit_ = false;
__thread Memstore::MemNode *MemstoreAlexTree::dummyval_ = NULL;
__thread MemstoreAlexTree::LeafNode *MemstoreAlexTree::dummyleaf_ = NULL;

void MemstoreAlexTree::printLeaf(LeafNode *n) {
	for(int i = 0; i < depth; i++)
		printf(" ");
	printf("Leaf Addr %lx Key num %d  :", n, n->num_keys);
	for(int i = 0; i < n->num_keys; i++)
		printf("key  %ld \t ", n->kvs[i].key);
//			printf("key  %ld value %ld \t ",n->keys[i], n->values[i]->value);
	printf("\n");
//		total_key += n->num_keys;
}


void MemstoreAlexTree::printInner(InnerNode *n, unsigned depth) {
	for(int i = 0; i < this->depth - depth; i++)
		printf(" ");
	printf("Inner %d Key num %d  :", depth, n->num_keys);
	for(int i = 0; i < n->num_keys; i++)
		printf("\t%ld	", n->keys[i]);
	printf("\n");
	for(int i = 0; i <= n->num_keys; i++)
		if(depth > 1) printInner(reinterpret_cast<InnerNode*>(n->children[i]), depth - 1);
		else printLeaf(reinterpret_cast<LeafNode*>(n->children[i]));
}

void MemstoreAlexTree::PrintStore() {
	printf("===============B+ Tree=========================\n");
	if(root == NULL) {
		printf("Empty Tree\n");
		return;
	}
//		 total_key = 0;
	if(depth == 0) printLeaf(reinterpret_cast<LeafNode*>(root));
	else {
		printInner(reinterpret_cast<InnerNode*>(root), depth);
	}
	printf("========================================\n");
//		 printf("Total key num %d\n", total_key);
}

void MemstoreAlexTree::PrintList() {
	void* min = root;
	int d = depth;
	while(d > 0) {
		min = reinterpret_cast<InnerNode*>(min)->children[0];
		d--;
	}
	LeafNode *leaf = reinterpret_cast<LeafNode*>(min);
	while(leaf != NULL) {
		printLeaf(leaf);
		if(leaf->right != NULL)
			assert(leaf->right->left == leaf);
		leaf = leaf->right;
	}
}
/*

void MemstoreAlexTree::topLeaf(LeafNode *n) {
	total_nodes++;
	if (n->writes > 40) printf("Leaf %lx , w %ld , r %ld\n", n, n->writes, n->reads);

}

void MemstoreAlexTree::topInner(InnerNode *n, unsigned depth){
	total_nodes++;
	if (n->writes > 40) printf("Inner %lx depth %d , w %ld , r %ld\n", n, depth, n->writes, n->reads);
	for (int i=0; i<=n->num_keys;i++)
		if (depth > 1) topInner(reinterpret_cast<InnerNode*>(n->children[i]), depth-1);
		else topLeaf(reinterpret_cast<LeafNode*>(n->children[i]));
}

void MemstoreAlexTree::top(){
	if (depth == 0) topLeaf(reinterpret_cast<LeafNode*>(root));
	else topInner(reinterpret_cast<InnerNode*>(root), depth);
	printf("TOTAL NODES %d\n", total_nodes);
}
*/
/*
void MemstoreAlexTree::checkConflict(int sig, int mode) {
	if(mode == 1) {
		waccess[current_tid][windex[current_tid] % CONFLICT_BUFFER_LEN] = sig;
		windex[current_tid]++;
		for(int i = 0; i < 4; i++) {
			if(i == current_tid) continue;
			for(int j = 0; j < CONFLICT_BUFFER_LEN; j++)
				if(sig == waccess[i][j]) wconflict++;
			for(int j = 0; j < CONFLICT_BUFFER_LEN; j++)
				if(sig == raccess[i][j]) rconflict++;
		}
	} else {
		raccess[current_tid][rindex[current_tid] % CONFLICT_BUFFER_LEN] = sig;
		rindex[current_tid]++;
		for(int i = 0; i < 4; i++) {
			if(i == current_tid) continue;
			for(int j = 0; j < CONFLICT_BUFFER_LEN; j++)
				if(sig == waccess[i][j]) rconflict++;
		}
	}
}
*/
MemstoreAlexTree::Iterator::Iterator(MemstoreAlexTree* tree) {
	tree_ = tree;
	node_ = NULL;
}

uint64_t* MemstoreAlexTree::Iterator::GetLink() {
	return link_;
}

uint64_t MemstoreAlexTree::Iterator::GetLinkTarget() {
	return target_;
}

// Returns true iff the iterator is positioned at a valid node.
bool MemstoreAlexTree::Iterator::Valid() {
	return (node_ != NULL) && (node_->num_keys > 0);
	//	printf("b %d\n",b);
}

// Advances to the next position.
// REQUIRES: Valid()
bool MemstoreAlexTree::Iterator::Next() {
	//get next different key
	//if (node_->seq != seq_) printf("%d %d\n",node_->seq,seq_);
	bool b = true;
#if BTREE_LOCK
	MutexSpinLock lock(&tree_->slock);
#else
	RTMScope begtx(&tree_->prof, 1, 1, &tree_->rtmlock);
#endif
	if(node_->seq != seq_) { //the current LeafNode is modified, search the LeafNode again
		b = false;
		while(node_ != NULL) {
			int k = 0;
			int num = node_->num_keys;
			while((k < num) && (key_ >= node_->kvs[k].key)) {
				++k;
			}
			if(k == num) {
				node_ = node_->right; //not in the current LeafNode
				if(node_ == NULL) return b;
			} else {
				leaf_index = k; //index of the key in the keylist of the current LeafNode
				break;
			}
		}
	} else {
		leaf_index++; //just move to the nexy key
	}
	if(leaf_index >= node_->num_keys) {
		node_ = node_->right;
		leaf_index = 0;
		if(node_ != NULL) {
			link_ = (uint64_t *)(&node_->seq); //the addr of the current LeafNode
			target_ = node_->seq; //copy the seqno of the current LeafNode
		}
	}
	if(node_ != NULL) {
		key_ = node_->kvs[leaf_index].key;
		value_ = node_->kvs[leaf_index].value;
		seq_ = node_->seq;
	}
	return b;
}

// Advances to the previous position.
// REQUIRES: Valid()
bool MemstoreAlexTree::Iterator::Prev() {
	// Instead of using explicit "prev" links, we just search for the
	// last node that falls before key.
	assert(Valid());

	//FIXME: This function doesn't support link information
	//  printf("PREV\n");
	bool b = true;

#if BTREE_LOCK
	MutexSpinLock lock(&tree_->slock);
#else
	RTMScope begtx(&tree_->prof, 1, 1, &tree_->rtmlock);
#endif

	if(node_->seq != seq_) {
		b = false;
		while(node_ != NULL) {
			int k = 0;
			int num = node_->num_keys;
			while((k < num) && (key_ > node_->kvs[k].key)) {
				++k;
			}
			if(k == num) {
				if(node_->right == NULL) break;
				node_ = node_->right;
			} else {
				leaf_index = k;
				break;
			}
		}
	}
	// printf("id %d\n",leaf_index);
	leaf_index--;
	if(leaf_index < 0) {
		node_ = node_->left;
		//if (node_ != NULL) printf("NOTNULL\n");
		if(node_ != NULL) {
			leaf_index = node_->num_keys - 1;
			link_ = (uint64_t *)(&node_->seq);
			target_ = node_->seq;
		}
	}

	if(node_ != NULL) {
		key_ = node_->kvs[leaf_index].key;
		value_ = node_->kvs[leaf_index].value;
		seq_ = node_->seq;
	}
	return b;
}

uint64_t MemstoreAlexTree::Iterator::Key() {
	return key_;
}

Memstore::MemNode* MemstoreAlexTree::Iterator::CurNode() {
	if(!Valid()) return NULL;
	return value_;
}

// Advance to the first entry with a key >= target
void MemstoreAlexTree::Iterator::Seek(uint64_t key) {
#if BTREE_LOCK
	MutexSpinLock lock(&tree_->slock);
#else
	RTMScope begtx(&tree_->prof, tree_->depth, 1, &tree_->rtmlock);
#endif

	LeafNode *leaf = tree_->FindLeaf(key); //the the LeafNode storing the key
	link_ = (uint64_t *)(&leaf->seq); //Pointer to the current LeafNode
	target_ = leaf->seq;	//copy of the seqno of the current LeafNode
	int num = leaf->num_keys;
	assert(num > 0);
	int k = 0;
	while((k < num) && (leaf->kvs[k].key < key)) {
		++k;
	}
	if(k == num) {
		node_ = leaf->right;
		leaf_index = 0;
	} else {
		leaf_index = k;
		node_ = leaf;
	}
	seq_ = node_->seq;
	key_ = node_->kvs[leaf_index].key;
	value_ = node_->kvs[leaf_index].value;
	//tree_->printLeaf(node_);
}

void MemstoreAlexTree::Iterator::SeekPrev(uint64_t key) {
#if BTREE_LOCK
	MutexSpinLock lock(&tree_->slock);
#else
	RTMScope begtx(&tree_->prof, tree_->depth, 1, &tree_->rtmlock);
#endif

	LeafNode *leaf = tree_->FindLeaf(key);
	link_ = (uint64_t *)(&leaf->seq);
	target_ = leaf->seq;

	int k = 0;
	int num = leaf->num_keys;
	while((k < num) && (key > leaf->kvs[k].key)) {
		++k;
	}
	if(k == 0) {
		node_ = leaf->left;
		link_ = (uint64_t *)(&node_->seq);
		target_ = node_->seq;
		leaf_index = node_->num_keys - 1;
	} else {
		k = k - 1;
		node_ = leaf;
	}
}

// Position at the first entry in list.
// Final state of iterator is Valid() iff list is not empty.
void MemstoreAlexTree::Iterator::SeekToFirst() {
	void* min = tree_->root;
	int d = tree_->depth;
	while(d > 0) {
		min = reinterpret_cast<InnerNode*>(min)->children[0];
		d--;
	}
	node_ = reinterpret_cast<LeafNode*>(min);
	link_ = (uint64_t *)(&node_->seq);
	target_ = node_->seq;
	leaf_index = 0;
#if BTREE_LOCK
	MutexSpinLock lock(&tree_->slock);
#else
	RTMScope begtx(&tree_->prof, 1, 1, &tree_->rtmlock);
#endif
	key_ = node_->kvs[0].key;
	value_ = node_->kvs[0].value;
	seq_ = node_->seq;
}

// Position at the last entry in list.
// Final state of iterator is Valid() iff list is not empty.
void MemstoreAlexTree::Iterator::SeekToLast() {
	//TODO
	assert(0);
}
}

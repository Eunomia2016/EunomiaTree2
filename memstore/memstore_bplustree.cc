#include "memstore/memstore_bplustree.h"

namespace leveldb {
	
	__thread RTMArena* MemstoreBPlusTree::arena_ = NULL;
	__thread bool MemstoreBPlusTree::localinit_ = false;
	__thread Memstore::MemNode *MemstoreBPlusTree::dummyval_ = NULL;
	void MemstoreBPlusTree::printLeaf(LeafNode *n) {
			printf("Leaf Key num %d\n", n->num_keys);
			for (int i=0; i<n->num_keys;i++)
				printf("\t%lx  ",n->keys[i]);
				printf("\n");
			total_key += n->num_keys;
		}
	

	void MemstoreBPlusTree::printInner(InnerNode *n, unsigned depth) {
		printf("Inner %d Key num %d\n", depth, n->num_keys);
		for (int i=0; i<n->num_keys;i++)
			 printf("\t%lx	",n->keys[i]);
		 printf("\n");
		for (int i=0; i<=n->num_keys; i++)
			if (depth>1) printInner(reinterpret_cast<InnerNode*>(n->children[i]), depth-1);
			else printLeaf(reinterpret_cast<LeafNode*>(n->children[i]));
	}

	void MemstoreBPlusTree::PrintStore() {
		 printf("===============B+ Tree=========================\n");
		 total_key = 0;
		 if (depth == 0) printLeaf(reinterpret_cast<LeafNode*>(root));
		 else {
			  printInner(reinterpret_cast<InnerNode*>(root), depth);
		 }
		 printf("========================================\n");
		 printf("Total key num %d\n", total_key);
	} 
	
	/*

	void MemstoreBPlusTree::topLeaf(LeafNode *n) {
		total_nodes++;
		if (n->writes > 40) printf("Leaf %lx , w %ld , r %ld\n", n, n->writes, n->reads);
		
	}

	void MemstoreBPlusTree::topInner(InnerNode *n, unsigned depth){
		total_nodes++;
		if (n->writes > 40) printf("Inner %lx depth %d , w %ld , r %ld\n", n, depth, n->writes, n->reads);
		for (int i=0; i<=n->num_keys;i++)
			if (depth > 1) topInner(reinterpret_cast<InnerNode*>(n->children[i]), depth-1);
			else topLeaf(reinterpret_cast<LeafNode*>(n->children[i]));
	}
	
	void MemstoreBPlusTree::top(){
		if (depth == 0) topLeaf(reinterpret_cast<LeafNode*>(root));
		else topInner(reinterpret_cast<InnerNode*>(root), depth);
		printf("TOTAL NODES %d\n", total_nodes);
	}

	void MemstoreBPlusTree::checkConflict(void *node, int mode) {
		if (mode == 1) {
			waccess[current_tid][windex[current_tid]] = node;
			windex[current_tid]++;
			for (int i= 0; i<4; i++) {
				if (i==current_tid) continue;
				for (int j=0; j<windex[i]; j++)
					if (node == waccess[i][j]) wconflict++;
				for (int j=0; j<rindex[i]; j++)
					if (node == raccess[i][j]) rconflict++;
			}
		}
		else {
			raccess[current_tid][rindex[current_tid]] = node;
			rindex[current_tid]++;
			for (int i= 0; i<4; i++) {
				if (i==current_tid) continue;
				for (int j=0; j<windex[i]; j++)
					if (node == waccess[i][j]) rconflict++;
			}
		}
		
	}*/
		

}


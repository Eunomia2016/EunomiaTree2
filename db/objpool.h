#ifndef OBJPOOL_H
#define OBJPOOL_H

#include <stdint.h>
#include <stdio.h>
#include <assert.h>


class OBJPool {

public:

	struct Obj {
		Obj *next;
		char value[0];
	};
	
	struct Header {
		uint64_t sn;
		Obj* head;
		Obj* tail;
		Header* next;
		int gcnum;

		Header(){
			sn = 0;
			head = NULL;
			tail = NULL;
			next = NULL;
			gcnum = 0;
		}
	};
	
	


private:


	Header* gclists_;
	Header* curlist_;
	
	int freenum_;
	Obj* freelist_;

	
public:

	bool debug;

	OBJPool();
	
	~OBJPool();
	
	void AddGCObj(char* gobj, uint64_t sn);

	char* GetFreeObj();

	void FreeList(Header* list);
	
	void GCList(Header* list);
	void GC(uint64_t safesn);

	void Print();
	
};


#endif

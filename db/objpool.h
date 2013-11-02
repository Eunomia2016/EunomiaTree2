#ifndef OBJPOOL_H
#define OBJPOOL_H

#include <stdint.h>
#include <stdio.h>
#include <assert.h>


class OBJPool {

	struct Obj {
		Obj *next;
		uint64_t* value;
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
	
	void AddGCObj(uint64_t* gobj, uint64_t sn);

	uint64_t* GetFreeObj();

	void FreeList(Header* list);
	
	void GCList(Header* list);
	void GC(uint64_t safesn);

	void Print();
	
};


#endif

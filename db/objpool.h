#ifndef OBJPOOL_H
#define OBJPOOL_H

#include <stdint.h>
#include <stdio.h>
#include <assert.h>


class OBJPool {

	struct Obj {
		Obj *next;
	};


private:

	int gcnum_;
	int freenum_;
	Obj* gclist_;
	Obj* gctail_;
	Obj* freelist_;

	
public:

	bool debug;

	OBJPool();
	
	~OBJPool();
	
	void AddGCObj(uint64_t* gobj);

	uint64_t* GetFreeObj();

	void FreeList();
	void GC();

	void Print();
	
};


#endif

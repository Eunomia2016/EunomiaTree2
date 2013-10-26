#ifndef PBUF_H
#define PBUF_H

#include <stdint.h>
#include <stdio.h>
#include <assert.h>
#include "util/spinlock.h"

#define BSIZE 64*1024 //64KB
#define LOGPATH "log/test.persist"

class LocalPBuf {

	struct PEntry {
		int tableid;
		uint64_t key;
		uint64_t seqno;
		uint64_t* value;
		int vlen;
	};

	uint64_t sn;
	int cur;
	PEntry buf[BSIZE];

public:

	LocalPBuf* next;
		
	LocalPBuf() {
		next = NULL;
		sn = 0;
		cur = 0;
	}

	int EmptySlotNum() {
		return BSIZE - cur;
	}

	uint64_t GetSN() {
		return sn;
	}

	void SetSN(uint64_t s) {
		sn = s;
	}

	void PutRecord(int tabid, uint64_t key, 
		uint64_t seqno, uint64_t* value, int vlen)
	{
		buf[cur].tableid = tabid;
		buf[cur].key = key;
		buf[cur].seqno = seqno;
		buf[cur].value = value;
		buf[cur].vlen = vlen;

		cur++;
	}

	void Reset() {
		
		cur = 0;
		sn = 0;
		next = NULL;
	}
	
};

class PBuf {
	
static __thread int tid_;

LocalPBuf** lbuf;

SpinLock frozenlock;
LocalPBuf* frozenbufs;

SpinLock freelock;
LocalPBuf* freebufs;


public:

	PBuf(int thr);
	
	~PBuf();

	void RegisterThread(int tid);

	void RecordTX(uint64_t sn, int recnum);

	void WriteRecord(int tabid, uint64_t key, 
		uint64_t seqno, uint64_t* value, int vlen);
	
	void FrozeLocalBuffer();

	void Writer();

	void Print();
	
};


#endif

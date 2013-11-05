#ifndef PBUF_H
#define PBUF_H

#include <stdint.h>
#include <stdio.h>
#include <assert.h>
#include <pthread.h>
#include "util/spinlock.h"
#include "log.h"



#define BSIZE 64*1024 //64KB

class LocalPBuf {

	struct PEntry {
		int tableid;
		uint64_t key;
		uint64_t seqno;
		uint64_t* value;
		int vlen;
	};

public:

	uint64_t sn;
	int cur;
	PEntry buf[BSIZE];



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

	void Serialize(Log* lf) {
		
		for(int i = 0; i < cur; i++) {
			lf->writeLog((char *)&buf[i].tableid, sizeof(int));
			lf->writeLog((char *)&buf[i].key, sizeof(uint64_t));
			lf->writeLog((char *)&buf[i].seqno, sizeof(uint64_t));
			
			if(buf[i].value == NULL) {
				uint64_t nulval = -1;
				lf->writeLog((char *)&nulval, sizeof(uint64_t));
			} else {
				lf->writeLog((char *)buf[i].value, buf[i].vlen);
			}
		}
	}
	
};

class PBuf {
	
static __thread int tid_;
static volatile bool sync_;

pthread_t write_id;

char * logpath;

int buflen;
LocalPBuf** lbuf;

SpinLock frozenlock;
LocalPBuf** frozenbufs;
uint64_t* localsn;

SpinLock freelock;
LocalPBuf* freebufs;

Log* logf;

//Only update in the logger thread
volatile uint64_t safe_sn;


public:

	PBuf(int thr);
	
	~PBuf();

	static void* loggerThread(void * arg);
	
	void RegisterThread(int tid);

	void RecordTX(uint64_t sn, int recnum);

	void WriteRecord(int tabid, uint64_t key, 
		uint64_t seqno, uint64_t* value, int vlen);
	
	void FrozeLocalBuffer(int idx);

	void FrozeAllBuffer();

	void Sync();

	void Writer();
		
	void Print();

	uint64_t GetSafeSN() {return safe_sn;};
	
};


#endif

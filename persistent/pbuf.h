#ifndef PBUF_H
#define PBUF_H

#include <stdint.h>
#include <stdio.h>
#include <assert.h>
#include <pthread.h>
#include "util/spinlock.h"
#include "log.h"
#include "lbuf.h"
#include "tbuf.h"


class PBuf {
	
static __thread int tid_;
static volatile bool sync_;

int buflen;
LocalPBuf** lbuf;

//Only update in the logger thread
volatile uint64_t safe_sn;

TBuf* tbufs[2];

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

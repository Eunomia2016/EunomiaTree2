#include "pbuf.h"
#include <assert.h>
#include <stdio.h>
#include <stdlib.h>
#include <pthread.h>
#include "db/dbtx.h"

using namespace leveldb;

__thread int PBuf::tid_;

PBuf::PBuf(int thr)
{
	lbuf = new PBuf*[thr];
	for(int i = 0; i < thr; i++) {
		lbuf[i] = new LocalPBuf();
	}
}

PBuf::~PBuf()
{
	
}

void PBuf::RegisterThread(int tid)
{
	tid_ = tid;
}


void PBuf::RecordTX(uint64_t sn, int recnum)
{
	if(recnum == 0)
		return;
	
	if(lbuf[tid_]->EmptySlotNum() < recnum || lbuf[tid_]->GetSN() != sn) {
		assert(lbuf[tid_]->GetSN() < sn);
		FrozeLocalBuffer();
		lbuf[tid_]->SetSN(sn)
	}

}


void PBuf::WriteRecord(int tabid, uint64_t key, 
						uint64_t seqno, uint64_t* value, int vlen)
{
	lbuf[tid_]->PutRecord(tabid, key, seqno, value, vlen);
}

void PBuf::FrozeLocalBuffer()
{
	frozenlock.Lock();
	lbuf[tid_]->next = frozenbufs;
	frozenbufs = lbuf[tid_];
	frozenlock.Unlock();

	freelock.Lock();
	
	if(freebufs == NULL) {
		lbuf[tid_] = new LocalPBuf();
	} else {
		lbuf[tid_] = freebufs;
		freebufs = freebufs->next;
		lbuf[tid_]->Reset();
	}
	
	freelock.Unlock();

}


void PBuf::Print()
{
	
}




#include "pbuf.h"
#include <assert.h>
#include <stdio.h>
#include <stdlib.h>
#include <pthread.h>
#include <time.h>
#include "db/dbtx.h"

using namespace leveldb;

__thread int PBuf::tid_;

volatile bool PBuf::sync_ = false;

PBuf::PBuf(int thr)
{
	buflen = thr;
	
	lbuf = new LocalPBuf*[thr];
	for(int i = 0; i < thr; i++) {
		lbuf[i] = new LocalPBuf();
	}


	tbufs[0] = new TBuf(thr, "/media/ssd/log1");
	tbufs[1] = new TBuf(thr, "/media/ssd2/log2");
	
	safe_sn = 0;
	
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
		assert(lbuf[tid_]->GetSN() <= sn);
		FrozeLocalBuffer(tid_);
		lbuf[tid_]->SetSN(sn);
	}

}


void PBuf::WriteRecord(int tabid, uint64_t key, 
						uint64_t seqno, uint64_t* value, int vlen)
{	
	lbuf[tid_]->PutRecord(tabid, key, seqno, value, vlen);
}

void PBuf::Sync()
{
	FrozeAllBuffer();
	tbufs[0]->Sync();
	tbufs[1]->Sync();

	tbufs[0]->WaitSyncFinish();
	tbufs[1]->WaitSyncFinish();
}

void PBuf::FrozeAllBuffer()
{
	for(int i = 0; i < buflen; i++) {
		FrozeLocalBuffer(i);
	}
}

void PBuf::FrozeLocalBuffer(int idx)
{
	tbufs[idx%2]->PublishLocalBuffer(idx, lbuf[idx]);

	
	lbuf[idx] = tbufs[0]->GetFreeBuf();

	if(lbuf[idx] == NULL)
		lbuf[idx] = tbufs[1]->GetFreeBuf();
	
	if(lbuf[idx] == NULL)
		lbuf[idx] = new LocalPBuf();
	
	safe_sn = tbufs[0]->GetSafeSN() < tbufs[1]->GetSafeSN()? tbufs[0]->GetSafeSN(): tbufs[1]->GetSafeSN();

}


void PBuf::Print()
{
	
}




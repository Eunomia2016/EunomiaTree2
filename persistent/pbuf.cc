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

// number of nanoseconds in 1 second (1e9)
#define ONE_SECOND_NS 1000000000

//40ms
#define SLEEPEPOCH  ONE_SECOND_NS / 1000 * 40

PBuf::PBuf(int thr)
{
	buflen = thr;
	lbuf = new LocalPBuf*[thr];
	for(int i = 0; i < thr; i++) {
		lbuf[i] = new LocalPBuf();
	}

	logpath = "test.txt";
	
	logf = new Log(logpath, true);
	
	//Create Serialization Thread
	pthread_create(&write_id, NULL, loggerThread, (void *)this);
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
		FrozeLocalBuffer(tid_);
		lbuf[tid_]->SetSN(sn);
	}

}


void PBuf::WriteRecord(int tabid, uint64_t key, 
						uint64_t seqno, uint64_t* value, int vlen)
{
	if(value == NULL) {
// 	  printf("ERROR Zero Value!!! [Write Record table %d key %ld seqno %ld value len %d]\n", tabid, key, seqno, vlen);
	}
	
	lbuf[tid_]->PutRecord(tabid, key, seqno, value, vlen);
}

void PBuf::Sync()
{
	FrozeAllBuffer();
	sync_ = true;
	pthread_join(write_id, NULL);
}

void PBuf::FrozeAllBuffer()
{
	for(int i = 0; i < buflen; i++) {
		FrozeLocalBuffer(i);
	}
}

void PBuf::FrozeLocalBuffer(int idx)
{
	frozenlock.Lock();
	lbuf[idx]->next = frozenbufs;
	frozenbufs = lbuf[idx];
	frozenlock.Unlock();

	freelock.Lock();
	
	if(freebufs == NULL) {
		lbuf[idx] = new LocalPBuf();
	} else {
		lbuf[idx] = freebufs;
		freebufs = freebufs->next;
		lbuf[idx]->Reset();
	}
	
	freelock.Unlock();

}

void* PBuf::loggerThread(void * arg)
{
	PBuf* pb = (PBuf*) arg;

	while(true) {

		if(sync_) {
			pb->Writer();
			break;
		}
		
		struct timespec t;
		t.tv_sec  = SLEEPEPOCH / ONE_SECOND_NS;
     	t.tv_nsec = SLEEPEPOCH % ONE_SECOND_NS;
      	nanosleep(&t, NULL);
	  	
		pb->Writer();
	}
}

void PBuf::Writer()
{

	if(frozenbufs == NULL)
		return;
	
	frozenlock.Lock();
	LocalPBuf* lfbufs = frozenbufs;
	frozenbufs = NULL;
	frozenlock.Unlock();

	LocalPBuf* cur = lfbufs;
	LocalPBuf* tail = cur;
	
	while(cur != NULL) {
		cur->Serialize(logf);
		tail = cur;
		cur =  cur->next;
	}

	
	freelock.Lock();
	assert(tail != NULL && tail->next == NULL);
	tail->next = freebufs;
	freebufs = lfbufs;
	freelock.Unlock();
}

void PBuf::Print()
{
	
}




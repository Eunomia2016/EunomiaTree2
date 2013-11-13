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

	frozenbufs = new LocalPBuf*[thr];
	for(int i = 0; i < thr; i++)
		frozenbufs[i] = NULL;

	logpath = "test.txt";

	localsn = new uint64_t[thr];
	for(int i = 0; i < thr; i++)
		localsn[i] = 1;
	
	safe_sn = 0;
	
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
	lbuf[idx]->next = frozenbufs[idx];
	frozenbufs[idx] = lbuf[idx];
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


uint64_t PBuf::write_time = 0;
inline unsigned long rdtsc(void)
{
	unsigned a, d;
	__asm __volatile("rdtsc":"=a"(a), "=d"(d));
	return ((unsigned long)a) | (((unsigned long) d) << 32);
}

void* PBuf::loggerThread(void * arg)
{
#if 0	
	   cpu_set_t  mask;
	  CPU_ZERO(&mask);
	  CPU_SET(7 , &mask);
	sched_setaffinity(0, sizeof(mask), &mask);
#endif
	PBuf* pb = (PBuf*) arg;

	while(true) {

		if(sync_) {
	//		uint64_t ss = rdtsc();
			pb->Writer();
	//		write_time += (rdtsc()-ss);
			break;
		}
		
		struct timespec t;
		t.tv_sec  = SLEEPEPOCH / ONE_SECOND_NS;
     	t.tv_nsec = SLEEPEPOCH % ONE_SECOND_NS;
      	nanosleep(&t, NULL);
	//  	
		pb->Writer();
	//	write_time += (rdtsc()-s);
	
	}
}

void PBuf::Writer()
{


	for(int i = 0; i < buflen; i++) {

		if(frozenbufs[i] == NULL|| frozenbufs[i]->cur == 0)
			continue;
	
		frozenlock.Lock();
		LocalPBuf* lfbufs = frozenbufs[i];
		frozenbufs[i] = NULL;
		frozenlock.Unlock();

		LocalPBuf* cur = lfbufs;
		LocalPBuf* tail = cur;

		uint64_t cursn = cur->GetSN();

		localsn[i] = cursn;

		assert(cursn >= (safe_sn + 1));
		
		while(cur != NULL) {
			
			assert(cursn >= cur->GetSN());
		//	uint64_t s = rdtsc();
			cur->Serialize(logf);
		//	write_time += (rdtsc()-s);
			tail = cur;
			cur =  cur->next;
		}

	
		freelock.Lock();
		assert(tail != NULL && tail->next == NULL);
		tail->next = freebufs;
		freebufs = lfbufs;
		freelock.Unlock();
	}

	uint64_t minsn = 0;
	for(int i = 0; i < buflen; i++) {
		if(i == 0 || minsn > localsn[i])
			minsn = localsn[i];
	}
		

	assert(minsn >= (safe_sn + 1));
		
	safe_sn = minsn - 1;
}

void PBuf::Print()
{
	
}




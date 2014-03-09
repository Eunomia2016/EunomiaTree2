#include "tbuf.h"
#include <assert.h>
#include <stdio.h>
#include <stdlib.h>
#include <pthread.h>
#include <time.h>
#include <unistd.h>



volatile bool TBuf::sync_ = false;

TBuf::TBuf(int thr, char* lpath)
{

	buflen = thr;

	frozenbufs = new LocalPBuf*[thr];
	for(int i = 0; i < thr; i++)
		frozenbufs[i] = NULL;

	logpath = lpath;
	logf = new Log(logpath, true);
	
	localsn = new uint64_t[thr];
	for(int i = 0; i < thr; i++)
		localsn[i] = 1;
	
	safe_sn = 0;
	
	//Create Serialization Thread
	pthread_create(&write_id, NULL, loggerThread, (void *)this);
}

TBuf::~TBuf()
{
	
}


void TBuf::Sync()
{
	sync_ = true;
	pthread_join(write_id, NULL);
}


void TBuf::PublishLocalBuffer(int tid, LocalPBuf* lbuf)
{
	frozenlock.Lock();
	lbuf->next = frozenbufs[tid];
	frozenbufs[tid] = lbuf;
	frozenlock.Unlock();

}

void* TBuf::loggerThread(void * arg)
{
#if 0	
	   cpu_set_t  mask;
	  CPU_ZERO(&mask);
	  CPU_SET(7 , &mask);
	sched_setaffinity(0, sizeof(mask), &mask);
#endif

	TBuf* tb = (TBuf*) arg;

	while(true) {

		if(sync_) {
	//		uint64_t ss = rdtsc();
			tb->Writer();
	//		write_time += (rdtsc()-ss);
			break;
		}
		  	
		tb->Writer();
	
	}
}

void TBuf::Writer()
{


	for(int i = 0; i < buflen; i++) {

		if(frozenbufs[i] == NULL|| frozenbufs[i]->cur == 0)
			continue;
	
		frozenlock.Lock();
		LocalPBuf* lfbufs = frozenbufs[i];
		frozenbufs[i] = NULL;
		frozenlock.Unlock();

		LocalPBuf* cur = lfbufs;
		LocalPBuf* next = NULL;
		LocalPBuf* tail = cur;

		uint64_t cursn = cur->GetSN();

		localsn[i] = cursn;

		assert(cursn >= (safe_sn + 1));
		
		while(cur != NULL) {
			
			assert(cursn >= cur->GetSN());

			cur->Serialize(logf);
			next = cur->next;
			delete cur;
			
			tail = cur;
			cur =  next;
		}
	}

	fdatasync(logf->fd);
	
	uint64_t minsn = 0;
	for(int i = 0; i < buflen; i++) {
		if(i == 0 || minsn > localsn[i])
			minsn = localsn[i];
	}
		

	assert(minsn >= (safe_sn + 1));
		
	safe_sn = minsn - 1;
}

void TBuf::Print()
{
	
}




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

	freebufs = NULL;
		
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

}

void TBuf::WaitSyncFinish()
{
	pthread_join(write_id, NULL);
}


void TBuf::PublishLocalBuffer(int tid, LocalPBuf* lbuf)
{
	frozenlock.Lock();
	lbuf->next = frozenbufs[tid];
	frozenbufs[tid] = lbuf;
	frozenlock.Unlock();

}

LocalPBuf* TBuf::GetFreeBuf()
{
	
	freelock.Lock();
	LocalPBuf* lbuf = freebufs;
	if(lbuf != NULL) 
		freebufs = freebufs->next;
	freelock.Unlock();
	
	if(lbuf != NULL) 
		lbuf->Reset();
	
	return lbuf;

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
#if 0		  	
		struct timespec t;
		t.tv_sec  = 0;
		t.tv_nsec = 1000;
		nanosleep(&t, NULL);
#endif		
		tb->Writer();
	
	}
}

void TBuf::Writer()
{


	int flushbytes = 0;
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

			flushbytes += cur->Serialize(logf);
			
			tail = cur;
			cur =  cur->next;
		}

		freelock.Lock();
		tail->next = freebufs;
		freebufs = lfbufs;
		freelock.Unlock();
	
	}

//	printf("Flush bytes %d\n", flushbytes);
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




// Copyright (c) 2011 The LevelDB Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file. See the AUTHORS file for names of contributors.

#ifndef STORAGE_LEVELDB_UTIL_RTM_H_
#define STORAGE_LEVELDB_UTIL_RTM_H_
#include <immintrin.h>
#include <sys/time.h>
#include "txprofile.h"
#include "spinlock.h"
#include "rtm_arena.h"



namespace leveldb {

#define LOCKELISION 0
#define MAXRETRY 10

class RTMArenaScope {
	
 RTMProfile localprofile;
 RTMProfile* globalprof;
 SpinLock* slock;
 RTMArena* arena_;
 int retry;
 int capacity;
 int conflict;
 int explict;
 int lockretry;
 int zero;
 	
 public:
  inline RTMArenaScope(SpinLock* sl, RTMProfile* prof, RTMArena* arena) {

	arena_ = arena;
	
  	globalprof = prof;
	retry = 0;
	capacity = 0;
	conflict = 0;
	lockretry = 0;
	explict = 0;
	zero = 0;
	slock = sl;

	while(true) {
	    unsigned stat;
	 	stat = _xbegin ();
		if(stat == _XBEGIN_STARTED) {
			//if(!slock->Islocked())
				return;
			
			//_xabort(0xff);
			
		}
		
		retry++; 
		if((stat & _XABORT_CONFLICT) != 0) 
		  conflict++;
	    else if((stat & _XABORT_CAPACITY) != 0)
	      capacity++;
		else if((stat & _XABORT_EXPLICIT) && _XABORT_CODE(stat)==0xf0) {
		 // printf("allocation\n");	
		  explict++;
		  arena_->AllocateFallback();
		  
	    }
		else if (stat == 0)
			zero++;
		
#if LOCKELISION
		else if((stat & _XABORT_EXPLICIT) && _XABORT_CODE(stat)==0xff)
		{
			while(slock->Islocked()) 
				_mm_pause();
			lockretry++;
		}

		if(zero > 2)
			break;
		/*
		if(capacity > 4)
			break;
		if(conflict > 8)
			break;
		if(retry > 32)
			break;
			*/
#endif

#if 0		
			localprofile.localRecordAbortStatus(stat);
				if(retry > 100000000){
					localprofile.reportAbortStatus();				
					printf("stat %d\n",stat);
					exit(1);
					retry = 0;
				}
#endif


	}
//    printf("Hold Lock\n");

	slock->Lock();

  }

  void Abort() {
  	_xabort(0x1);
  }

  inline  ~RTMArenaScope() {  

#if LOCKELISION

    if(slock->Islocked())
		slock->Unlock();
	else
#endif
		_xend();

#if 1	
	//access the global profile info outside the transaction scope
	if(globalprof != NULL) {
		RTMProfile::atomic_inc32(&globalprof->succCounts);
	    RTMProfile::atomic_add32(&globalprof->abortCounts, retry);
		RTMProfile::atomic_add32(&globalprof->status[XABORT_CONFLICT_INDEX], conflict);
		RTMProfile::atomic_add32(&globalprof->status[XABORT_CAPACITY_INDEX], capacity);
		RTMProfile::atomic_add32(&globalprof->status[XABORT_EXPLICIT_INDEX], explict);
		RTMProfile::atomic_add32(&globalprof->status[XABORT_DEBUG_INDEX], zero);
		//RTMProfile::atomic_add32(&globalprof->status[XABORT_DEBUG_INDEX], lockretry);
		
		//globalprof->MergeLocalStatus(localprofile);

	}
#endif

  }

};

}  // namespace leveldb


#endif  // STORAGE_LEVELDB_UTIL_RTM_H_

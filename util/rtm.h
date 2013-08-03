// Copyright (c) 2011 The LevelDB Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file. See the AUTHORS file for names of contributors.

#ifndef STORAGE_LEVELDB_UTIL_RTM_H_
#define STORAGE_LEVELDB_UTIL_RTM_H_
#include <immintrin.h>
#include <sys/time.h>
#include "util/spinlock.h"
#include "txprofile.h"


namespace leveldb {

#define MAXCAPACITY 10
#define MAXCONFLICT 100
#define RTMPROFILE 1

class RTMScope {
	
 RTMProfile localprofile;
 RTMProfile* globalprof;
 int retry;
 int conflict;
 int capacity;
 uint64_t befcommit;
 uint64_t aftcommit;

 static SpinLock fblock;

 public:
  inline RTMScope(RTMProfile* prof) {
  	globalprof = prof;
	retry = 0;
	conflict = 0;
	capacity = 0;
	
	while(true) {
	    unsigned stat;
	 	stat = _xbegin();
		if(stat == _XBEGIN_STARTED) {

		  //Put the global lock into read set
		  if(fblock.IsLocked())
		    _xabort(0xff);
		  
		  return;
			
		} else {
		
		  retry++;

		  if((stat & _XABORT_CONFLICT) != 0) 
		  	conflict++;
		  else if((stat & _XABORT_CAPACITY) != 0)
			capacity++;

		  if((stat & _XABORT_EXPLICIT) && _XABORT_CODE(stat) == 0xff) {
			 while(fblock.IsLocked())
			 	_mm_pause();
		  }
		  if(capacity > MAXCAPACITY) {
//		  	printf("hold lock MAXCAPACITY\n");
		  	break;
		  }
		  else if (conflict > MAXCONFLICT) {  	
	//	  	printf("hold lock MAXCONFLICT\n");
		  	break;
		  }
		}
	}
	//printf("hold lock\n");
	fblock.Lock();
	
  }

  void Abort() {
  	
  	  _xabort(0x1);
	
  }
inline  ~RTMScope() {  
  	if(fblock.IsLocked())
	  fblock.Unlock();
	else
	  _xend ();
	//access the global profile info outside the transaction scope
#if RTMPROFILE
	if(globalprof != NULL) {
		globalprof->abortCounts += retry;
		globalprof->capacityCounts += capacity;
		globalprof->conflictCounts += conflict;
	}
#endif
//		globalprof->MergeLocalStatus(localprofile);

  }

 private:
  RTMScope(const RTMScope&);
  void operator=(const RTMScope&);
};

}  // namespace leveldb


#endif  // STORAGE_LEVELDB_UTIL_RTM_H_

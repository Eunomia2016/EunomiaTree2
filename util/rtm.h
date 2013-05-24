// Copyright (c) 2011 The LevelDB Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file. See the AUTHORS file for names of contributors.

#ifndef STORAGE_LEVELDB_UTIL_RTM_H_
#define STORAGE_LEVELDB_UTIL_RTM_H_
#include <immintrin.h>
#include <sys/time.h>
#include "txprofile.h"

namespace leveldb {

class RTMScope {
	
 RTMProfile localprofile;
 RTMProfile* globalprof;
 int retry;

 int max ;
 int rcur;
 int wcur;
 uint64_t readaddr[100];
 uint64_t writeaddr[100];

 public:
  RTMScope(RTMProfile* prof) {
  	globalprof = prof;
	retry = 0;
	max = 1000;
	rcur = 0;
	wcur = 0;
	while(true) {
	    unsigned stat;
	 	stat = _xbegin ();
		if(stat == _XBEGIN_STARTED) {
			return;
			
		} else {

			
			retry++; 
			/*
			if(retry >1000 & retry <= 1100) {
				pthread_yield();
			} if(retry > 1100) {
				retry = 0;
				struct timespec sinterval;
				sinterval.tv_sec = 0;
    			sinterval.tv_nsec = 10;  // 100,000 ns = 100u
    			nanosleep(&sinterval, NULL);
			}
			*/
			
			
			localprofile.localRecordAbortStatus(stat);
			if(retry > 10000){
				localprofile.reportAbortStatus();
				int i = 0;
				for(i = 0; i < rcur; i++) {
					printf("Read addr %lx\n", readaddr[rcur]);
				}
				for(i = 0; i < wcur; i++) {
					printf("Write addr %lx\n", writeaddr[wcur]);
				}
				
				exit(1);
				retry = 0;
			}
			
			continue;
		}
	}
  }

  void addRead(uint64_t addr)
  {
  	//assert(rcur < max);
	readaddr[rcur++] = addr;	
  }

  void addWrite(uint64_t addr)
  {
	  //assert(wcur < max);
	  writeaddr[wcur++] = addr;

  }
  void Abort() {
  	_xabort(0x1);
  }
  ~RTMScope() {  

	_xend ();
	//access the global profile info outside the transaction scope
	if(globalprof != NULL)
		globalprof->MergeLocalStatus(localprofile);

  }

 private:
  RTMScope(const RTMScope&);
  void operator=(const RTMScope&);
};

}  // namespace leveldb


#endif  // STORAGE_LEVELDB_UTIL_RTM_H_

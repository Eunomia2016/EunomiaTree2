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
	
// RTMProfile localprofile;
 RTMProfile* globalprof;
 int retry;
 uint64_t befcommit;
 uint64_t aftcommit;

 public:
  inline RTMScope(RTMProfile* prof) {
  	globalprof = prof;
	retry = 0;
	while(true) {
	    unsigned stat;
	 	stat = _xbegin ();
		if(stat == _XBEGIN_STARTED) {
			return;
			
		} else {			
			retry++; 
/*
			localprofile.localRecordAbortStatus(stat);
			if(retry > 100000){
				localprofile.reportAbortStatus();	
				exit(1);
				retry = 0;
			}
	*/		
			continue;
		}
	}
  }

  void Abort() {
  	_xabort(0x1);
  }
inline  ~RTMScope() {  

	_xend ();
	//access the global profile info outside the transaction scope
	if(globalprof != NULL) {
		globalprof->abortCounts += retry;
	}
//		globalprof->MergeLocalStatus(localprofile);

  }

 private:
  RTMScope(const RTMScope&);
  void operator=(const RTMScope&);
};

}  // namespace leveldb


#endif  // STORAGE_LEVELDB_UTIL_RTM_H_

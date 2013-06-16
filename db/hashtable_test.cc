// Copyright (c) 2011 The LevelDB Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file. See the AUTHORS file for names of contributors.
#include "db/hashtable_template.h"
#include "db/dbtransaction_template.h"
#include "db/txmemstore_template.h"

#include "leveldb/env.h"
#include "port/port.h"
#include "util/mutexlock.h"
#include "util/rwlock.h"



static int FLAGS_txs = 100;
static int FLAGS_threads = 4;
static const char* FLAGS_benchmarks =
	"equal,"
	"counter,"
	"nocycle,"
	"consistency";

namespace leveldb {

typedef uint64_t Key;

class KeyComparator : public leveldb::Comparator {
    public:
	int operator()(const Key& a, const Key& b) const {
		if (a < b) {
	      return -1;
	    } else if (a > b) {
	      return +1;
	    } else {
	      return 0;
	    }
	}

	virtual int Compare(const Slice& a, const Slice& b) const {
		assert(0);
		return 0;
	}

	virtual const char* Name()  const {
		assert(0);
		return 0;
	};

   virtual void FindShortestSeparator(
      std::string* start,
      const Slice& limit)  const {
		assert(0);

	}
  
   virtual void FindShortSuccessor(std::string* key)  const {
		assert(0);

	}

};

class KeyHash : public leveldb::HashFunction  {

    public:

	uint64_t MurmurHash64A ( const void * key, int len, unsigned int seed )	{

		const uint64_t m = 0xc6a4a7935bd1e995;
		const int r = 47;
		uint64_t h = seed ^ (len * m);
		const uint64_t * data = (const uint64_t *)key;
		const uint64_t * end = data + (len/8);

		while(data != end)	{
			uint64_t k = *data++;
			k *= m; 
			k ^= k >> r; 
			k *= m; 	
			h ^= k;
			h *= m; 
		}

		const unsigned char * data2 = (const unsigned char*)data;

		switch(len & 7)	{
  		  case 7: h ^= uint64_t(data2[6]) << 48;
		  case 6: h ^= uint64_t(data2[5]) << 40;
		  case 5: h ^= uint64_t(data2[4]) << 32;
		  case 4: h ^= uint64_t(data2[3]) << 24;
		  case 3: h ^= uint64_t(data2[2]) << 16;
		  case 2: h ^= uint64_t(data2[1]) << 8;
		  case 1: h ^= uint64_t(data2[0]);
		  		  h *= m;
		};

		h ^= h >> r;
		h *= m;
		h ^= h >> r;	

		return h;
	} 

	virtual uint64_t hash(uint64_t& key)	{
		return key;
//		return MurmurHash64A((void *)&key, 8, 0);
	}
};


struct SharedState {
  port::Mutex mu;
  port::CondVar cv;
  int total;

  volatile double start_time;
  volatile double end_time;
	  	
  int num_initialized;
  int num_done;
  bool start;
  bool fail;
	
  SharedState() : cv(&mu) { }
};

struct ThreadState {
	int tid;
	SharedState *shared;
	ThreadState(int index)
      : tid(index)
    {
    }
};

class Benchmark {
  private:
	static int counter;
	static RWLock rwlock;
	
  public:
	Benchmark(){
		counter = 0;
	}
	struct ThreadArg {
		ThreadState *thread;
	};
  
	  
	static void CounterTest(void* v) {
		ThreadArg* arg = reinterpret_cast<ThreadArg*>(v);
		int tid = (arg->thread)->tid;
		SharedState *shared = arg->thread->shared;
		
		printf("start %d\n",tid);
		int lcounter = 0;
		for (int i=tid*FLAGS_txs; i< (tid+1)*FLAGS_txs; i++ ) {
			
			//if(tid == 0) {
				rwlock.StartWrite();
				counter++;
				rwlock.EndWrite();
			//} else {
				rwlock.StartRead();
				lcounter += counter;
				rwlock.EndRead();
			//}
		}
		{
		  MutexLock l(&shared->mu);
		  shared->num_done++;
		  if (shared->num_done >= shared->total) {
			shared->cv.SignalAll();
		  }
		}
		//printf("end %d\n",tid);
	}
	  
	void Run(void (*method)(void* arg), Slice name ) {
		int num = FLAGS_threads;
		printf("%s start\n", name.ToString().c_str());				 		 

		SharedState shared;
		shared.total = num;
		shared.num_initialized = 0;
		shared.start_time = 0;
		shared.end_time = 0;
		shared.num_done = 0;
		shared.start = false;
		shared.fail = false;
		 ThreadArg* arg = new ThreadArg[num];
		 for (int i = 0; i < num; i++) {	 	
		 	arg[i].thread = new ThreadState(i);
			arg[i].thread->shared = &shared;
			//printf("Out tid %lx\n", &arg[i]);
			Env::Default()->StartThread(method, &arg[i]);
			
		 }

		 shared.mu.Lock();
		 while (shared.num_done < num) {
		  shared.cv.Wait();
		 }
		 shared.mu.Unlock();
		 printf("all done %d\n", counter);
 		  
	}
};

int Benchmark::counter = 0;
RWLock Benchmark::rwlock;

}// end namespace leveldb

int main(int argc, char**argv)
{
	for (int i = 1; i < argc; i++) {

		 int n;
		 char junk;
	     if (sscanf(argv[i], "--threads=%d%c", &n, &junk) == 1) {
		   FLAGS_threads = n;
	 	 } else if (sscanf(argv[i], "--num=%d%c", &n, &junk) == 1) {
		   FLAGS_txs = n;
	 	 }
		 
 	}

	const char* benchmarks = FLAGS_benchmarks;
	void (* method)(void* arg) = NULL;
	leveldb::KeyHash kh;
  	leveldb::KeyComparator cmp;
	method = leveldb::Benchmark::CounterTest;
   
     
	 leveldb::Benchmark *benchmark = new leveldb::Benchmark();
  	 benchmark->Run(method, "test");
   
	

	
  	return 0;
}
	
	


// Copyright (c) 2011 The LevelDB Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file. See the AUTHORS file for names of contributors.

#include "db/txskiplist.h"
#include "db/hashtable.h"
#include "db/dbtransaction.h"
#include "util/mutexlock.h"
#include <set>
#include "leveldb/env.h"
#include "util/arena.h"
#include "util/hash.h"
#include "util/random.h"
#include <deque>
#include <set>
#include "port/port.h"
#include <iostream>
#include "leveldb/comparator.h"
#include "dbformat.h"
#include <vector>



static const char* FLAGS_benchmarks ="random";

static int FLAGS_num = 200;
static int FLAGS_threads = 2;
static int FLAGS_rdnum = 2;
static int FLAGS_wtnum = 2;



namespace leveldb {
	
typedef uint64_t Key;

__inline__ int64_t XADD64(int64_t* addr, int64_t val) {
    asm volatile(
        "lock;xaddq %0, %1"
        : "+a"(val), "+m"(*addr)
        :
        : "cc");

    return val;
}


class Benchmark {


private:
	//leveldb::ScalaSkipList ssl;
	class KeyComparator : public leveldb::Comparator {
    public:
   
	int operator()(const uint64_t& a, const uint64_t& b) const {
		if (a < b) {
	      return -1;
	    } else if (a > b) {
	      return +1;
	    } else {
	      return 0;
	    }
	}

	virtual int Compare(const Slice& a, const Slice& b) const {
		const uint64_t anum = DecodeFixed64(a.data());
    	const uint64_t bnum = DecodeFixed64(b.data());
		if (anum < anum) {
	      return -1;
	    } else if (anum > anum) {
	      return +1;
	    } else {
	      return 0;
	    }
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

   int64_t total_count;
   int64_t read_count;
   int64_t write_count;
   
   KeyComparator comparator;

   HashTable seqs;
   TXSkiplist* store;
   port::Mutex mutex;
	
private:
	
	static uint64_t getkey(Key key) { return (key >> 40); }
	static uint64_t gen(Key key) { return (key >> 8) & 0xffffffffu; }
	static uint64_t hash(Key key) { return key & 0xff; }

	static uint64_t HashNumbers(uint64_t k, uint64_t g) {
		uint64_t data[2] = { k, g };
		return Hash(reinterpret_cast<char*>(data), sizeof(data), 0);
	}

	static Key MakeKey(uint64_t k, uint64_t g) {
		assert(sizeof(Key) == sizeof(uint64_t));
		assert(g <= 0xffffffffu);
		//return ((k << 40) | (g << 8) | (HashNumbers(k, g) & 0xff));
		//return ((k << 40) | (g));
		return (g << 16 | k);
	}
		
	struct SharedState {
	  port::Mutex mu;
	  port::CondVar cv;
	  int total;

	  volatile double start_time;
	  volatile double end_time;
	  	
	  int num_initialized;
	  int num_done;
	  bool start;

	
	  SharedState() : cv(&mu) { }
	};
	
	// Per-thread state for concurrent executions of the same benchmark.
	struct ThreadState {
	  int tid;			   // 0..n-1 when running in n threads
	  SharedState* shared;
	  int count;
	  int falseConflict;
	  int conflict;
	  double time;
	  Random rnd;         // Has different seeds for different threads

	  ThreadState(int index)
	      : tid(index),
	        rnd(1000 + index) {
	 		falseConflict = 0;
			conflict = 0;
	  }
	  
	};

	  struct ThreadArg {
		Benchmark* bm;
		SharedState* shared;
		ThreadState* thread;
		void (Benchmark::*method)(ThreadState*);
	  };
	
	  static void ThreadBody(void* v) {
		ThreadArg* arg = reinterpret_cast<ThreadArg*>(v);
		SharedState* shared = arg->shared;
		ThreadState* thread = arg->thread;
		{
		  MutexLock l(&shared->mu);
		 
		  shared->num_initialized++;
		  if (shared->num_initialized >= shared->total) {
			shared->cv.SignalAll();
		  }
		  while (!shared->start) {
			shared->cv.Wait();
		  }
		}

		double start = leveldb::Env::Default()->NowMicros();
		if(shared->start_time == 0)
			shared->start_time = start;

		(arg->bm->*(arg->method))(thread);
		//std::cout << thread->tid << std::endl;

		double end = leveldb::Env::Default()->NowMicros();
		shared->end_time = end;
		thread->time = end - start;

		
		{
		  MutexLock l(&shared->mu);
		  shared->num_done++;
		  if (shared->num_done >= shared->total) {
			shared->cv.SignalAll();
		  }
		}
	  }

	   void WriteRandom(ThreadState* thread) {
 	   	  DoWrite(thread, false);
	   }

	   void WriteSeq(ThreadState* thread) {
 	   	  DoWrite(thread, true);
	   }

	   
	   void DoWrite(ThreadState* thread, bool seq) {
	   
		   int tid = thread->tid;
		   int seqNum = 0;
		   int rnum = read_count;
		   int wnum = write_count;
		   
		   //printf("DoWrite %d\n", total_count);
			while(total_count > 0) {
				
				int64_t oldv = XADD64(&total_count, -1000);
				if(oldv <= 0)
					   break;

				
				for (int i =0; i < 1000; i++) {		   
				/*	Key k;
					if(seq)
						k = MakeKey(tid,seqNum++);
					else
						k = MakeKey(tid, thread->rnd.Next());*/
				//	printf("Exec %d\n", i+1);
	
					DBTransaction tx(&seqs, store, &mutex);
					int conflict = 0;
					
					ValueType t = kTypeValue;
					char* kc = new char[100];
					char* vc = new char[100];
					
					leveldb::Status s;
					std::string str;
					bool done = false;
					
					while( !done ) {
						tx.Begin();
						//first write tuples
						for(int i = 0; i < wnum; i++) {
							snprintf(kc, sizeof(kc), "%d", thread->rnd.Next());
							leveldb::Slice k(kc);
							tx.Add(t, k, k);
						}

						for(int i = 0; i < rnum; i++) {
							snprintf(kc, sizeof(kc), "%d", thread->rnd.Next());
							leveldb::Slice k(kc);
							tx.Get(k, &str, &s);
						}
						
						done = tx.End();
						
						if( !done )
							conflict++;
						//delete vc;
						
					}
					
					thread->conflict += conflict;
					thread->falseConflict += tx.rtmProf.abortCounts;
					
					delete kc;
					delete vc;
				}		
			}
	
		}


	   void ReadRandom(ThreadState* thread) {
 	   	  DoRead(thread, false);
	   }

	   void ReadSeq(ThreadState* thread) {
 	   	  DoRead(thread, true);
	   }

	   
	   void DoRead(ThreadState* thread, bool seq) {
	   
		   int tid = thread->tid;
		   int seqNum = 0;
		   
		   while(total_count > 0) {

			   uint64_t oldv = XADD64(&total_count, -1000);
			   
			   if(oldv <= 0)
			   	   break;
			   
			   for (int i =0; i < 1000; i++) {
	    		   
				   Key k;
				   if(seq)
				   	 k = MakeKey(tid,seqNum++);
				   else
				   	 k = MakeKey(tid, thread->rnd.Next());

			   }
		   }
		 }

	public:

	  Benchmark(): total_count(FLAGS_num), read_count(FLAGS_rdnum), write_count(FLAGS_wtnum)
	  {
		leveldb::Options options;
		leveldb::InternalKeyComparator cmp(options.comparator);
	
		store = new leveldb::TXSkiplist(cmp);
	  }
	  
	  ~Benchmark() {delete store;}
	  
	  void RunBenchmark(int n,
						void (Benchmark::*method)(ThreadState*)) {
		SharedState shared;
		shared.total = n;
		shared.num_initialized = 0;
		shared.start_time = 0;
		shared.end_time = 0;
		shared.num_done = 0;
		shared.start = false;
	
//		double start = leveldb::Env::Default()->NowMicros();
		
		ThreadArg* arg = new ThreadArg[n];
		for (int i = 0; i < n; i++) {
		  arg[i].bm = this;
		  arg[i].method = method;
		  arg[i].shared = &shared;
		  arg[i].thread = new ThreadState(i);
		  arg[i].thread->shared = &shared;
		  arg[i].thread->time = 0;
		  Env::Default()->StartThread(ThreadBody, &arg[i]);
		}
	
		shared.mu.Lock();
		while (shared.num_initialized < n) {
		  shared.cv.Wait();
		}
	
		shared.start = true;
		printf("Send Start Signal\n");
		shared.cv.SignalAll();
//		std::cout << "Startup Time : " << (leveldb::Env::Default()->NowMicros() - start)/1000 << " ms" << std::endl;
		
		while (shared.num_done < n) {
		  shared.cv.Wait();
		}
		shared.mu.Unlock();
		/*
		uint64_t seq;
		ValueType t = kTypeValue;
		char* key = new char[100];
		snprintf(key, sizeof(key), "%d", 1);
		leveldb::Slice k(key);

		seqs.Lookup(k, (void **)&seq);
		printf("lastest seq num %d\n", seq);

		LookupKey lkey(k, seq);
		std::string val;
		Status s;
		store->Get(lkey, &val, &s);
		printf("Value %s\n", val.c_str());
		*/

		printf(" ...... Iterate  MemStore ......\n");
		leveldb::Iterator* iter = store->NewIterator();
		iter->SeekToFirst();
		int count = 0;
	
		while(iter->Valid()) {		
			count++;
		//	printf("Key: %s  ", iter->key());
		//	printf("Value: %s \n", iter->value());
			iter->Next();
		}

		printf("MemStore Total %d\n", count);

		//printf(" ...... Iterate  Seq Hash Table ......");
		//seqs.PrintHashTable();
		
		/*
		if(method == &Benchmark::WriteRandom || method == &Benchmark::WriteSeq)
			printf("Total Write Run Time : %lf ms\n", (shared.end_time - shared.start_time)/1000);
		else
			printf("Total Read Run Time : %lf ms\n", (shared.end_time - shared.start_time)/1000);
		*/
		/*
		for (int i = 0; i < n; i++) {
		  //printf("Thread[%d] Run Time %lf ms\n", i, arg[i].thread->time/1000);
		}*/

		int conflict = 0;
		int falseConflict = 0;
		for (int i = 0; i < n; i++) {
		 	conflict += arg[i].thread->conflict;
			falseConflict += arg[i].thread->falseConflict;
		}

		printf("Conflict %d FalseConflict %d\n", conflict, falseConflict);
		
		for (int i = 0; i < n; i++) {
		  delete arg[i].thread;
		}
		delete[] arg;
	  }


	void Run(){

	  int num_threads = FLAGS_threads;  
	  
      void (Benchmark::*wmethod)(ThreadState*) = NULL;
	  void (Benchmark::*rmethod)(ThreadState*) = NULL;

	  Slice name = FLAGS_benchmarks;
	  if (name == Slice("seq")) {
        wmethod = &Benchmark::WriteSeq;
		rmethod = &Benchmark::ReadSeq;
      } else if (name == Slice("random")) {
        wmethod = &Benchmark::WriteRandom;
		rmethod = &Benchmark::ReadRandom;
      } else {
		std::cout << "Wrong benchmake name " << name.ToString() << std::endl; 
		return;
	  }

//	  double start = leveldb::Env::Default()->NowMicros();
	  total_count = FLAGS_num;
      RunBenchmark(num_threads, wmethod);
	 // total_count = FLAGS_num;
     // RunBenchmark(num_threads, num_, rmethod);
	  
 //     std::cout << "Total Time : " << (leveldb::Env::Default()->NowMicros() - start)/1000 << " ms" << std::endl;
	  
    }
  
};

}  // namespace leveldb



int main(int argc, char** argv) {

 for (int i = 1; i < argc; i++) {

	 int n;
	 char junk;
	 
	 if (leveldb::Slice(argv[i]).starts_with("--benchmarks=")) {
	   FLAGS_benchmarks = argv[i] + strlen("--benchmarks=");
	 } else if (sscanf(argv[i], "--num=%d%c", &n, &junk) == 1) {
	   FLAGS_num = n;
	 } else if (sscanf(argv[i], "--threads=%d%c", &n, &junk) == 1) {
	   FLAGS_threads = n;
	 } else if (sscanf(argv[i], "--read=%d%c", &n, &junk) == 1) {
	   FLAGS_rdnum= n;
	 } else if (sscanf(argv[i], "--write=%d%c", &n, &junk) == 1) {
	   FLAGS_wtnum = n;
	 }
 }

	
  leveldb::Benchmark benchmark;
  benchmark.Run();
  
  return 1;
}

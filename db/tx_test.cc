// Copyright (c) 2011 The LevelDB Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file. See the AUTHORS file for names of contributors.

#include "db/dbtransaction.h"
#include "leveldb/env.h"
#include "port/port.h"
#include "util/mutexlock.h"

static int FLAGS_threads = 1;

namespace leveldb {
/*
class Transaction {
  public:
    int num;
    DBTransaction tx;
    unsigned long bitmap;
    double start_;
    double finish_;
    Transaction(int n, DBTransaction t){
	num = n;
	tx = t;
	bitmap = 0;
    }   
}*/

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
	HashTable *seqs;
	MemTable *store;
	port::Mutex *mutex;
  public:
	Benchmark(HashTable *t, MemTable *s , port::Mutex *m) {
		seqs = t;
		store = s;
		mutex = m;
	}
	struct ThreadArg {
		ThreadState *thread;
		HashTable *seqs;
		MemTable *store;
		port::Mutex *mutex;
		
	};
	static void ThreadBody(void* v) {
		ThreadArg* arg = reinterpret_cast<ThreadArg*>(v);
		int tid = (arg->thread)->tid;
		SharedState *shared = arg->thread->shared;
		HashTable *seqs = arg->seqs;
		MemTable *store = arg->store;
		port::Mutex *mutex = arg->mutex;

		ValueType t = kTypeValue;
		std::string *str  = new std::string[3];

		//printf("In tid %lx\n", arg);
		printf("start %d\n",tid);
		
		
		for (int i=tid*10; i< (tid+1)*10; i++ ) {
			DBTransaction tx(seqs, store, mutex);
			bool b = false;
			while (b==false) {
			tx.Begin();
			
			for (int j=1; j<4; j++) {
	 			char* key = new char[100];
				snprintf(key, sizeof(key), "%d", j);
				Slice k(key);
	
				char* value = new char[100];
				snprintf(value, sizeof(value), "%d", i);
				Slice *v = new leveldb::Slice(value);

				printf("Insert %s ", key);
				printf(" Value %s\n", value);
				tx.Add(t, k, *v);			
			}
			b = tx.End();
			//if (b==true)printf("%d\n", i);
			}
			
			DBTransaction tx1(seqs, store, mutex);
			b =false;
			while (b==false) {
			tx1.Begin();
			
			for (int j=1; j<4; j++) {
				char* key = new char[100];
				snprintf(key, sizeof(key), "%d", j);
				Slice k(key);
				

				Status s;
				printf("Read begin %d\n",j);
				tx1.Get(k, &(str[j-1]), &s);
				printf("Tid %d get %s %s\n",tid,key,&str[j-1]);
			}						
			b = tx1.End();
			if (b==true)printf("%d\n", i);
			}
			assert(str[0]==str[1]);
			assert(str[1]==str[2]);
			printf("Tid %d Iter %d\n",tid,i);

		}
		{
		  MutexLock l(&shared->mu);
		  shared->num_done++;
		  if (shared->num_done >= shared->total) {
			shared->cv.SignalAll();
		  }
		}

	}
	void Run() {
		 
		SharedState shared;
		shared.total = FLAGS_threads;
		shared.num_initialized = 0;
		shared.start_time = 0;
		shared.end_time = 0;
		shared.num_done = 0;
		shared.start = false;
		 
		 
		 ThreadArg* arg = new ThreadArg[FLAGS_threads];
		 
		 for (int i = 0; i < FLAGS_threads; i++) {	 	
		 	arg[i].thread = new ThreadState(i);
			arg[i].seqs = seqs;
			arg[i].store = store;
			arg[i].mutex = mutex;
			arg[i].thread->shared = &shared;
			//printf("Out tid %lx\n", &arg[i]);
			Env::Default()->StartThread(ThreadBody, &arg[i]);
			
		 }

		 shared.mu.Lock();
		 while (shared.num_done < FLAGS_threads) {
		  shared.cv.Wait();
		 }
		 shared.mu.Unlock();
	}
};
}// end namespace leveldb

int main()
{
	leveldb::Options options;
	leveldb::InternalKeyComparator cmp(options.comparator);
	leveldb::HashTable seqs;
	leveldb::MemTable *store = new leveldb::MemTable(cmp);
	leveldb::port::Mutex mutex;
	
	leveldb::Benchmark *benchmark = new leveldb::Benchmark(&seqs, store, &mutex);
  	benchmark->Run();
  	return 0;
}
	
	


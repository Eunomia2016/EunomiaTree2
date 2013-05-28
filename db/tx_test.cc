// Copyright (c) 2011 The LevelDB Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file. See the AUTHORS file for names of contributors.

#include "db/dbtransaction.h"
#include "leveldb/env.h"
#include "port/port.h"
#include "util/mutexlock.h"


static int FLAGS_threads = 4;
static const char* FLAGS_benchmarks =
	"equal,"
	"counter,"
	"nocycle,"
	"consistency";

namespace leveldb {


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

	static void ConsistencyTest(void* v) {
		ThreadArg* arg = reinterpret_cast<ThreadArg*>(v);
		int tid = (arg->thread)->tid;
		SharedState *shared = arg->thread->shared;
		HashTable *seqs = arg->seqs;
		MemTable *store = arg->store;
		port::Mutex *mutex = arg->mutex;
		
		ValueType t = kTypeValue;
		for (int i=tid; i<tid+1000; i+=2 ){
			DBTransaction tx(seqs, store, mutex);
			bool b = false;
			while (b==false) {
			tx.Begin();
		    char* key = new char[100];
			snprintf(key, sizeof(key), "%d", i);
			Slice k(key);
			char* value = new char[100];
			snprintf(value, sizeof(value), "%d", tid);
			Slice *v = new leveldb::Slice(value);
			tx.Add(t, k, *v);
			//printf("tid %d iter %d\n",tid, i);
				

			char* key1 = new char[100];
			snprintf(key1, sizeof(key1), "%d", i+1);
			Slice k1(key1);
			char* value1 = new char[100];
			snprintf(value1, sizeof(value1), "%d", tid);
			Slice *v1 = new leveldb::Slice(value1);
			tx.Add(t, k1, *v1);
			
		
			b = tx.End();
			//printf("tid %d iter %d\n",tid, i);	
			}			
		}
		{
		  MutexLock l(&shared->mu);
		  shared->num_done++;
		  if (shared->num_done >= shared->total) {
			shared->cv.SignalAll();
		  }
		}
	}
	
	static void NocycleTest(void* v) {
		ThreadArg* arg = reinterpret_cast<ThreadArg*>(v);
		int tid = (arg->thread)->tid;
		SharedState *shared = arg->thread->shared;
		HashTable *seqs = arg->seqs;
		MemTable *store = arg->store;
		port::Mutex *mutex = arg->mutex;

		ValueType t = kTypeValue;
		std::string *str  = new std::string[4];
		//printf("start %d\n",tid);
		
		
		for (int i=tid*10000; i< (tid+1)*10000; i++ ) {
			DBTransaction tx(seqs, store, mutex);
			bool b = false;
			while (b==false) {
			tx.Begin();
			char* key = new char[100];
			snprintf(key, sizeof(key), "%d", tid);
			Slice k(key);
			char* value = new char[100];
			snprintf(value, sizeof(value), "%d", 1);
			Slice *v = new leveldb::Slice(value);
			tx.Add(t, k, *v);		

			char* key1 = new char[100];
			snprintf(key1, sizeof(key1), "%d", (tid+1) % 4);
			Slice k1(key1);
			char* value1 = new char[100];
			snprintf(value1, sizeof(value1), "%d", 2);
			Slice *v1 = new leveldb::Slice(value1);
			tx.Add(t, k1, *v1);	

			b = tx.End();
			}

			if (i % 10 == tid && i>10) {
				DBTransaction tx1(seqs, store, mutex);
				b =false;
				while (b==false) {
				tx1.Begin();
			
				for (int j=0; j<4; j++) {
					char* key = new char[100];
					snprintf(key, sizeof(key), "%d", j);
					Slice k(key);				

					Status s;
					tx1.Get(k, &(str[j]), &s);
					//printf("Tid %d get %s %s\n",tid,key,&str[j]);
				}						
				b = tx1.End();
			
				}
				bool e = (str[0]==str[1]) && (str[1]==str[2]) && (str[2]==str[3]);
				assert(!e);
			}
			
		}
		{
		  MutexLock l(&shared->mu);
		  shared->num_done++;
		  if (shared->num_done >= shared->total) {
			shared->cv.SignalAll();
		  }
		}
	}
	static void CounterTest(void* v) {
		ThreadArg* arg = reinterpret_cast<ThreadArg*>(v);
		int tid = (arg->thread)->tid;
		SharedState *shared = arg->thread->shared;
		HashTable *seqs = arg->seqs;
		MemTable *store = arg->store;
		port::Mutex *mutex = arg->mutex;
		//printf("start %d\n",tid);
		ValueType t = kTypeValue;
		for (int i=tid*10000; i< (tid+1)*10000; i++ ) {
			DBTransaction tx(seqs, store, mutex);
			bool b = false;
			while (b==false) {
			tx.Begin();
			
			
	 		char* key = new char[100];
			snprintf(key, sizeof(key), "%d", 1);
			Slice k(key);
			Status s;
			std::string str;
			tx.Get(k, &str, &s);
	
			char* value = new char[100];
			snprintf(value, sizeof(value), "%d", atoi(str.c_str())+1);
			Slice *v = new leveldb::Slice(value);

			//printf("Insert %s ", key);
			//printf(" Value %s\n", value);
			tx.Add(t, k, *v);			
			
			b = tx.End();
			
			}
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
	
	static void EqualTest(void* v) {
		ThreadArg* arg = reinterpret_cast<ThreadArg*>(v);
		int tid = (arg->thread)->tid;
		SharedState *shared = arg->thread->shared;
		HashTable *seqs = arg->seqs;
		MemTable *store = arg->store;
		port::Mutex *mutex = arg->mutex;

		ValueType t = kTypeValue;
		std::string *str  = new std::string[3];

		//printf("In tid %lx\n", arg);
		//printf("start %d\n",tid);
		
		
		for (int i=tid*1000; i< (tid+1)*1000; i++ ) {
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

				//printf("Insert %s ", key);
				//printf(" Value %s\n", value);
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
				//printf("Read begin %d\n",j);
				tx1.Get(k, &(str[j-1]), &s);
				//printf("Tid %d get %s %s\n",tid,key,&str[j-1]);
			}						
			b = tx1.End();
			//if (b==true)printf("%d\n", i);
			}
			assert(str[0]==str[1]);
			assert(str[1]==str[2]);
			//printf("Tid %d Iter %d\n",tid,i);

		}
		{
		  MutexLock l(&shared->mu);
		  shared->num_done++;
		  if (shared->num_done >= shared->total) {
			shared->cv.SignalAll();
		  }
		}

	}
	void Run(void (*method)(void* arg), Slice name ) {
		int num = FLAGS_threads;
		printf("%s start\n", name.data());				 		
		if (name == Slice("counter")) {
			ValueType t = kTypeValue;
			DBTransaction tx(seqs, store, mutex);
			bool b =false;
			while (b==false) {
			tx.Begin();
			
			
			char* key = new char[100];
			snprintf(key, sizeof(key), "%d", 1);
			Slice k(key);
			char* value = new char[100];
			snprintf(value, sizeof(value), "%d", 0);
			Slice *v = new leveldb::Slice(value);

			tx.Add(t, k, *v);				
												
			b = tx.End();
			//if (b==true)printf("%d\n", i);
			}
			//printf("init \n");
		}
		else if (name == Slice("nocycle")) num = 4;


		SharedState shared;
		shared.total = num;
		shared.num_initialized = 0;
		shared.start_time = 0;
		shared.end_time = 0;
		shared.num_done = 0;
		shared.start = false;
		 
		 ThreadArg* arg = new ThreadArg[num];
		 for (int i = 0; i < num; i++) {	 	
		 	arg[i].thread = new ThreadState(i);
			arg[i].seqs = seqs;
			arg[i].store = store;
			arg[i].mutex = mutex;
			arg[i].thread->shared = &shared;
			//printf("Out tid %lx\n", &arg[i]);
			Env::Default()->StartThread(method, &arg[i]);
			
		 }

		 shared.mu.Lock();
		 while (shared.num_done < num) {
		  shared.cv.Wait();
		 }
		 shared.mu.Unlock();
		 //printf("all done\n");
		 if (name == Slice("equal")) printf("EqualTest pass!\n");
		 else if (name == Slice("nocycle")) printf("NocycleTest pass!\n");
		 else if (name == Slice("counter")) {
		 	ValueType t = kTypeValue;
		 	DBTransaction tx(seqs, store, mutex);
			bool b =false; int result;
			//printf("verify\n");
			while (b==false) {
			tx.Begin();			
			
			char* key = new char[100];
			snprintf(key, sizeof(key), "%d", 1);
			Slice k(key);
			Status s;
			std::string str;
			tx.Get(k, &str, &s);
			result = atoi(str.c_str());
			//printf("result %d\n",result);
			b = tx.End();
			}
			assert(result == (10000*num));
			printf("CounterTest pass!\n");
		 }
		 else if (name == Slice("consistency")) {
		 	//printf("verify\n");
		 	for (int i = 0; i< num+999; i++) {
				char* key = new char[100];
				snprintf(key, sizeof(key), "%d", i);
				Slice k(key);
				bool found = false;
				uint64_t seq = 0;
				found = seqs->Lookup(key, (void **)&seq);
				assert(found);

				uint64_t seq1 = num+1;
				LookupKey lkey(key, seq1);				
				found = false;
				std::string value;
				Status s; int j = 0;
				uint64_t mseq = seq1;
				while(!found && j<3) {	
					j++;				
					mutex->Lock();
					found = store->GetSeq(lkey, &value, &s , &mseq);
					mutex->Unlock();	
				}			
				assert(found);
				assert(mseq<=seq);
				
				
		 	}
		 	printf("ConsistencyTest pass!\n");
		 }
	}
};
}// end namespace leveldb

int main(int argc, char**argv)
{
	for (int i = 1; i < argc; i++) {

		 int n;
		 char junk;
	 
		 if (leveldb::Slice(argv[i]).starts_with("--benchmarks=")) {
		   FLAGS_benchmarks = argv[i] + strlen("--benchmarks=");
		 } else if (sscanf(argv[i], "--threads=%d%c", &n, &junk) == 1) {
		   FLAGS_threads = n;
	 	 }
 	}

	const char* benchmarks = FLAGS_benchmarks;
	void (* method)(void* arg) = NULL;
	leveldb::Options options;
	leveldb::InternalKeyComparator cmp(options.comparator);
    while (benchmarks != NULL) {
      const char* sep = strchr(benchmarks, ',');
      leveldb::Slice name;
      if (sep == NULL) {
        name = benchmarks;
        benchmarks = NULL;
      } else {
        name = leveldb::Slice(benchmarks, sep - benchmarks);
        benchmarks = sep + 1;
      }
	  if (name == leveldb::Slice("equal")){
        method = &leveldb::Benchmark::EqualTest;
      } 
	  else if (name == leveldb::Slice("counter")) {
	  	method = &leveldb::Benchmark::CounterTest;
	  }
	  else if (name == leveldb::Slice("nocycle")) {
	  	method = &leveldb::Benchmark::NocycleTest;
	  }
	  else if (name == leveldb::Slice("consistency")) {
	  	method = &leveldb::Benchmark::ConsistencyTest;
	  }
	  
	  leveldb::HashTable seqs;
	  leveldb::MemTable *store = new leveldb::MemTable(cmp);
	  leveldb::port::Mutex mutex;
	
	  leveldb::Benchmark *benchmark = new leveldb::Benchmark(&seqs, store, &mutex);
  	  benchmark->Run(method, name);
    }
	

	
  	return 0;
}
	
	


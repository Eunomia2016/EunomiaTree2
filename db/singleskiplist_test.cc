// Copyright (c) 2011 The LevelDB Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file. See the AUTHORS file for names of contributors.

#include "db/dbtx.h"
#include "db/dbrotx.h"
#include "db/memstore_skiplist.h"

#include "leveldb/env.h"
#include "port/port.h"
#include "util/mutexlock.h"


static int FLAGS_txs = 100;
static int FLAGS_threads = 4;
static const char* FLAGS_benchmarks =
	"equal,"
	"counter,"
	"nocycle,"
	"delete,"
	"nocycle_readonly,"
	"readonly";
//	"range";
	

namespace leveldb {

typedef uint64_t Key;


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
	
	MemStoreSkipList* store;
	
  public:
	Benchmark(	MemStoreSkipList *s ) {
		store = s;
		
	}
	struct ThreadArg {
		ThreadState *thread;
		MemStoreSkipList *store;
		
		
	};

	static void NocycleReadonlyTest(void* v) {

		ThreadArg* arg = reinterpret_cast<ThreadArg*>(v);
		int tid = (arg->thread)->tid;
		SharedState *shared = arg->thread->shared;
		MemStoreSkipList *store = arg->store;
		
		
		int num = shared->total;
		
		uint64_t *str  = new uint64_t[num];
		//printf("start %d\n",tid);
		bool fail = false;
		
		for (int i=tid*FLAGS_txs; i< (tid+1)*FLAGS_txs; i++ ) {

			leveldb::DBTX tx(store);
			bool b = false;
			while (b==false) {
				tx.Begin();
				uint64_t* key = new uint64_t(); 
				*key = tid;
				uint64_t* value = new uint64_t(); 
				*value = 1;
				tx.Add(*key, value);
		
				uint64_t* key1 = new uint64_t(); 
				*key1 = (tid+1) % num;
				uint64_t* value1 = new uint64_t(); 
				*value1 = 2;
				tx.Add(*key1, value1);
				b = tx.End();
			}
			
			if (i % 10 == (tid%10) && i>10) {
				leveldb::DBROTX tx1(store);
							
				tx1.Begin();
			
				for (int j=0; j<num; j++) {

					uint64_t *k = new uint64_t();
					*k = j;
					uint64_t *v;
					
					tx1.Get(*k, &v);
					str[j] = *v;
					
				}						
				b = tx1.End();
			   

				bool e = true;
				for (int j=0;j<num-1; j++) {
					e = e && (str[j]==str[j+1]);
				}
				
				//assert(!e); 
				if (e) {
					fail = true;  
					printf("all keys have same value\n");
					break;
				}
			}
			
		}
		
		{
		  MutexLock l(&shared->mu);
		  if (fail) shared->fail = fail; 
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
		MemStoreSkipList *store = arg->store;
		
		
		int num = shared->total;
		
		uint64_t *str  = new uint64_t[num];
		//printf("start %d\n",tid);
		bool fail = false;
		
		for (int i=tid*FLAGS_txs; i< (tid+1)*FLAGS_txs; i++ ) {

			leveldb::DBTX tx(store);
			bool b = false;
			while (b==false) {
				tx.Begin();
				uint64_t* key = new uint64_t(); 
				*key = tid;
				uint64_t* value = new uint64_t(); 
				*value = 1;
				tx.Add(*key, value);
		
				uint64_t* key1 = new uint64_t(); 
				*key1 = (tid+1) % num;
				uint64_t* value1 = new uint64_t(); 
				*value1 = 2;
				tx.Add(*key1, value1);
				b = tx.End();
			}
			//printf("Add %d %d\n",tid,(tid+1) % num);
			if (i % 10 == (tid%10) && i>10) {
				leveldb::DBTX tx1(store);
				b =false;
				
				while (b==false) {
				tx1.Begin();
			
				for (int j=0; j<num; j++) {

					uint64_t *k = new uint64_t();
					*k = j;
					uint64_t *v;
					
					tx1.Get(*k, &v);
					//printf("%d-----1\n",j);
					str[j] = *v;
					//printf("%d------2\n",j);
					
				}						
				b = tx1.End();
			   
				}

				bool e = true;
				for (int j=0;j<num-1; j++) {
					e = e && (str[j]==str[j+1]);
				}
				
				//assert(!e); 
				if (e) {
					fail = true;  
					printf("all keys have same value\n");
					break;
				}
			}
			
		}
		
		{
		  MutexLock l(&shared->mu);
		  if (fail) shared->fail = fail; 
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
		MemStoreSkipList *store = arg->store;
		
		
		//printf("start %d\n",tid);
		
		for (int i=tid*FLAGS_txs; i< (tid+1)*FLAGS_txs; i++ ) {
			leveldb::DBTX tx( store);
			bool b = false;
			while (b==false) {
			tx.Begin();
			
			
	 		uint64_t k = 1;
			uint64_t *v;
			
			tx.Get(k, &v);

			uint64_t *value = new uint64_t();
			*value = *v + 1;

			//printf("Insert %s ", key);
			//printf(" Value %s\n", value);
			tx.Add(k, value);			
			
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
		MemStoreSkipList *store = arg->store;
		

		
		uint64_t* str  = new uint64_t[3];

		//printf("In tid %lx\n", arg);
		//printf("start %d\n",tid);
		
		bool fail = false;
		for (int i = tid*FLAGS_txs; i < (tid+1)*FLAGS_txs; i++ ) {

			//printf("[EqualTest]snapshot %d\n", store->snapshot);
			leveldb::DBTX tx( store);
			bool b = false;
			while (b == false) {
				tx.Begin();
				
				for (int j=1; j<4; j++) {
		 			
					uint64_t *key = new uint64_t();
					*key = j;
					uint64_t *value = new uint64_t();
					*value = i;
		 			
					tx.Add(*key, value);			
				}
				b = tx.End();

			}
			
			leveldb::DBTX tx1( store);
			b = false;
			while (b == false) {
				tx1.Begin();
				
				for (int j = 1; j < 4; j++) {
					
					uint64_t *key = new uint64_t();
					*key = j;
					uint64_t *value;					
					
					tx1.Get(*key, &value);
					str[j-1] = *value;
				
				}						
				b = tx1.End();

			}
			
			if (!(str[0]==str[1])){
				printf("Key 1 has value %d, Key 2 has value %d, not equal\n",str[0],str[1]);
				fail = true;
				
				DBTX::slock.Lock();
				store->PrintList();
				
				DBTX::slock.Unlock();
				break;
			}
			if (!(str[1]==str[2])) {
				printf("Key 2 has value %d, Key 3 has value %d, not equal\n",str[1],str[2]);
				fail = true;
				
				DBTX::slock.Lock();
				store->PrintList();
				DBTX::slock.Unlock();

				break;
			}
			//printf("Pass 1\n");
			{
				leveldb::DBROTX tx2( store);
				bool found[3];
				tx2.Begin();
				int j;
				for (j = 1; j < 4; j++) {
					
					uint64_t *key = new uint64_t();
					*key = j;
					uint64_t *value;					
						
					found[j-1] = tx2.Get(*key, &value);
					if (found[j-1]) str[j-1] = *value;
					
				}						

				tx2.End();
				bool f = true;
				for (j = 1; j < 4; j++)
					if (!found[j-1]) {
						printf("[%d]Key %d not found\n", i, j);
						f = false;
					}
				if (!f) {

					fail = true;
					//store->PrintList();

					break;
				}
				
				if (!(str[0]==str[1])){
					printf("In RO, Key 1 has value %d, Key 2 has value %d, not equal\n",str[0],str[1]);
					fail = true;
					break;
				}
				if (!(str[1]==str[2])) {
					printf("In RO, Key 2 has value %d, Key 3 has value %d, not equal\n",str[1],str[2]);
					fail = true;
					break;
				}
				
			}
			//printf("Pass 2\n");
		}
		{
		  MutexLock l(&shared->mu);
		  if (fail) shared->fail = fail;
		  shared->num_done++;
		  if (shared->num_done >= shared->total) {
			shared->cv.SignalAll();
		  }
		}

	}


	
	static void DeleteTest(void* v) {
	
			ThreadArg* arg = reinterpret_cast<ThreadArg*>(v);
			int tid = (arg->thread)->tid;
			SharedState *shared = arg->thread->shared;
			MemStoreSkipList *store = arg->store;
			
	
	
			//printf("In tid %lx\n", arg);
			//printf("start %d\n",tid);
			
			bool fail = false;
			for (int i = tid*FLAGS_txs; i < (tid+1)*FLAGS_txs; i++ ) {
	
				leveldb::DBTX tx( store);
				bool b = false;
				while (b == false) {
					tx.Begin();
					
					uint64_t *value = new uint64_t();
					*value = i;
					tx.Add(4, value);			

					uint64_t *value1 = new uint64_t();
					*value1 = i;
					tx.Add(5, value1);	
					
					b = tx.End();
	
				}

				leveldb::DBTX tx1( store);
				b = false; 
				bool f1 = true; bool f2 = false;
				uint64_t *value; uint64_t *value1;
				while (b == false) {
					tx1.Begin();
					
										
						
					f1 = tx1.Get(4, &value);
					f2 = tx1.Get(5, &value);	

					
					tx1.Get(3, &value);
					tx1.Get(6, &value1);	

					
					b = tx1.End();
	
				}
				
				if (f1 != f2){
					printf("Get Key 4 return %d, Get Key 5 return %d, not equal\n",f1,f2);
					fail = true;
					break;
				}
				if (*value != *value1) {
					printf("Key 3 value %d and Key 6 value %d, should have same values\n",*value, *value1);
					fail = true;
					break;
				}
				
				leveldb::DBTX tx2( store);
				b = false;
				while (b == false) {
					tx2.Begin();

					uint64_t *value = new uint64_t();
					*value = i;
					tx.Add(3, value);	


					tx2.Delete(4);			

					tx2.Delete(5);

		

					uint64_t *value1 = new uint64_t();
					*value1 = i;
					tx.Add(6, value1);	
					
					b = tx2.End();
	
				}
				
				leveldb::DBROTX tx3( store);
				
				f1 = true; f2 = false;
				
				
				tx3.Begin();				
														
				f1 = tx3.Get(4, &value);
				f2 = tx3.Get(5, &value);	
				
				tx3.End();
	
				
				
				if (f1 != f2){
					printf("In read-only tx, Get Key 4 return %d, Get Key 5 return %d, not equal\n",f1,f2);
					fail = true;
					break;
				}
	
			}
			{
			  MutexLock l(&shared->mu);
			  if (fail) shared->fail = fail;
			  shared->num_done++;
			  if (shared->num_done >= shared->total) {
				shared->cv.SignalAll();
			  }
			}
	
	}
	
	void Run(void (*method)(void* arg), Slice name ) {
		int num = FLAGS_threads;
		printf("%s start\n", name.ToString().c_str());				 		


		if (name == Slice("readonly")  || name == Slice("range")) {
			
			for (int j = 1; j<=3; j++) {
			  leveldb::DBTX tx( store);
			  bool b =false;
			 
			
				for (int i=1; i<100;i++) {
					if (i % 10 == 0) continue;
					b = false;
				 	while (b==false) {
			    		tx.Begin();	
				  		
				  	  	uint64_t *key = new uint64_t();
				  	  	*key = i;
				  	  	uint64_t *value = new uint64_t();
				  	  	*value = j;			
					  	tx.Add(*key, value);				
				  		b = tx.End();
						  
					}

				  
				}
				
			
			
		
		
				leveldb::DBROTX tx1( store);

				tx1.Begin();
				uint64_t* k = new uint64_t();
				*k = 1;
					
				uint64_t *r;
				tx1.Get(*k,  &r);
				if (*r != j) {
					printf("Key 1 get %d should be %d\n", *r, j);
					return;
				}
				tx1.End();
			}	  
		}

		if (name == Slice("readonly")) {	
			//check
			leveldb::DBROTX tx2(store);
			int m; uint64_t *r;
			bool c1 = false;bool c2 = false;bool c3 = false;
			tx2.Begin();	
			for (m=1; m<100;m++) {
			  
			    bool found;  
			   		  		    
			    uint64_t *key = new uint64_t();
			    *key = m;
			    
			    
			    found = tx2.Get(*key,  &r);		

			    if (m % 10 == 0 ) {
				  if (found) c1 = true;
				  break;
			    }
			    else if (!found)  {
			  	  c2 = true;
				  break;
			    }
			    else if (*r != 3) {
			  	  c3 = true;
				  break;			 			  
			    }
				
			}
			tx2.End();
			
			if (c1 ) {
				printf("Key %d not inserted but found\n", m);
				return;
			}
			else if (c2)  {
			  	printf("Key %d inserted but not found\n", m);
				return;
			}
			else if (c3) {
			  	printf("Key %d get %d instead of 3\n", m, *r);
				return;			 			  
			}
		
			printf("ReadonlyTest pass!\n");	
			return;
		}
/*
		else if (name == Slice("range")) {
			leveldb::DBROTX tx2(store);
			bool c1 = false;bool c2 = false;bool c3 = false;			 
			  
			 tx2.begin();		
			 Iterator iter = tx2.Iterator();
			 iter.Seek(1); 
			 uint64_t key = 1;
			 uint64_t m = 0;
			 while (iter.Valid()) {
			    m = iter.Key();
			    uint64_t *r = iter.Value();
			    
			    if (m % 10 == 0 ) {
				  c1 = true;
				  break;
			    }
			    
			   
			    else if (m != key)  {
			  	  c2 = true;
				  break;
			    }
			    else if (*r != 3) {
			  	  c3 = true;
				  break;			 			  
			    }
			    k++;
			    if  (m % 10 == 9) k++;
			    iter.Next();
			 }
			 tx2.End();
			
			if (c1 ) {
				printf("Key %d not inserted but found\n", m);
				return;
			}
			else if (c2)  {
			  	printf("Key %d inserted but not found\n", key);
				return;
			}
			else if (c3) {
			  	printf("Key %d get %d instead of 3\n", m, *r);
				return;			 			  
			}
			else if (m!=99) {
				printf("Iterate to %d should be 99\n", m);
				return;
			}
			printf("RangeTest pass!\n");	
			return;
		}
		
*/
		
		if (name == Slice("counter") ) {
			
			
			leveldb::DBTX tx( store);
			bool b =false;
			while (b==false) {
			tx.Begin();
			
			
			uint64_t *key = new uint64_t();
			*key = 1;
			uint64_t *value = new uint64_t();
			*value = 0;
			
			tx.Add(*key, value);				
												
			b = tx.End();
			
			//if (b==true)printf("%d\n", i);
			}
			//printf("init \n");
		}
		else if (name == Slice("nocycle") || name == Slice("nocycle_readonly")) {
			leveldb::DBTX tx( store);
			bool b =false;
			while (b==false) {
			tx.Begin();
			
			for (int i=0; i<num; i++) {
			  uint64_t *key = new uint64_t();
			  *key = i;
			  uint64_t *value = new uint64_t();
			  *value = 1;
			
			  tx.Add(*key, value);				
			}									
			b = tx.End();
			
			//if (b==true)printf("%d\n", i);
			}
		}
	    else if (name == Slice("delete")) {
			leveldb::DBTX tx( store);
			bool b =false;
			while (b==false) {
			tx.Begin();
			
			for (int i=0; i<10; i++) {
			  uint64_t *key = new uint64_t();
			  *key = i;
			  uint64_t *value = new uint64_t();
			  *value = 1;
			
			  tx.Add(*key, value);				
			}									
			b = tx.End();
			
			//if (b==true)printf("%d\n", i);
			}
		}

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
			
			arg[i].store = store;
			
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
		 if (shared.fail) {
		 	printf("%s fail!\n", name.ToString().c_str());	
		 }else if (name == Slice("equal")) printf("EqualTest pass!\n");
		 else if (name == Slice("nocycle")) printf("NocycleTest pass!\n");
		 else if (name == Slice("nocycle_readonly")) printf("NocycleReadonlyTest pass!\n");		 
		 else if (name == Slice("delete")) printf("DeleteTest pass!\n");
		 else if (name == Slice("counter")) {
		 	
		 	
		 	leveldb::DBTX tx(store);
			bool b =false; int result;
			//printf("verify\n");
			while (b==false) {
			tx.Begin();			
			
			uint64_t* k = new uint64_t();
			*k = 1;
			
			uint64_t *str;
			tx.Get(*k, &str);
			result = *str;
			//printf("result %d\n",result);
			b = tx.End();
//			store->PrintList();
			}
			if (result != (FLAGS_txs*num)) {
				printf("Get %d instead of %d from the counter fail!\n",result,FLAGS_txs*num);
				
			}
			else printf("CounterTest pass!\n");
		 }
		 
			
	}
};
}// end namespace leveldb

int main(int argc, char**argv)
{
	for (int i = 1; i < argc; i++) {

		 int n;
		 char junk;
	 	 if (leveldb::Slice(argv[i]).starts_with("--help")){
		 	printf("To Run :\n./tx_test [--benchmarks=Benchmark Name(default: all)] [--num=number of tx per thread(default: 100)] [--threads= number of threads (defaults: 4)]\n");
			printf("Benchmarks : \nequal\t Each tx write (KeyA, x) (KeyB, x) , check get(KeyA)==get(KeyB) in other transactions\ncounter\t badcount\nnocycle(nocycle_readonly)\t n threads, each tx write (tid,1) ((tid+1) %n,2) , never have all keys' value are the same\n");
			printf("delete\t write or delete 2 keys in a tx, check both keys exist or both not exist\n");
			return 0;
	 	 }
		 if (leveldb::Slice(argv[i]).starts_with("--benchmarks=")) {
		   FLAGS_benchmarks = argv[i] + strlen("--benchmarks=");
		 } else if (sscanf(argv[i], "--threads=%d%c", &n, &junk) == 1) {
		   FLAGS_threads = n;
	 	 } else if (sscanf(argv[i], "--num=%d%c", &n, &junk) == 1) {
		   FLAGS_txs = n;
	 	 }
		 
 	}

	const char* benchmarks = FLAGS_benchmarks;
	void (* method)(void* arg) = NULL;
	
  	
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
	  else if (name == leveldb::Slice("nocycle_readonly")) {
	  	method = &leveldb::Benchmark::NocycleReadonlyTest;
	  }
	  else if (name == leveldb::Slice("delete")) {
	  	method = &leveldb::Benchmark::DeleteTest;
	  }
	  else if (name == leveldb::Slice("readonly") || name == leveldb::Slice("range")) {
	  	method = NULL;
	  }
	  

	 leveldb::MemStoreSkipList *store = new leveldb::MemStoreSkipList();
	
	  
	 leveldb::Benchmark *benchmark = new leveldb::Benchmark(store);

	  benchmark->Run(method, name);
    }
	

	
  	return 0;
}
	
	


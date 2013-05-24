// Copyright (c) 2011 The LevelDB Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file. See the AUTHORS file for names of contributors.

#include "db/dbtransaction.h"
#include "leveldb/env.h"
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
struct ThreadState {
	int tid;
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
		ThreadState *tid;
		HashTable *seqs;
		MemTable *store;
		port::Mutex *mutex;
	};
	static void ThreadBody(void* v) {
		ThreadArg* arg = reinterpret_cast<ThreadArg*>(v);
		int tid = (arg->tid)->tid;
		HashTable *seqs = arg->seqs;
		MemTable *store = arg->store;
		port::Mutex *mutex = arg->mutex;

		ValueType t = kTypeValue;
		std::string *str  = new std::string[3];

		printf("In tid %lx\n", arg);
		printf("start %d\n",tid);
		
		
		for (int i=tid*10; i< (tid+1)*10; i++ ) {
			DBTransaction tx(seqs, store, mutex);
			bool b = false;
			while (!b) {
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
			}
			
			DBTransaction tx1(seqs, store, mutex);
			b =false;
			while (!b) {
			tx1.Begin();
			for (int j=1; j<4; j++) {
				char* key = new char[100];
				snprintf(key, sizeof(key), "%d", j);
				Slice k(key);
				

				Status s;
				tx1.Get(k, &(str[j-1]), &s);
			}						
			b = tx1.End();
			}
			assert(str[0]==str[1]);
			assert(str[1]==str[2]);

		}

	}
	void Run() {
		 ThreadArg* arg = new ThreadArg[4];
		 
		 for (int i = 0; i < 4; i++) {	 	
		 	arg[i].tid = new ThreadState(i);
			arg[i].seqs = seqs;
			arg[i].store = store;
			arg[i].mutex = mutex;
			printf("Out tid %lx\n", &arg[i]);
			Env::Default()->StartThread(ThreadBody, &arg[i]);
			
		 }

		 while(true);
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
	
	


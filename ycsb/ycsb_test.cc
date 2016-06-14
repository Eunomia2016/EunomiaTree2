#include <set>
#include "leveldb/env.h"
#include "util/arena.h"
#include "util/hash.h"
#include "util/random.h"
#include "util/testharness.h"
#include <deque>
#include <set>
#include "port/port.h"
#include "port/atomic.h"
#include <iostream>
#include "util/mutexlock.h"
#include "leveldb/comparator.h"

#include <vector>
#include "memstore/memstore_bplustree.h"
#include "lockfreememstore/lockfree_hash.h"
#include "memstore/memstore_hash.h"
#include "memstore/memstore_skiplist.h"
#include "memstore/memstore_cuckoohash.h"

#include "db/dbtx.h"
#include "db/dbtables.h"
static const char* FLAGS_benchmarks = "mix";

static int FLAGS_num = 100000;
static int FLAGS_threads = 1;
static uint64_t nkeys = 100000000;
static uint64_t pre_keys = 10000;

uint64_t next_o_id = 0;

#define CHECK 0
#define YCSBRecordSize 100
#define GETCOPY 0
namespace leveldb {

typedef uint64_t Key;

static inline ALWAYS_INLINE int64_t makeKeys(int32_t w_id, int32_t d_id, int32_t o_id, int32_t number) {
	int32_t upper_id = w_id * 10 + d_id;
	int64_t oid = static_cast<int64_t>(upper_id) * 10000000 + static_cast<int64_t>(o_id);
	int64_t olid = oid * 15 + number;
	int64_t id = static_cast<int64_t>(olid);
	return id;
}

class fast_random {
public:
	fast_random(unsigned long seed)
		: seed(0) {
		set_seed0(seed);
	}

	inline unsigned long
	next() {
		return ((unsigned long) next(32) << 32) + next(32);
	}

	inline uint32_t
	next_u32() {
		return next(32);
	}

	inline uint16_t
	next_u16() {
		return next(16);
	}

	/** [0.0, 1.0) */
	inline double
	next_uniform() {
		return (((unsigned long) next(26) << 27) + next(27)) / (double)(1L << 53);
	}

	inline char
	next_char() {
		return next(8) % 256;
	}

	inline std::string
	next_string(size_t len) {
		std::string s(len, 0);
		for(size_t i = 0; i < len; i++)
			s[i] = next_char();
		return s;
	}

	inline unsigned long
	get_seed() {
		return seed;
	}

	inline void
	set_seed(unsigned long seed) {
		this->seed = seed;
	}

private:
	inline void
	set_seed0(unsigned long seed) {
		this->seed = (seed ^ 0x5DEECE66DL) & ((1L << 48) - 1);
	}

	inline unsigned long
	next(unsigned int bits) {
		seed = (seed * 0x5DEECE66DL + 0xBL) & ((1L << 48) - 1);
		return (unsigned long)(seed >> (48 - bits));
	}

	unsigned long seed;
};

class Benchmark {

private:
	int64_t total_count;

	Memstore *table;

	port::SpinLock slock;

	Random ramdon;

	DBTables *store;

private:

	struct SharedState {
		port::Mutex mu;
		port::CondVar cv;
		int total;

		volatile double start_time;
		volatile double end_time;

		int num_initialized;
		int num_done;
		bool start;
		volatile int num_half_done;
		std::vector<Arena*>* collector;

		SharedState() : cv(&mu) { }
	};

	// Per-thread state for concurrent executions of the same benchmark.
	struct ThreadState {
		int tid;			   // 0..n-1 when running in n threads
		SharedState* shared;
		int count;
		double time1;
		double time2;
		fast_random rnd;
		uint64_t seed;

		ThreadState(int index, uint64_t seed)
			: tid(index), rnd(seed) {
		}
	};

	struct ThreadArg {
		Benchmark* bm;
		SharedState* shared;
		ThreadState* thread;
		void (Benchmark::*method)(ThreadState*);
	};

	static void ThreadBody(void* v) {

		//printf("ThreadBody\n");
		ThreadArg* arg = reinterpret_cast<ThreadArg*>(v);
		SharedState* shared = arg->shared;
		ThreadState* thread = arg->thread;
		{
			MutexLock l(&shared->mu);

			shared->num_initialized++;
			if(shared->num_initialized >= shared->total) {
				shared->cv.SignalAll();
			}
			while(!shared->start) {
				shared->cv.Wait();
			}
		}

		double start = leveldb::Env::Default()->NowMicros();
		if(shared->start_time == 0)
			shared->start_time = start;

		(arg->bm->*(arg->method))(thread);

		double end = leveldb::Env::Default()->NowMicros();
		shared->end_time = end;
		//thread->time = end - start;

		{
			MutexLock l(&shared->mu);

			shared->num_done++;
			if(shared->num_done >= shared->total) {
				shared->cv.SignalAll();
			}
		}
	}

	void bind_cores(int tid){
		cpu_set_t mask;
		CPU_ZERO(&mask);
		CPU_SET(tid , &mask);
		pthread_setaffinity_np(pthread_self(), sizeof(mask), &mask);
	}
	//Main workload of YCSB
	void TxMix(ThreadState* thread) {
		int tid = thread->tid;

		bind_cores(tid);

		store->ThreadLocalInit(tid);
		int num = thread->count;

		int finish = 0 ;
		fast_random r = thread->rnd;
		std::string v;

		char nv[YCSBRecordSize];
		DBTX tx(store);
		tx.ThreadLocalInit();

		double start = leveldb::Env::Default()->NowMicros();

		while(finish < num) {
			double d = r.next_uniform();
			if(d < 0.8) {
				//printf("read begins\n");
				uint64_t key = r.next() % pre_keys;
				bool b = false;
				while(!b) {
					tx.Begin();
					uint64_t *s;
					tx.Get(0, key, &s);
#if GETCOPY
					std::string *p = &v;
					p->assign((char *)s, YCSBRecordSize);
#endif
					b = tx.End();
				}
				//printf("read ends\n");
				finish++;
			}
			if(d < 0.2) {
				//printf("write begins\n");
				uint64_t key = r.next() % pre_keys;
				bool b = false;
				while(!b) {
					tx.Begin();
					uint64_t *s;

					tx.Get(0, key, &s);
#if GETCOPY
					std::string *p = &v;
					p->assign((char *)s, YCSBRecordSize);
#endif
					std::string c(YCSBRecordSize, 'c');
					tx.Add(0, key, (uint64_t *)c.data(), YCSBRecordSize);

					b = tx.End();
				}
				finish++;
				//printf("write ends\n");
			}
		}


		double end = leveldb::Env::Default()->NowMicros();
		printf("Exe time: %f\n", (end - start) / 1000 / 1000);
		printf("Thread[%d] Op Throughput %lf ops/s\n", tid, num / ((end - start) / 1000 / 1000));
	}


	void Mix(ThreadState* thread) {
		int tid = thread->tid;

		bind_cores(tid);

		int num = thread->count;
		int finish = 0 ;
		int next_id = 3000;
		fast_random r = thread->rnd;
		double start = leveldb::Env::Default()->NowMicros();
		std::string v;
		char nv[YCSBRecordSize];
		while(finish < num) {
			double d = r.next_uniform();
			//Read
			if(d < 0) {
				uint64_t key = r.next() % pre_keys;
				Memstore::MemNode * mn = table->Get(key);
				char *s = (char *)(mn->value);
#if GETCOPY
				std::string *p = &v;
				p->assign(s, YCSBRecordSize);
#else
				//if(s == NULL)
					//printf("N");
#endif
				finish++;
			}
			//RMW
			else{
				//uint64_t key = r.next() % nkeys;
				next_id++;
				for(int idx = 0; idx < 15; idx++){
					uint64_t key = makeKeys(1,1, next_id, idx);
					//printf("write key = %lu\n", key);
					Memstore::MemNode * mn = table->GetWithInsert(key).node;
					char *s = (char *)(mn->value);
#if GETCOPY
					std::string *p = &v;
					p->assign(s,  YCSBRecordSize);
#else
					//if(s == NULL)
						//printf("N");
#endif
					std::string c(YCSBRecordSize, 'c');
					memcpy(nv, c.data(), YCSBRecordSize);
					mn = table->GetWithInsert(key).node;
					mn->value = (uint64_t *)(nv);
				}

				finish++;
			}
		}

		double end = leveldb::Env::Default()->NowMicros();
		printf("Exe time: %f\n", (end - start) / 1000 / 1000);
		printf("Thread[%d] Op Throughput %lf ops/s\n", tid, finish / ((end - start) / 1000 / 1000));
	}

public:

	Benchmark(): total_count(FLAGS_num), ramdon(1000) {}
	~Benchmark() {

	}

	void RunBenchmark(int thread_num, int num,
					  void (Benchmark::*method)(ThreadState*)) {
		SharedState shared;
		shared.total = thread_num;
		shared.num_initialized = 0;
		shared.start_time = 0;
		shared.end_time = 0;
		shared.num_done = 0;
		shared.start = false;
		shared.num_half_done = 0;

//		double start = leveldb::Env::Default()->NowMicros();
		fast_random r(8544290);
		ThreadArg* arg = new ThreadArg[thread_num];
		for(int i = 0; i < thread_num; i++) {
			arg[i].bm = this;
			arg[i].method = method;
			arg[i].shared = &shared;
			arg[i].thread = new ThreadState(i, r.next());
			arg[i].thread->shared = &shared;
			arg[i].thread->count = num;
			Env::Default()->StartThread(ThreadBody, &arg[i]);
		}

		shared.mu.Lock();
		while(shared.num_initialized < thread_num) {
			shared.cv.Wait();
		}

		shared.start = true;
		printf("Send Start Signal\n");
		shared.cv.SignalAll();
//		std::cout << "Startup Time : " << (leveldb::Env::Default()->NowMicros() - start)/1000 << " ms" << std::endl;

		while(shared.num_done < thread_num) {
			shared.cv.Wait();
		}
		shared.mu.Unlock();


		printf("Total Run Time : %lf ms\n", (shared.end_time - shared.start_time) / 1000);
		/*
			for (int i = 0; i < thread_num; i++) {
			  printf("Thread[%d] Put Throughput %lf ops/s\n", i, num/(arg[i].thread->time1/1000/1000));
			  printf("Thread[%d] Get Throughput %lf ops/s\n", i, num/(arg[i].thread->time2/1000/1000));
			}*/

		for(int i = 0; i < thread_num; i++) {
			delete arg[i].thread;
		}
		delete[] arg;
	}

	void Run() {
		//table = new leveldb::MemstoreBPlusTree();
		table = new leveldb::MemstoreEunoTree();
		//table = new leveldb::LockfreeHashTable();
		//table = new leveldb::MemstoreHashTable();
		//table = new leveldb::MemStoreSkipList();
		//table = new MemstoreCuckooHashTable();
		store = new DBTables();

		int num_threads = FLAGS_threads;
		int num_ = FLAGS_num;
		store->RCUInit(num_threads);
		Slice name = FLAGS_benchmarks;

		if(true) {
			for(uint64_t i = 1; i < pre_keys; i++) {
				std::string *s = new std::string(YCSBRecordSize, 'a');
				if(name == "txmix"){
					store->tables[0]->Put(i, (uint64_t *)s->data());
				}else{ 
					table->Put(i, (uint64_t *)s->data());
				}
			}
		}

		//  printf("depth %d\n",((leveldb::MemstoreBPlusTree *)table)->depth);
		//  table->PrintStore();
		//  exit(0);

		void (Benchmark::*method)(ThreadState*) = NULL;
		if(name == "mix")
			method = &Benchmark::Mix;
		else if(name == "txmix")
			method = &Benchmark::TxMix;
		printf("RunBenchmark\n");
		RunBenchmark(num_threads, num_, method);
		delete table;
	}

};

}  // namespace leveldb

int main(int argc, char** argv) {
	for(int i = 1; i < argc; i++) {
		int n;
		char junk;

		if(leveldb::Slice(argv[i]).starts_with("--benchmark=")) {
			FLAGS_benchmarks = argv[i] + strlen("--benchmark=");
		} else if(sscanf(argv[i], "--num=%d%c", &n, &junk) == 1) {
			FLAGS_num = n;
		} else if(sscanf(argv[i], "--threads=%d%c", &n, &junk) == 1) {
			FLAGS_threads = n;
		}
	}

	leveldb::Benchmark benchmark;
	benchmark.Run();
//  while (1);
	return 1;
}

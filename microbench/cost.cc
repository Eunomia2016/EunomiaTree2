#include <stdio.h>
#include <stdint.h>
#include <sys/mman.h>
#include <string.h>
#include <malloc.h>
#include <pthread.h>
#include <unistd.h>
#include <time.h>
#include <assert.h>
#include <sched.h>
#include "rtmRegion.h"
#include "lockRegion.h"


#define ARRAYSIZE 4*1024*1024/8 //4M
#define CASHELINESIZE 64 //64 bytes

//critical data
char padding[CASHELINESIZE];
int workingset = 16 ; //Default ws: 16
__thread uint64_t *array;
char padding1[CASHELINESIZE];
__thread uint8_t lock = 0;
char padding2[CASHELINESIZE];

volatile int ready = 0;
volatile int epoch = 0;
int thrnum = 1;
int bench = 1; // 1: RTM 2: Lock 3: Atomic 4: Raw

inline void RTMCompute() {
	
	for(int i = 0; i < workingset; i++) {		
		RTMRegion rtm(NULL);
		array[i]++;
	}
	
}

inline void LockCompute() {

	for(int i = 0; i < workingset; i++) {
		
		LockRegion l(&lock);
		array[i]++;
	}
}

inline void AtomicCompute() {
	for(int i = 0; i < workingset; i++) {
		atomic_inc64(&array[i]);
	}
}

inline void RawCompute() {
	for(int i = 0; i < workingset; i++) {
		array[i]++;
	}
}



int
diff_timespec(const struct timespec &end, const struct timespec &start)
{
    int diff = (end.tv_sec > start.tv_sec)?(end.tv_sec-start.tv_sec)*1000:0;
    assert(diff || end.tv_sec == start.tv_sec);
    if (end.tv_nsec > start.tv_nsec) {
        diff += (end.tv_nsec-start.tv_nsec)/1000000;
    } else {
        diff -= (start.tv_nsec-end.tv_nsec)/1000000;
    }
    return diff;
}

void thread_init(){
	//Allocate the array at heap
	array = (uint64_t *)malloc(ARRAYSIZE * sizeof(uint64_t));	
	//Touch every byte to avoid page fault 
	memset(array, 0, ARRAYSIZE * sizeof(uint64_t)); 

}

void* thread_body(void *x) {

	RTMRegionProfile prof;
	uint64_t count = 0;
	int lbench = bench;
	int lepoch = 0;
	
	struct timespec start, end;
	
	uint64_t tid = (uint64_t)x;

	cpu_set_t  mask;
    CPU_ZERO(&mask);
    CPU_SET(tid, &mask);
    sched_setaffinity(0, sizeof(mask), &mask);

	thread_init();
	
	__sync_fetch_and_add(&ready, 1);
	
	while(epoch == 0);

	clock_gettime(CLOCK_REALTIME, &start);
	lepoch = epoch;
	
	while(true) {

		
		{
			if(lbench == 1)
				RTMCompute();
			else if(lbench == 2)
				LockCompute();
			else if(lbench == 3)
				AtomicCompute();
			else if(lbench == 4)
				RawCompute();
			count++;	
		}

		if(lepoch < epoch) {
			clock_gettime(CLOCK_REALTIME, &end);
			int t = diff_timespec(end, start);
			printf("Thread [%d] Time %.2f s Throughput %.3f\n", 
						tid, t/1000.0, count*1.0/t);

			count = 0;
			clock_gettime(CLOCK_REALTIME, &start);
			lepoch = epoch;
			
		}

	}
	
}


int main(int argc, char** argv) {

	//Parse args
	for(int i = 1; i < argc; i++) {
		int n = 0;
		char junk;
		if (strcmp(argv[i], "--help") == 0){
			printf("./a.out -c=count number -t = thread number -r/l/a/n\n");
					return 1;
		}
		else if(sscanf(argv[i], "-c=%d%c", &n, &junk) == 1) {
			workingset = n;
		}
		else if(sscanf(argv[i], "-t=%d%c", &n, &junk) == 1) {
			thrnum = n;
		}else if(strcmp(argv[i], "-r") == 0) {
			bench = 1;
		}else if(strcmp(argv[i], "-l") == 0) {
			bench = 2;
		}else if(strcmp(argv[i], "-a") == 0) {
			bench = 3;
		}else if(strcmp(argv[i], "-n") == 0) {
			bench = 4;
		}
	}

	printf("Touch Work Set %d\n", workingset);
	
	pthread_t th[8];
	for(int i = 0; i < thrnum; i++)
		pthread_create(&th[i], NULL, thread_body, (void *)i);

	//Barriar to wait all threads become ready
	while (ready < 1);

	//Begin at the first epoch
	epoch = 1;
	
	while(true) {
		sleep(5);
		epoch++;
	}

	for(int i = 0; i < thrnum; i++)
		pthread_join(th[i], NULL);
	
	
	return 1;
}


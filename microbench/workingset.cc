#include <stdio.h>
#include <stdint.h>
#include <sys/mman.h>
#include <string.h>
#include <malloc.h>
#include <pthread.h>
#include <unistd.h>
#include <time.h>
#include <assert.h>
#include "rtmRegion.h"

typedef unsigned long uint64_t;

#define ARRAYSIZE 4*1024*1024/CASHELINESIZE //4M
#define CASHELINESIZE 64 //64 bytes

struct Cacheline {
	char data[CASHELINESIZE];
};

//critical data
char padding[64];
int workingset = 16 * 1024; //Default ws: 16KB
Cacheline *array;
char padding1[64];

volatile int ready = 0;
volatile int epoch = 0;

int tmpcount;
inline int Read(char * data) {
	int res = 0;
	for(int i = 0; i < workingset; i++) {
		res += (int)data[i];
	}
	return res;
}

inline void Write(char * data) {
	for(int i = 0; i < workingset; i++) {
		data[i]++;
	}
}

inline int ReadWrite(char* data) {
	int res = 0;
	for(int i = 0; i < workingset; i++) {
		if(i % 2)
			data[i]++;
		else
			res += (int)data[i];
	}
	return res;
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


void* thread_body(void *x) {

	RTMRegionProfile prof;
	int count = 0;
	int lepoch = 0;
	
	struct timespec start, end;
	
	__sync_fetch_and_add(&ready, 1);
	
	while(epoch == 0);

	clock_gettime(CLOCK_REALTIME, &start);
	lepoch = epoch;
	
	while(true) {

		
		{
			RTMRegion rtm(&prof);
			count += Read((char *)array);	
		}

		if(lepoch < epoch) {
			clock_gettime(CLOCK_REALTIME, &end);
			prof.ReportProfile();
			prof.Reset();
			printf("Time %.2f s\n", diff_timespec(end, start)/1000.0);
			printf("count %d\n", count);
			clock_gettime(CLOCK_REALTIME, &start);
			lepoch = epoch;
			
		}

	}

	prof.ReportProfile();
	
}


int main(int argc, char** argv) {

	//Parse args
	for(int i = 1; i < argc; i++) {
		int n = 0;
		char junk;
		if (strcmp(argv[i], "--help") == 0){
			printf("./a.out --ws=working set size (KB default:16KB)\n");
					return 1;
		}
		else if(sscanf(argv[i], "--ws=%d%c", &n, &junk) == 1) {
					workingset = n * 1024;
		}
	}

	printf("Touch Work Set %d\n", workingset);
	
	//Allocate the array at heap
	array = (Cacheline *)malloc(ARRAYSIZE * sizeof(Cacheline));
	
	//Touch every byte to avoid page fault 
	memset(array, 1, ARRAYSIZE * sizeof(Cacheline)); 
	
	pthread_t th;
	pthread_create(&th, NULL, thread_body, NULL);

	//Barriar to wait all threads become ready
	while (ready < 1);

	//Begin at the first epoch
	epoch = 1;
	
	while(true) {
		sleep(5);
		epoch++;
	}
	pthread_join(th, NULL);
	
	return 1;
}


#include <stdio.h>
#include <stdint.h>
#include <sys/mman.h>
#include <string.h>
#include <malloc.h>
#include <pthread.h>
#include "rtmRegion.h"

typedef unsigned long uint64_t;

#define ARRAYSIZE 4*1024*1024/CASHELINESIZE //4M
#define CASHELINESIZE 64 //64 bytes

struct Cacheline {
	char data[CASHELINESIZE];
};

int workingset = 16 * 1024; //Default ws: 16KB
Cacheline *array;


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


void* thread_body(void *x) {
	RTMRegionProfile prof;
	int count = 0;
	for(int i = 0 ; i < 10000; i++) {
		RTMRegion rtm(&prof);
		count += Read((char *)array);
	}

	prof.ReportProfile();
	printf("Count %ld\n", count);
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
	pthread_join(th, NULL);
	
	return 1;
}


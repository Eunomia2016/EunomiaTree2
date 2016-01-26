#ifndef _NUMA_UTIL_H_
#define _NUMA_UTIL_H_

#include <numa.h>
#include <numaif.h>
#include <utmpx.h>
#include <time.h>
#include <sched.h>
#define BILLION 1000000000L
static int Numa_get_node(void* ptr) {
	int numa_node = -1;
	get_mempolicy(&numa_node, NULL, 0, ptr, MPOL_F_NODE | MPOL_F_ADDR);
	return numa_node;
}

static int Numa_current_node() {
	int cpu = sched_getcpu();
	int node = numa_node_of_cpu(cpu);
	return node;
}

static void* Numa_alloc_onnode(size_t size, int node){
	void * ptr = numa_alloc_onnode(size, node);
	if(ptr == NULL){
		fprintf(stderr, "numa_alloc_error\n");
	}
	return ptr;
}
static void Numa_free(void * start, size_t size){
	numa_free(start, size);
}
static long get_nanoseconds(struct timespec& begin, struct timespec& end) {
	return (end.tv_sec - begin.tv_sec) * BILLION + (end.tv_nsec - begin.tv_nsec);
}

#endif

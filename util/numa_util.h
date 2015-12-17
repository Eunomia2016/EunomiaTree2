#ifndef _NUMA_UTIL_H_
#define _NUMA_UTIL_H_

#include <numa.h>
#include <numaif.h>
#include <utmpx.h>
int get_numa_node(void* ptr){
	int numa_node = -1;
	get_mempolicy(&numa_node, NULL, 0, ptr, MPOL_F_NODE|MPOL_F_ADDR);
	return numa_node;
}

int get_current_node(){
	int cpu = sched_getcpu();
	int node = numa_node_of_cpu(cpu);
	return node;
}
#endif

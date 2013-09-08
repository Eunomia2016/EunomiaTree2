#include "port/atomic.h"
#include "rmqueue.h"
#include <assert.h>
#include <stdio.h>
#include <stdlib.h>

RMQueue::RMQueue()
{
	//the default size of the gc queue is 64
	qsize = 1024;
	head = tail = 0;
	queue = new RMElement*[qsize];
	need_del = 0;
	actual_del = 0;
	elems = 0;
	
}
	
RMQueue::~RMQueue()
{
	while(head != tail) {
		delete queue[head];
		head = (head + 1) % qsize;
	}

	delete[] queue;

}

void RMQueue::AddRMElement(Epoch* e, uint64_t** arr, int len)
{
	//the queue is empty
	elems++;
	queue[tail] = new RMElement(e, arr, len);
	
	if((tail + 1) % qsize == head) {
		printf("ERROR: RMQueue Over Flow %d\n", elems);
		printf("Cur \n");
		e->Print();
		printf("Queue \n");
		Print();
		exit(1);
	}
	tail = (tail + 1) % qsize;
	assert(tail != head);
	
#if RMTEST
	need_del++;
#endif	
}

void RMQueue::GC(Epoch* current)
{	
	while(head != tail && queue[head]->epoch->Compare(current) < 0) {
		delete queue[head];
		head = (head + 1) % qsize;
		elems--;
#if RMTEST
		actual_del++;
#endif
	}
	
}

void RMQueue::Print()
{
	int index = head;
	while(index != tail) {
		
		queue[index]->epoch->Print();
		
		index = (index + 1) % qsize;
	}
}
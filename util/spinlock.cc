#include "port/atomic.h"
#include "util/spinlock.h"
#include <assert.h>
#include <stdio.h>


/* For various reader-writer lock algorithms, refer to
 * http://www.cs.rochester.edu/research/synchronization/pseudocode/rw.html
 * Current Linux rwlock is reader preference. */
 
/* The following implementation is based on the description of Intel TBB's
 * almost fair rwlock.
 * http://software.intel.com/en-us/blogs/2009/04/02/shared-access-with-tbb-readwrite-locks/ */

namespace leveldb {
	

void SpinLock::Lock() {
    while (1) {
       if (!xchg16((uint16_t *)&lock, 1)) return;
   
       while (lock) cpu_relax();
   }
}

void SpinLock::Unlock() 
{
	barrier();
    lock = 0;
}


uint16_t SpinLock::Trylock()
{
	return xchg16((uint16_t *)&lock, 1);
}


}


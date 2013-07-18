#ifndef LEVELDB_SPINLOCK_H
#define LEVELDB_SPINLOCK_H

#include <stdint.h>

/* The counter should be initialized to be 0. */
namespace leveldb {

class SpinLock  {

public:
  //0: free, 1: busy
  volatile uint16_t lock;

public:

  SpinLock(){ lock = 0;}
  
  void Lock();
  void Unlock();

  inline uint16_t IsLocked(){return lock;}
  uint16_t Trylock();


};

}
#endif /* _RWLOCK_H */

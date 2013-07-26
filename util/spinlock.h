#ifndef LEVELDB_SPINLOCK_H
#define LEVELDB_SPINLOCK_H

#include <stdint.h>

/* The counter should be initialized to be 0. */
namespace leveldb {

class SpinLock  {

public:
  //0: free, 1: busy
  //occupy an exclusive cache line
  volatile uint8_t padding1[32];
  volatile uint16_t lock;
  volatile uint8_t padding2[32];
public:

  SpinLock(){ lock = 0;}
  
  void Lock();
  void Unlock();

  inline uint16_t IsLocked(){return lock;}
  uint16_t Trylock();


};

}
#endif /* _RWLOCK_H */

#ifndef MEMSTORE_H_
#define MEMSTORE_H_

class Memstore {

 public:
  
  struct MemNode
  {
	uint64_t counter;
	uint64_t seq;
	uint64_t* value;
	MemNode* oldVersions;
  };


  class Iterator {
   public:
    // Initialize an iterator over the specified list.
    // The returned iterator is not valid.
    Iterator() { assert(0);}

    // Returns true iff the iterator is positioned at a valid node.
    virtual bool Valid(){ assert(0);}

    // Returns the key at the current position.
    // REQUIRES: Valid()
   virtual  MemNode* CurNode(){ assert(0);}

    // Advances to the next position.
    // REQUIRES: Valid()
    virtual void Next(){ assert(0);}

    // Advances to the previous position.
    // REQUIRES: Valid()
    virtual void Prev(){ assert(0);}

    // Advance to the first entry with a key >= target
    virtual void Seek(uint64_t key){ assert(0);}

	virtual void SeekPrev(uint64_t key) { assert(0);}

    // Position at the first entry in list.
    // Final state of iterator is Valid() iff list is not empty.
    virtual void SeekToFirst() { assert(0);}

    // Position at the last entry in list.
    // Final state of iterator is Valid() iff list is not empty.
    virtual void SeekToLast() { assert(0);}

  };

 public:

  Memstore(){};
  ~Memstore(){};

  //Only for initialization
  virtual void Put(uint64_t k, uint64_t* val) { assert(0); }
  
  virtual MemNode* GetWithInsert(uint64_t key) = 0;
  
  virtual void PrintStore() { assert(0); }
  
  virtual void ThreadLocalInit() { assert(0); }

  static MemNode* GetMemNode()
  {
	MemNode* mn = new Memstore::MemNode();
	mn->seq = 0;
	mn->value = 0;
	mn->counter = 0;
	mn->oldVersions = NULL;
	return mn;
  }
  
  
};


#endif

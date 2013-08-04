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
    Iterator() {}

	virtual bool Valid() = 0;

    // Returns the key at the current position.
    // REQUIRES: Valid()
    virtual MemNode* CurNode() = 0;

	virtual uint64_t Key() = 0;

    // Advances to the next position.
    // REQUIRES: Valid()
    virtual void Next() = 0;

    // Advances to the previous position.
    // REQUIRES: Valid()
    virtual void Prev() = 0;

    // Advance to the first entry with a key >= target
    virtual void Seek(uint64_t key) = 0;

	virtual void SeekPrev(uint64_t key) = 0;

    // Position at the first entry in list.
    // Final state of iterator is Valid() iff list is not empty.
    virtual void SeekToFirst() = 0;

    // Position at the last entry in list.
    // Final state of iterator is Valid() iff list is not empty.
    virtual void SeekToLast() = 0;

	virtual uint64_t* GetLink() = 0;

	virtual uint64_t GetLinkTarget() = 0;

  };

 public:

  Memstore(){};
  ~Memstore(){};

  //Only for initialization

  virtual Memstore::Iterator* GetIterator() = 0;
  
  virtual void Put(uint64_t k, uint64_t* val) = 0;

  virtual MemNode* Get(uint64_t key) = 0;
  
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
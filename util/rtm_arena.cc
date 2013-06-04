// Copyright (c) 2011 The LevelDB Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file. See the AUTHORS file for names of contributors.

#include "util/rtm_arena.h"
#include <assert.h>
#include <stdio.h>

namespace leveldb {

static const int kBlockSize = 4096;

RTMArena::RTMArena() {
  blocks_memory_ = 0;
  alloc_ptr_ = NULL;  // First allocation will allocate a block
  alloc_bytes_remaining_ = 0;
  for(int i = 0; i < 64; i ++)
  	cacheset[i] = 0;

  cachelineaddr = 0;
}

RTMArena::~RTMArena() {
  for (size_t i = 0; i < blocks_.size(); i++) {
  	//printf("Free %lx\n", blocks_[i]);
    delete[] blocks_[i];
  }
  /*
  for(int i = 0; i < 64; i ++)
  	printf("cacheset[%d] %d ", i, cacheset[i]);

  printf("\n");*/
}

char* RTMArena::AllocateFallback(size_t bytes) {
  if (bytes > kBlockSize / 4) {
    // Object is more than a quarter of our block size.  Allocate it separately
    // to avoid wasting too much space in leftover bytes.
    char* result = AllocateNewBlock(bytes);
    return result;
  }

  // We waste the remaining space in the current block.
  alloc_ptr_ = AllocateNewBlock(kBlockSize);
  alloc_bytes_remaining_ = kBlockSize;

  char* result = alloc_ptr_;
  alloc_ptr_ += bytes;
  alloc_bytes_remaining_ -= bytes;
  return result;
}

char* RTMArena::AllocateAligned(size_t bytes) {
	//printf("Alloca size %d\n", bytes);
  const int align = 64;    // We'll align to cache line
  assert((align & (align-1)) == 0);   // Pointer size should be a power of 2
  size_t current_mod = reinterpret_cast<uintptr_t>(alloc_ptr_) & (align-1);
  size_t slop = (current_mod == 0 ? 0 : align - current_mod);
  size_t needed = bytes + slop;
  char* result;
  if (needed <= alloc_bytes_remaining_) {
    result = alloc_ptr_ + slop;
    alloc_ptr_ += needed;
    alloc_bytes_remaining_ -= needed;
  } else {
    // AllocateFallback always returned aligned memory
    result = AllocateFallback(bytes);
  }
  
/*
  if( cachelineaddr != (uint64_t)result >> 6) {
  	cachelineaddr = (uint64_t)result >> 6;
  	int index = (int)(((uint64_t)result>>6)&0x3f);
  	cacheset[index]++;
  }
  */
  assert((reinterpret_cast<uintptr_t>(result) & (align-1)) == 0);
  return result;
}

char* RTMArena::AllocateNewBlock(size_t block_bytes) {
  char* result = new char[block_bytes];
  //printf("Allocate %lx\n", result);
  blocks_memory_ += block_bytes;
  blocks_.push_back(result);
  return result;
}

}  // namespace leveldb

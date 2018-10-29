// Copyright (c) 2011 The LevelDB Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file. See the AUTHORS file for names of contributors.

#ifndef STORAGE_LEVELDB_UTIL_ARENA_H_
#define STORAGE_LEVELDB_UTIL_ARENA_H_

#include <vector>
#include <assert.h>
#include <stddef.h>
#include <stdint.h>
#include "port/port.h"

namespace leveldb {

// 通过 vector 维护着一个内存块列表
class Arena {
 public:
  Arena();
  ~Arena();

  // Return a pointer to a newly allocated memory block of "bytes" bytes.
  //
  // 从该 arena 返回一个指向大小为 bytes 的内存的指针。
  // bytes 必须大于 0，具体见实现。
  char* Allocate(size_t bytes);

  // Allocate memory with the normal alignment guarantees provided by malloc
  //
  // 返回一个对齐后的可用内存的地址。具体对齐逻辑见实现。
  char* AllocateAligned(size_t bytes);

  // Returns an estimate of the total memory usage of data allocated
  // by the arena.
  //
  // 返回该 arena 持有的全部内存的字节数的估计值（未采用同步设施）
  size_t MemoryUsage() const {
    return reinterpret_cast<uintptr_t>(memory_usage_.NoBarrier_Load());
  }

 private:
  char* AllocateFallback(size_t bytes);
  char* AllocateNewBlock(size_t block_bytes);

  // Allocation state
  char* alloc_ptr_; // 指向该 arena 当前空闲字节起始地址
  size_t alloc_bytes_remaining_; // 该 arena 当前空闲字节数

  // Array of new[] allocated memory blocks
  // 存放通过 new[] 分配的全部内存块
  std::vector<char*> blocks_;

  // Total memory usage of the arena.
  // 该 arena 持有的全部内存字节数
  port::AtomicPointer memory_usage_;

  // No copying allowed
  Arena(const Arena&);
  void operator=(const Arena&);
};

inline char* Arena::Allocate(size_t bytes) {
  // The semantics of what to return are a bit messy if we allow
  // 0-byte allocations, so we disallow them here (we don't need
  // them for our internal use).
  //
  // 如果我们允许分配 0 字节内存，那该方法返回什么的语义就有点乱，所以我们不允许返回 0 字节（我们
  // 内部也没这个需求）。
  assert(bytes > 0);
  if (bytes <= alloc_bytes_remaining_) { // arena 剩余内存够则直接分配
    char* result = alloc_ptr_;
    alloc_ptr_ += bytes;
    alloc_bytes_remaining_ -= bytes;
    return result;
  }
  // arena 可用内存不够了，为用户申请新内存
  return AllocateFallback(bytes);
}

}  // namespace leveldb

#endif  // STORAGE_LEVELDB_UTIL_ARENA_H_

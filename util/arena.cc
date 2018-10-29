// Copyright (c) 2011 The LevelDB Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file. See the AUTHORS file for names of contributors.

#include "util/arena.h"
#include <assert.h>

namespace leveldb {

// arena 中每个 block 为 4KB，正好为一个内存页面大小
static const int kBlockSize = 4096;

Arena::Arena() : memory_usage_(0) {
  alloc_ptr_ = nullptr;  // First allocation will allocate a block
  alloc_bytes_remaining_ = 0;
}

Arena::~Arena() {
  for (size_t i = 0; i < blocks_.size(); i++) {
    delete[] blocks_[i];
  }
}

// 当 arena 可用内存不够时调用该方法来申请新内存
char* Arena::AllocateFallback(size_t bytes) {
  // 如果申请的内存超过默认内存块大小的 1/4，则单独分配一个大小恰好为 bytes 大小的内存块；否则，
  // 单独分配一个默认大小（4KB）的内存块给用户，会造成浪费。新分配的内存块会被加入 arena 然后将其起始指针返回给用户。
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

// 按照 8 字节或者指针长度进行内存对齐，然后返回对齐后分配的内存起始地址
char* Arena::AllocateAligned(size_t bytes) {
  // 如果机器指针的对齐方式超过 8 字节则按照指针的对齐方式进行对齐；否则按照 8 字节进行对齐。
  const int align = (sizeof(void*) > 8) ? sizeof(void*) : 8;
  // 确保当按照指针大小对齐时，机器的指针大小是 2 的幂
  assert((align & (align-1)) == 0);   // Pointer size should be a power of 2
  // 求 alloc_ptr 与 align 的模运算的结果，
  // 以确认 alloc_ptr 是否恰好为 align 的整数倍。
  size_t current_mod = reinterpret_cast<uintptr_t>(alloc_ptr_) & (align-1);
  // 如果 alloc_ptr 的值恰好为 align 整数倍，则已经满足对齐要求，可以从该地址直接进行分配；
  // 否则，比如 align 为 8，alloc_ptr 等于 15，则需要将 alloc_ptr 增加 align - current_mod = 8 - 7 = 1 个字节。
  size_t slop = (current_mod == 0 ? 0 : align - current_mod);
  size_t needed = bytes + slop; // 虽然用户申请 bytes 个字节，但是因为对齐要求，实际消耗的内存大小为 bytes + slop
  char* result;
  if (needed <= alloc_bytes_remaining_) { // 如果 arena 空闲内存满足要求则直接分配
    result = alloc_ptr_ + slop; // 将 result 指向对齐后的地址
    alloc_ptr_ += needed;
    alloc_bytes_remaining_ -= needed;
  } else { // 否则从堆中申请新的内存块，注意重新分配内存块时 malloc 会保证对齐。
    // AllocateFallback always returned aligned memory
    result = AllocateFallback(bytes);
  }
  assert((reinterpret_cast<uintptr_t>(result) & (align-1)) == 0);
  return result;
}

// 分配一个大小为 block_bytes 的块并将其加入到 arena
char* Arena::AllocateNewBlock(size_t block_bytes) {
  char* result = new char[block_bytes];
  blocks_.push_back(result);
  memory_usage_.NoBarrier_Store(
      reinterpret_cast<void*>(MemoryUsage() + block_bytes + sizeof(char*)));
  return result;
}

}  // namespace leveldb

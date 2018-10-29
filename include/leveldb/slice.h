// Copyright (c) 2011 The LevelDB Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file. See the AUTHORS file for names of contributors.
//
// Slice is a simple structure containing a pointer into some external
// storage and a size.  The user of a Slice must ensure that the slice
// is not used after the corresponding external storage has been
// deallocated.
//
// Multiple threads can invoke const methods on a Slice without
// external synchronization, but if any of the threads may call a
// non-const method, all threads accessing the same Slice must use
// external synchronization.
/**
 * Slice 是 leveldb 自己定义的字符串类型，一个指向字符数组的指针 + 字符个数。
 *
 * 所用字符数组是 char 型的，编码需要使用者自己处理。
 *
 * 指向的字符数组不在 Slice 内部，而是使用者提供的一个外部存储，所以使用过程中要确保这块存储是有效的。
 *
 * Slice 类除了 const 方法，其他的方法都是非线程安全的，调用时需要自己加锁。
 */
#ifndef STORAGE_LEVELDB_INCLUDE_SLICE_H_
#define STORAGE_LEVELDB_INCLUDE_SLICE_H_

#include <assert.h>
#include <stddef.h>
#include <string.h>
#include <string>
#include "leveldb/export.h"

namespace leveldb {

class LEVELDB_EXPORT Slice {
 public:
  // Create an empty slice.
  Slice() : data_(""), size_(0) { }

  // Create a slice that refers to d[0,n-1].
  Slice(const char* d, size_t n) : data_(d), size_(n) { }

  // Create a slice that refers to the contents of "s"
  // 允许 std::string& 隐式地转换为 Slice 类型
  Slice(const std::string& s) : data_(s.data()), size_(s.size()) { }

  // Create a slice that refers to s[0,strlen(s)-1]
  Slice(const char* s) : data_(s), size_(strlen(s)) { }

  // Intentionally copyable.
  Slice(const Slice&) = default;
  Slice& operator=(const Slice&) = default;

  // Return a pointer to the beginning of the referenced data
  const char* data() const { return data_; }

  // Return the length (in bytes) of the referenced data
  size_t size() const { return size_; }

  // Return true iff the length of the referenced data is zero
  bool empty() const { return size_ == 0; }

  // Return the ith byte in the referenced data.
  // REQUIRES: n < size()
  char operator[](size_t n) const {
    assert(n < size());
    return data_[n];
  }

  // Change this slice to refer to an empty array
  void clear() { data_ = ""; size_ = 0; }

  // Drop the first "n" bytes from this slice.
  void remove_prefix(size_t n) {
    assert(n <= size());
    data_ += n;
    size_ -= n;
  }

  // Return a string that contains the copy of the referenced data.
  std::string ToString() const { return std::string(data_, size_); }

  // Three-way comparison.  Returns value:
  //   <  0 iff "*this" <  "b",
  //   == 0 iff "*this" == "b",
  //   >  0 iff "*this" >  "b"
  //
  // 原理同 std::string
  int compare(const Slice& b) const;

  // Return true iff "x" is a prefix of "*this"
  bool starts_with(const Slice& x) const {
    return ((size_ >= x.size_) &&
            (memcmp(data_, x.data_, x.size_) == 0));
  }

 private:
  const char* data_;
  size_t size_;
};

inline bool operator==(const Slice& x, const Slice& y) {
  return ((x.size() == y.size()) &&
          (memcmp(x.data(), y.data(), x.size()) == 0));
}

inline bool operator!=(const Slice& x, const Slice& y) {
  return !(x == y);
}

inline int Slice::compare(const Slice& b) const {
  const size_t min_len = (size_ < b.size_) ? size_ : b.size_;
  int r = memcmp(data_, b.data_, min_len); // 第一次比较先忽略长度，按照最短的那个长度，左端对齐逐字节按照字典序比较。
  if (r == 0) { // 谁长谁大
    if (size_ < b.size_) r = -1;
    else if (size_ > b.size_) r = +1;
  }
  return r;
}

}  // namespace leveldb


#endif  // STORAGE_LEVELDB_INCLUDE_SLICE_H_

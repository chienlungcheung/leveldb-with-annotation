// Copyright (c) 2011 The LevelDB Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file. See the AUTHORS file for names of contributors.

#ifndef STORAGE_LEVELDB_INCLUDE_COMPARATOR_H_
#define STORAGE_LEVELDB_INCLUDE_COMPARATOR_H_

#include <string>
#include "leveldb/export.h"

namespace leveldb {

class Slice;

// A Comparator object provides a total order across slices that are
// used as keys in an sstable or a database.  A Comparator implementation
// must be thread-safe since leveldb may invoke its methods concurrently
// from multiple threads.
/**
 * 一个 Comparator 对象为数据库中全部 keys 的提供了一个全序关系，keys 都是 Slice 类型。
 *
 * Comparator 实现必须是线程安全的，因为 leveldb 可能从不同的线程中并发地调用 Comparator 的各个方法。
 */
class LEVELDB_EXPORT Comparator {
 public:
  virtual ~Comparator();

  // Three-way comparison.  Returns value:
  //   < 0 iff "a" < "b", 当且仅当 "a" < "b" 返回负数
  //   == 0 iff "a" == "b", 当且仅当 "a" == "b" 返回 0
  //   > 0 iff "a" > "b" 当且仅当 "a" > "b" 返回正数
  virtual int Compare(const Slice& a, const Slice& b) const = 0;

  // The name of the comparator.  Used to check for comparator
  // mismatches (i.e., a DB created with one comparator is
  // accessed using a different comparator.
  //
  // The client of this package should switch to a new name whenever
  // the comparator implementation changes in a way that will cause
  // the relative ordering of any two keys to change.
  //
  // Names starting with "leveldb." are reserved and should not be used
  // by any clients of this package.
  //
  // 返回 comparator 的名字。用于检查 comparator 是否匹配（即，检查用 comparator A 创建的 DB 是否
  // 被 comparator B 访问了）。
  //
  // 不同排序逻辑的 comparator 应该有不一样的名字。
  //
  // 以 "leveldb." 开头的名字被 leveldb 保留使用，客户端不要采用这种命名方式。
  virtual const char* Name() const = 0;

  // Advanced functions: these are used to reduce the space requirements
  // for internal data structures like index blocks.
  // 下面两个是高级方法：他们用于减少内部数据结构如 index blocks 的空间使用。

  // If *start < limit, changes *start to a short string in [start,limit).
  // Simple comparator implementations may return with *start unchanged,
  // i.e., an implementation of this method that does nothing is correct.
  //
  // 如果 *start < limit，则把 *start 改为一个落在 [start, limit) 之间的短字符串，
  // 最终的长度更短但是值逻辑变大的 start 内容会被用于 index block 中的数据项的 key。
  // 简单的 comparator 实现可以不修改 *start，也就是说这个方法可以什么也不做保持 start 不变，因为
  // 这仍然满足限制条件 [start, limit)，左边是闭区间。
  //
  // 注意，该方法仅在设置 index block 非末尾 entry 的时候调用；如果是末尾，调用下面的 FindShortSuccessor。
  // 具体原因见 table_format.md 中对 index entry key 的描述。
  virtual void FindShortestSeparator(
      std::string* start,
      const Slice& limit) const = 0;

  // Changes *key to a short string >= *key.
  // Simple comparator implementations may return with *key unchanged,
  // i.e., an implementation of this method that does nothing is correct.
  //
  // 将 *key 改为一个长度大于等于自己的短字符串。
  // 简单的 comparator 实现可以不修改 *key，也就是说这个方法可以什么也不做。
  //
  // 注意，该方法仅在设置 index block 末尾 entry 的时候调用；如果是非末尾，调用 FindShortestSeparator。
  virtual void FindShortSuccessor(std::string* key) const = 0;
};

// Return a builtin comparator that uses lexicographic byte-wise
// ordering.  The result remains the property of this module and
// must not be deleted.
//
// 返回内置的 comparator，它使用逐字节的字典序来实现。返回的结果是 leveldb 内部属性，不要删除。
LEVELDB_EXPORT const Comparator* BytewiseComparator();

}  // namespace leveldb

#endif  // STORAGE_LEVELDB_INCLUDE_COMPARATOR_H_

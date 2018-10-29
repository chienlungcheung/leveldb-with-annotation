// Copyright (c) 2011 The LevelDB Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file. See the AUTHORS file for names of contributors.
//
// WriteBatch holds a collection of updates to apply atomically to a DB.
//
// The updates are applied in the order in which they are added
// to the WriteBatch.  For example, the value of "key" will be "v3"
// after the following batch is written:
//
//    batch.Put("key", "v1");
//    batch.Delete("key");
//    batch.Put("key", "v2");
//    batch.Put("key", "v3");
//
// Multiple threads can invoke const methods on a WriteBatch without
// external synchronization, but if any of the threads may call a
// non-const method, all threads accessing the same WriteBatch must use
// external synchronization.

/**
 * WriteBatch 包含了一组要原子化地应用到某个数据库上的更新操作。
 *
 * 这些更新操作会被按照他们被添加到 WriteBatch 的顺序执行。比如下面操作这个批量操作的结果是 v3：
 *
 *     batch.Put("key", "v1");
 *     batch.Delete("key");
 *     batch.Put("key", "v2");
 *     batch.Put("key", "v3");
 *
 * 该类的 const 方法是线程安全的，其它方法如果被多线程调用需要借助外部同步设施。
 */
#ifndef STORAGE_LEVELDB_INCLUDE_WRITE_BATCH_H_
#define STORAGE_LEVELDB_INCLUDE_WRITE_BATCH_H_

#include <string>
#include "leveldb/export.h"
#include "leveldb/status.h"

namespace leveldb {

class Slice;

class LEVELDB_EXPORT WriteBatch {
 public:
  WriteBatch();

  // Intentionally copyable.
  WriteBatch(const WriteBatch&) = default; // 默认拷贝构造
  WriteBatch& operator =(const WriteBatch&) = default; // 默认赋值构造

  ~WriteBatch();

  // Store the mapping "key->value" in the database.
  /**
   * 将 <key, value> 对写入数据库
   * @param key Slice 类型的 key，这个 key 就是 user_key 了。（leveldb 内部用的其实是 internal_key，由 user_key + tag 构成）
   * @param value Slice 类型的 value
   */
  void Put(const Slice& key, const Slice& value);

  // If the database contains a mapping for "key", erase it.  Else do nothing.
  /**
   * 如果数据库包含 key 则擦除之，否则什么也不做。
   * @param key 带擦除的的 key，Slice 类型
   */
  void Delete(const Slice& key);

  // Clear all updates buffered in this batch.
  /**
   * 将缓存到 batch 中的全部操作清空
   */
  void Clear();

  // The size of the database changes caused by this batch.
  //
  // This number is tied to implementation details, and may change across
  // releases. It is intended for LevelDB usage metrics.
  /**
   * 由该 batch 导致的数据库变化的大小。
   *
   * 具体值与实现细节有关，而且可能随着不同版本发布发生变化。该值用于 LevelDB 使用量度量指标。
   * @return
   */
  size_t ApproximateSize();

  // Copies the operations in "source" to this batch.
  //
  // This runs in O(source size) time. However, the constant factor is better
  // than calling Iterate() over the source batch with a Handler that replicates
  // the operations into this batch.
  /**
   * 将 source 中的操作追加到本 batch。
   *
   * 该方法时间复杂度为 O(source size)，但复杂度的常数因子要优于在 source 上调用 Iterate() 同时用 Handler 挨个进行拷贝。
   * @param source
   */
  void Append(const WriteBatch& source);

  // Support for iterating over the contents of a batch.
  /**
   * 内部类。
   * 迭代一个 batch 并应用其包含的各个操作时使用.
   * leveldb 实现了一个基于 memtable 的 handler， MemTableInserter,
   * 它的 Put、Delete 都是基于底层的 memtable 实现的，最终效果就是在 memtable 上施行 Put、Delete。
   */
  class Handler {
   public:
    virtual ~Handler();
    virtual void Put(const Slice& key, const Slice& value) = 0;
    virtual void Delete(const Slice& key) = 0;
  };
  Status Iterate(Handler* handler) const;

 private:
  friend class WriteBatchInternal; // 该类为 WriteBatch 的友元类，而且无状态，只提供静态方法用于操作 WriteBatch 里面的数据

  // WriteBatch::rep_ 构成：
  //   std::string 类型，由 8 字节序列号 + 4 字节计数值 + 计数值指定个数的 records 构成
  //
  // record 构成：
  //   kTypeValue（1 个字节） + key（varstring 类型，可变长度字符串） + value（varstring 类型，可变长度字符串）
  //      这个 record 表示插入了一个新的键值对。
  //
  //   或者
  //
  //   kTypeDeletion（1 个字节） + key（varstring 类型，可变长度字符串），
  //      这个 record 表示对应的 key 被删除了（leveldb 的删除也是一个插入操作，只不过没有 value）
  //
  // varstring 构成：
  //   len（varint32(1~5字节） 或 varint64(1~9字节）类型，而且每个字节只有低 7 位才是有效数据部分，最高位为标识位，标识是否有后续字节）
  //   + data（长度为 len）
  std::string rep_;  // See comment in write_batch.cc for the format of rep_
};

}  // namespace leveldb

#endif  // STORAGE_LEVELDB_INCLUDE_WRITE_BATCH_H_

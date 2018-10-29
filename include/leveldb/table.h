// Copyright (c) 2011 The LevelDB Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file. See the AUTHORS file for names of contributors.

#ifndef STORAGE_LEVELDB_INCLUDE_TABLE_H_
#define STORAGE_LEVELDB_INCLUDE_TABLE_H_

#include <stdint.h>
#include "leveldb/export.h"
#include "leveldb/iterator.h"

namespace leveldb {

class Block;
class BlockHandle;
class Footer;
struct Options;
class RandomAccessFile;
struct ReadOptions;
class TableCache;

// A Table is a sorted map from strings to strings.  Tables are
// immutable and persistent.  A Table may be safely accessed from
// multiple threads without external synchronization.
//
// Table 就是一个 strings 到 strings 的有序 map。
// Table 是不可变且持久化的。
// Table 可以被多个线程在不依赖外部同步设施的情况下安全地访问。
class LEVELDB_EXPORT Table {
 public:
  // Attempt to open the table that is stored in bytes [0..file_size)
  // of "file", and read the metadata entries necessary to allow
  // retrieving data from the table.
  //
  // If successful, returns ok and sets "*table" to the newly opened
  // table.  The client should delete "*table" when no longer needed.
  // If there was an error while initializing the table, sets "*table"
  // to nullptr and returns a non-ok status.  Does not take ownership of
  // "*source", but the client must ensure that "source" remains live
  // for the duration of the returned table's lifetime.
  //
  // *file must remain live while this Table is in use.
  //
  // 打开一个保存在 file 的 [0..file_size) 的有序 table，并读取必要的 metadata 数据项
  // 以允许从该 table 检索数据。
  //
  // 如果成功，返回 OK 并将 *table 设置为新打开的 table。当不再使用该 table 时候，客户端负责删除之。
  // 如果在初始化 table 出错，将 *table 设置为 nullptr 并返回 non-OK。
  // 而且，在 table 打开期间，客户端要确保数据源持续有效。
  //
  // 当 table 在使用过程中，*file 必须保持有效。
  static Status Open(const Options& options,
                     RandomAccessFile* file,
                     uint64_t file_size,
                     Table** table);

  Table(const Table&) = delete;
  void operator=(const Table&) = delete;

  ~Table();

  // Returns a new iterator over the table contents.
  // The result of NewIterator() is initially invalid (caller must
  // call one of the Seek methods on the iterator before using it).
  //
  // 返回一个基于该 table 的迭代器。
  // 该方法返回的结果默认是无效的（在使用该迭代器之前，
  // 调用者必须在返回的迭代器上调用其中一个 Seek 方法）
  Iterator* NewIterator(const ReadOptions&) const;

  // Given a key, return an approximate byte offset in the file where
  // the data for that key begins (or would begin if the key were
  // present in the file).  The returned value is in terms of file
  // bytes, and so includes effects like compression of the underlying data.
  // E.g., the approximate offset of the last key in the table will
  // be close to the file length.
  //
  // 根据参数 key，返回一个与 key 对应的数据项起始地址在文件中的大致偏移量，（如果数据项
  // 不存在，那么返回的就是如果它存在时所处的大致位置）。
  uint64_t ApproximateOffsetOf(const Slice& key) const;

 private:
  struct Rep;
  Rep* rep_;

  explicit Table(Rep* rep) { rep_ = rep; }
  static Iterator* BlockReader(void*, const ReadOptions&, const Slice&);

  // Calls (*handle_result)(arg, ...) with the entry found after a call
  // to Seek(key).  May not make such a call if filter policy says
  // that key is not present.
  //
  // 如果调用 Seek(key) 找到某个数据项后自动调用 (*handle_result)(arg, ...)。
  friend class TableCache;
  Status InternalGet(
      const ReadOptions&, const Slice& key,
      void* arg,
      void (*handle_result)(void* arg, const Slice& k, const Slice& v));


  void ReadMeta(const Footer& footer);
  void ReadFilter(const Slice& filter_handle_value);
};

}  // namespace leveldb

#endif  // STORAGE_LEVELDB_INCLUDE_TABLE_H_

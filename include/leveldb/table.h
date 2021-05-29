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

// Table 是 sstable 文件反序列化后的内存形式, 包括 
// data blocks, data-index block, filter block 等.
//
// Table 就是一个 strings 到 strings 的有序 map. 
// Table 是不可变且持久化的. 
// Table 可以被多个线程在不依赖外部同步设施的情况下安全地访问. 
class LEVELDB_EXPORT Table {
 public:
  // *file must remain live while this Table is in use.
  //
  // 打开一个保存在 file 中 [0..file_size) 里的
  // 有序 table, 并读取必要的 metadata 数据项
  // 以从该 table 检索数据. 
  //
  // 如果成功, 返回 OK 并将 *table 设置为新打开
  // 的 table. 当不再使用该 table 时候, 客户端负责删除之. 
  // 如果在初始化 table 出错, 将 *table 设置
  // 为 nullptr 并返回 non-OK. 
  // 而且, 在 table 打开期间, 客户端要确保数据源持续有效,
  // 即当 table 在使用过程中, *file 必须保持有效. 
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
  // 返回一个基于该 table 内容的迭代器. 
  // 该方法返回的结果默认是无效的(在使用该迭代器之前, 
  // 调用者在使用前必须调用其中一个 Seek 方法来
  // 使迭代器生效.)
  Iterator* NewIterator(const ReadOptions&) const;

  // Given a key, return an approximate byte offset in the file where
  // the data for that key begins (or would begin if the key were
  // present in the file).  The returned value is in terms of file
  // bytes, and so includes effects like compression of the underlying data.
  // E.g., the approximate offset of the last key in the table will
  // be close to the file length.
  //
  // 返回 key 对应的数据项在文件中的大致起始地址(如果数据项不存在, 
  // 那么返回的就是如果它存在时所处的大致位置). 
  uint64_t ApproximateOffsetOf(const Slice& key) const;

 private:
  struct Rep;
  Rep* rep_;

  explicit Table(Rep* rep) { rep_ = rep; }
  static Iterator* BlockReader(void*, const ReadOptions&, const Slice&);

  // Seek(key) 找到某个数据项则会自动
  // 调用 (*handle_result)(arg, ...);
  // 如果过滤器明确表示不能做则不会调用.
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

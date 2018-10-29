// Copyright (c) 2011 The LevelDB Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file. See the AUTHORS file for names of contributors.

#ifndef STORAGE_LEVELDB_DB_WRITE_BATCH_INTERNAL_H_
#define STORAGE_LEVELDB_DB_WRITE_BATCH_INTERNAL_H_

#include "db/dbformat.h"
#include "leveldb/write_batch.h"

namespace leveldb {

class MemTable;

// WriteBatchInternal provides static methods for manipulating a
// WriteBatch that we don't want in the public WriteBatch interface.
//
// 该类为 WriteBatch 的友元类，而且无状态，只提供静态方法用于操作 WriteBatch 里面的数据。
class WriteBatchInternal {
 public:
  // Return the number of entries in the batch.
  //
  // 返回 batch 里的操作记录数目
  static int Count(const WriteBatch* batch);

  // Set the count for the number of entries in the batch.
  //
  //　设置　batch　里的操作记录书目为　n
  static void SetCount(WriteBatch* batch, int n);

  // Return the sequence number for the start of this batch.
  //
  // 返回 batch 的序列号
  static SequenceNumber Sequence(const WriteBatch* batch);

  // Store the specified number as the sequence number for the start of
  // this batch.
  //
  // 为 batch 指定序列号为 seq
  static void SetSequence(WriteBatch* batch, SequenceNumber seq);

  /**
   * 返回 batch 的全部操作记录
   * @param batch
   * @return
   */
  static Slice Contents(const WriteBatch* batch) {
    return Slice(batch->rep_);
  }

  /**
   * 返回 batch 所存储内容的字节数
   * @param batch
   * @return
   */
  static size_t ByteSize(const WriteBatch* batch) {
    return batch->rep_.size();
  }

  // 将 contents 存储的操作内容赋值给 batch b
  static void SetContents(WriteBatch* batch, const Slice& contents);

  // 将 b 中包含的操作应用到 memtable 中
  static Status InsertInto(const WriteBatch* batch, MemTable* memtable);

  // 将 src 中除了 header 以外内容追加到 dst 中
  static void Append(WriteBatch* dst, const WriteBatch* src);
};

}  // namespace leveldb


#endif  // STORAGE_LEVELDB_DB_WRITE_BATCH_INTERNAL_H_

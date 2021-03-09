// Copyright (c) 2011 The LevelDB Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file. See the AUTHORS file for names of contributors.
//
// TableBuilder provides the interface used to build a Table
// (an immutable and sorted map from keys to values).
//
// Multiple threads can invoke const methods on a TableBuilder without
// external synchronization, but if any of the threads may call a
// non-const method, all threads accessing the same TableBuilder must use
// external synchronization.

#ifndef STORAGE_LEVELDB_INCLUDE_TABLE_BUILDER_H_
#define STORAGE_LEVELDB_INCLUDE_TABLE_BUILDER_H_

#include <stdint.h>
#include "leveldb/export.h"
#include "leveldb/options.h"
#include "leveldb/status.h"

namespace leveldb {

class BlockBuilder;
class BlockHandle;
class WritableFile;

// 该类用于构造 Table(一个不可变且有序的 map). 
//
// 如果用户从多个线程调用该类的 const 方法, 线程安全; 
// 如果从多个线程调用非 const 方法, 则需要依赖外部同步设施确保线程安全. 
class LEVELDB_EXPORT TableBuilder {
 public:
  // Create a builder that will store the contents of the table it is
  // building in *file.  Does not close the file.  It is up to the
  // caller to close the file after calling Finish().
  //
  // TableBuilder 会把 table 的内容写入文件, 而且不会关闭文件直到用户调用 Finish(). 
  TableBuilder(const Options& options, WritableFile* file);

  TableBuilder(const TableBuilder&) = delete;
  void operator=(const TableBuilder&) = delete;

  // REQUIRES: Either Finish() or Abandon() has been called.
  //
  // 析构之前必须调用了 Finish() 或 Abandon(). 
  ~TableBuilder();

  // Change the options used by this builder.  Note: only some of the
  // option fields can be changed after construction.  If a field is
  // not allowed to change dynamically and its value in the structure
  // passed to the constructor is different from its value in the
  // structure passed to this method, this method will return an error
  // without changing any fields.
  //
  // 修改该 TableBuilder 使用的 options. 
  // 注意 TableBuilder 构造之后, 一些配置就无法动态修改了, 
  // 针对这些无法动态修改的配置, 如果用户在该方法进行传入的配置与
  // 构造时传入的不一样, 则该方法会报错而且不会更改这些无法动态修改的配置. 
  Status ChangeOptions(const Options& options);

  // Add key,value to the table being constructed.
  // REQUIRES: key is after any previously added key according to comparator.
  // REQUIRES: Finish(), Abandon() have not been called
  //
  // 将一对 key,value 传入正在构造的 table. 
  // 要求 1: key 必须大于任何之前已经添加过的 key
  // 要求 2: 还没调用过 Finish() 或者 Abandon()
  void Add(const Slice& key, const Slice& value);

  // Advanced operation: flush any buffered key/value pairs to file.
  // Can be used to ensure that two adjacent entries never live in
  // the same data block.  Most clients should not need to use this method.
  // REQUIRES: Finish(), Abandon() have not been called
  //
  // 高级操作: 将缓冲的全部 key,value 对写到文件. 
  // 该方法可以用于实现不将临近的两个数据项写入同一个 data block. 
  // 大部分客户端不需要调用该方法. 
  // 要求: 还没调用过 Finish() 或者 Abandon(). 
  void Flush();

  // Return non-ok iff some error has been detected.
  //
  // 当且仅当检测到错误的时候返回 non-ok. 
  Status status() const;

  // Finish building the table.  Stops using the file passed to the
  // constructor after this function returns.
  // REQUIRES: Finish(), Abandon() have not been called
  //
  // 完成 table 构建. 该方法返回后停止使用在构造方法中传入的文件. 
  // 要求: 还没调用过 Finish() 或者 Abandon(). 
  // table 构成: data blocks, filter block, metaindex block, index block
  Status Finish();

  // Indicate that the contents of this builder should be abandoned.  Stops
  // using the file passed to the constructor after this function returns.
  // If the caller is not going to call Finish(), it must call Abandon()
  // before destroying this builder.
  // REQUIRES: Finish(), Abandon() have not been called
  //
  // 指示该 TableBuilder 对应的内容应该丢弃. 该方法返回后停止使用在构造方法中传入的文件. 
  // 如果调用者不打算调用 Finish() 则在销毁该 TableBuilder 之前必须调用 Abandon. 
  // 要求: 还没调用过 Finish() 或者 Abandon(). 
  void Abandon();

  // Number of calls to Add() so far.
  //
  // 目前为止调用 Add() 的次数
  uint64_t NumEntries() const;

  // Size of the file generated so far.  If invoked after a successful
  // Finish() call, returns the size of the final generated file.
  //
  // 目前为止生成的文件的大小. 如果成功调用了 Finish(), 则返回最终生成文件的大小. 
  uint64_t FileSize() const;

 private:
  bool ok() const { return status().ok(); }
  void WriteBlock(BlockBuilder* block, BlockHandle* handle);
  void WriteRawBlock(const Slice& data, CompressionType, BlockHandle* handle);

  struct Rep;
  Rep* rep_;
};

}  // namespace leveldb

#endif  // STORAGE_LEVELDB_INCLUDE_TABLE_BUILDER_H_

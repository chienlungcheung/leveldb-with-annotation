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

// 该类用于构造 sstable(sorted string table) 文件. 
//
// 如果用户从多个线程调用该类的 const 方法, 线程安全; 
// 如果从多个线程调用非 const 方法, 则需要依赖外部同步设施确保线程安全. 
class LEVELDB_EXPORT TableBuilder {
 public:
  // TableBuilder 会把 table 的内容写入文件, 
  // 而且不会关闭文件直到用户调用 Finish(). 
  TableBuilder(const Options& options, WritableFile* file);

  TableBuilder(const TableBuilder&) = delete;
  void operator=(const TableBuilder&) = delete;

  // 析构之前必须调用了 Finish() 或 Abandon(). 
  ~TableBuilder();

  // 修改该 TableBuilder 使用的 options. 
  // 注意 TableBuilder 构造之后, 一些配置就无法动态修改了, 
  // 针对这些无法动态修改的配置, 如果用户在该方法进行传入的配置与
  // 构造时传入的不一样, 则该方法会报错而且不会更改这些无法动态修改的配置. 
  Status ChangeOptions(const Options& options);

  // 将一对 <key,value> 追加到正在构造的 table 中. 
  // 该方法追加数据时会同时影响到 data block, data index block, 
  // meta block 的构造.
  // 要求 1: key 必须大于任何之前已经添加过的 keys, 
  //        因为该文件是有序的.
  // 要求 2: 还没调用过 Finish() 或者 Abandon(), 
  //        调用了这两个方法表示 table 对应文件被关掉了.
  void Add(const Slice& key, const Slice& value);

  // 高级操作: 将缓冲的全部 key,value 对写到文件. 
  // 该方法可以用于确保不将临近的两个数据项写入同一个 data block. 
  // 大部分客户端不需要调用该方法. 
  // 要求: 还没调用过 Finish() 或者 Abandon(). 
  void Flush();

  // 当且仅当检测到错误的时候返回 non-ok. 
  Status status() const;

  // 完成 table 构建. 该方法返回后停止使用在构造方法中传入的文件. 
  // 要求: 还没调用过 Finish() 或者 Abandon(). 
  // table 构成: data blocks, filter block, metaindex block, index block
  Status Finish();

  // 指示该 TableBuilder 对应的内容应该丢弃. 
  // 该方法返回后停止使用在构造方法中传入的文件. 
  // 如果调用者不打算调用 Finish() 则在销毁该 
  // TableBuilder 之前必须调用 Abandon. 
  // 要求: 还没调用过 Finish() 或者 Abandon(). 
  void Abandon();

  // 目前为止调用 Add() 的次数, 这个次数也是
  // 追加的数据项的个数.
  uint64_t NumEntries() const;

  // 目前为止生成的文件的大小. 
  // 如果成功调用了 Finish(), 则返回最终生成文件的大小. 
  uint64_t FileSize() const;

 private:
  bool ok() const { return status().ok(); }
  // 将 block 内容根据设置进行压缩, 然后写入文件;
  // 同时将 block 在 table 偏移量和 size 设置到
  // handle 中, 写完 block 会将其 handle 写入
  // index block.
  void WriteBlock(BlockBuilder* block, BlockHandle* handle);
  // 将 block 及其 trailer(注意这个 trailer 不是 block 内部的 trailer)
  // 写入 table 对应的文件, 
  // 并将 block 对应的 BlockHandle 内容保存到 handle 中, 同时计算
  // 下个 block 在 table 中的偏移量. 
  // 写失败时该方法只将错误状态记录到 r->status, 不做其它任何处理.
  void WriteRawBlock(const Slice& data, CompressionType, BlockHandle* handle);

  struct Rep;
  Rep* rep_;
};

}  // namespace leveldb

#endif  // STORAGE_LEVELDB_INCLUDE_TABLE_BUILDER_H_

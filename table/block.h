// Copyright (c) 2011 The LevelDB Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file. See the AUTHORS file for names of contributors.

#ifndef STORAGE_LEVELDB_TABLE_BLOCK_H_
#define STORAGE_LEVELDB_TABLE_BLOCK_H_

#include <stddef.h>
#include <stdint.h>
#include "leveldb/iterator.h"

namespace leveldb {

struct BlockContents;
class Comparator;
// Block 布局如下: 
// 0. 每个 block 包含的数据有"数据项 + restart array + restart number"
//
// 1. block 中每个数据项的格式如下: 
// - shared_bytes: varint32(与前一个 key 共享的前缀的长度, varint32 类型)
// - unshared_bytes: varint32(当前 key 除去共享前缀后的长度, varint32 类型)
// - value_length: varint32(当前 key 对应的 value 的长度, varint32 类型)
// - key_delta: char[unshared_bytes](当前 key 除去共享前缀后的字节内容)
// - value: char[value_length](当前 key 对应的 value 的数据内容)
// 注意, 如果该数据项位于 restart 处, 则 shared_bytes 等于 0.
// (在构建 block 的时候会每隔一段设置一个 restart point, 
// 位于 restart point 的数据项的 key 不会进行前缀压缩, 此项之后
// 的数据项会相对于前一个数据项进行前缀压缩直至下一个 restart  point.) 
//
// 2. block 结尾处有个 trailer, 格式如下: 
// - restarts: uint32[num_restarts](保存 restart points 在 block 内偏移量的数组)
// - num_restarts: uint32(restart points 偏移量数组大小)
// restarts[i] 保存的是第 i 个 restart point 在 block 内的偏移量. 

class Block {
 public:
  // 使用特定的 contents 来构造一个 Block
  explicit Block(const BlockContents& contents);

  ~Block();

  size_t size() const { return size_; }
  // 根据用户定制的 comparator 构造该 block 的一个迭代器
  Iterator* NewIterator(const Comparator* comparator);

 private:
  uint32_t NumRestarts() const;

  // block 全部数据(数据项 + restart array + restart number)
  const char* data_;
  // block 总大小 
  size_t size_; 
  // block 的 restart array 在 block 中的起始偏移量
  uint32_t restart_offset_;
  // 如果 data_ 指向的空间是在堆上分配的, 
  // 那么该 block 对象销毁时需要释放该处空间, 该成员使用见析构方法.
  bool owned_;                  

  // 不允许拷贝 block 
  Block(const Block&);
  void operator=(const Block&);

  // 为迭代 block 内容服务的迭代器, block 相当于迭代器的数据源.
  class Iter;
};

}  // namespace leveldb

#endif  // STORAGE_LEVELDB_TABLE_BLOCK_H_

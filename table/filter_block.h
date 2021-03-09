// Copyright (c) 2012 The LevelDB Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file. See the AUTHORS file for names of contributors.
//
// A filter block is stored near the end of a Table file.  It contains
// filters (e.g., bloom filters) for all data blocks in the table combined
// into a single filter block.

#ifndef STORAGE_LEVELDB_TABLE_FILTER_BLOCK_H_
#define STORAGE_LEVELDB_TABLE_FILTER_BLOCK_H_

#include <stddef.h>
#include <stdint.h>
#include <string>
#include <vector>
#include "leveldb/slice.h"
#include "util/hash.h"

namespace leveldb {

class FilterPolicy;

// A FilterBlockBuilder is used to construct all of the filters for a
// particular Table.  It generates a single string which is stored as
// a special block in the Table.
//
// The sequence of calls to FilterBlockBuilder must match the regexp:
//      (StartBlock AddKey*)* Finish
//
// FilterBlockBuilder 用于构造 table 的全部 filters. 
// 最后生成一个字符串保存在 Table 的一个 meta block 中. 
//
// 该类方法调用序列必须满足下面的正则表达式: 
//      (StartBlock AddKey*)* Finish
// 最少调用一次 Finish, 而且 AddKey 和 Finish 之间不能插入 StartBlock 调用. 
class FilterBlockBuilder {
 public:
  explicit FilterBlockBuilder(const FilterPolicy*);

  void StartBlock(uint64_t block_offset);
  void AddKey(const Slice& key);
  Slice Finish();

 private:
  void GenerateFilter();

  const FilterPolicy* policy_;
  // 调用 Add 追加的每个 key 都会被同时追加到这个字符串中(用于后续构造过滤器使用)
  std::string keys_;              // Flattened key contents
  // 每个被 Add 的 key 在 keys 字符串中的索引
  std::vector<size_t> start_;     // Starting index in keys_ of each key
  // 目前为止计算出来的 filter 数据
  std::string result_;            // Filter data computed so far
  // 临时变量, 用于 policy_->CreateFilter()
  std::vector<Slice> tmp_keys_;   // policy_->CreateFilter() argument 内容与 keys_ 一样不过是 Slice 类型, 专用于 policy_->CreateFilter()
  std::vector<uint32_t> filter_offsets_; // 每个 filter 对应的在 filter block 中的偏移量

  // No copying allowed
  FilterBlockBuilder(const FilterBlockBuilder&);
  void operator=(const FilterBlockBuilder&);
};

// 与 FilterBlockBuilder 相反, 将一个 filter block 解析出来, 
// 然后用来查询某个 key 是否在某个 block 中.
class FilterBlockReader {
 public:
  // 使用一个 FilterPolicy 和一个 filter block 构造一个 FilterBlockReader. 
  // 注意两个参数生命期非常关键, 因为直接存的地址, 它们不能先于此处构造的对象死掉. 
  FilterBlockReader(const FilterPolicy* policy, const Slice& contents);
  bool KeyMayMatch(uint64_t block_offset, const Slice& key);

 private:
  const FilterPolicy* policy_;
  // 指向 filter block 起始地址
  const char* data_;
  // 指向 filter block 尾部 offset array 的起始地址, 
  // 这也是 filter block 的末尾.
  const char* offset_; 
  // offset array 中的元素个数
  size_t num_;
  // base 的 log, 位于 filter block 最后一个字节, 
  // 具体见 .cc 文件 kFilterBaseLg 介绍.
  size_t base_lg_;
};

}

#endif  // STORAGE_LEVELDB_TABLE_FILTER_BLOCK_H_

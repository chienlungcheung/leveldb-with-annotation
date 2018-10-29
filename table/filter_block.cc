// Copyright (c) 2012 The LevelDB Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file. See the AUTHORS file for names of contributors.

#include "table/filter_block.h"

#include "leveldb/filter_policy.h"
#include "util/coding.h"

namespace leveldb {

// See doc/table_format.md for an explanation of the filter block format.

// Generate new filter every 2KB of data
// 具体见 table_format.md 中的 base
static const size_t kFilterBaseLg = 11;
static const size_t kFilterBase = 1 << kFilterBaseLg;

FilterBlockBuilder::FilterBlockBuilder(const FilterPolicy* policy)
    : policy_(policy) {
}

// 记录一个新的要生成的 filter 的起始偏移量到 filter_offsets_
void FilterBlockBuilder::StartBlock(uint64_t block_offset) {
  // 计算以 block_offset 为起始地址的 block 对应的 filter offset array 索引
  uint64_t filter_index = (block_offset / kFilterBase);
  assert(filter_index >= filter_offsets_.size());
  // filter 是一个接一个构造的，对应的索引数组也是对应着逐渐增长的，
  // 而非一次性构造好往里面填，毕竟不知道要生成多少个 filters
  while (filter_index > filter_offsets_.size()) {
    GenerateFilter();
  }
}

// 向 keys 中增加一个 key，同时将 key 在 keys 中偏移量保存到 start 向量中
void FilterBlockBuilder::AddKey(const Slice& key) {
  Slice k = key;
  start_.push_back(keys_.size());
  keys_.append(k.data(), k.size());
}

// 计算 filter offset array 并追加到 result_，最后 result_ 保存的就是一个完整的 filter block
Slice FilterBlockBuilder::Finish() {
  if (!start_.empty()) { // 说明还有 keys 没有计算对应的 filter
    GenerateFilter();
  }

  // Append array of per-filter offsets
  const uint32_t array_offset = result_.size();
  // 将每个 filter 内容在 result 中的起始偏移量编码追加到 result
  for (size_t i = 0; i < filter_offsets_.size(); i++) {
    PutFixed32(&result_, filter_offsets_[i]);
  }

  // 将 filter offset array 的起始偏移量编码追加到 result
  PutFixed32(&result_, array_offset);
  // 将 base 的 log 值追加到 result
  result_.push_back(kFilterBaseLg);  // Save encoding parameter in result
  // 到此为止，result 已经是一个完整的 filter block 了，将其封装为 Slice 后返回
  return Slice(result_);
}

// 为当前 keys_ 保存的全部 keys 生成一个 filter 并追加到 result_ 中。
void FilterBlockBuilder::GenerateFilter() {
  // key 的个数
  const size_t num_keys = start_.size();
  if (num_keys == 0) {
    // Fast path if there are no keys for this filter
    // 没有 key 需要计算 filter，则直接把计算的上一个的 filter 的结束地址
    // 作为本次要计算的 filter 的在 filter  block 中的起始偏移量。
    // 这么做的目的是方便后面在 Finish() 中直接获取每个 filter 对应的起始偏移量，然后编码到 filter block。
    filter_offsets_.push_back(result_.size());
    return;
  }

  // Make list of keys from flattened key structure
  // 将 keys 大小放到 start 中作为最后一个 key 的结束地址，这样每个 key 都能用同样算法进行处理而不会溢出
  start_.push_back(keys_.size());  // Simplify length computation 方便后面这个 start_[i+1] - start_[i] 计算
  tmp_keys_.resize(num_keys);
  for (size_t i = 0; i < num_keys; i++) {
    const char* base = keys_.data() + start_[i]; // 第 i 个 key 在 keys 中的起始地址
    size_t length = start_[i+1] - start_[i]; // 第 i 个 key 的长度
    tmp_keys_[i] = Slice(base, length); // 将第 i 个 key 封装成 Slice 并保存到 tmp_keys 用于后续计算 filter
  }

  // Generate filter for current set of keys and append to result_.
  // 将接下来马上要根据 tmp_keys_ 计算的 filter 在 result 中的偏移量保存到 filter_offsets_
  filter_offsets_.push_back(result_.size());
  // 根据 tmp_keys_ 计算 filter 并追加到 result
  policy_->CreateFilter(&tmp_keys_[0], static_cast<int>(num_keys), &result_);

  // 清空与 key 相关的存储以方便计算下个 filter 使用
  tmp_keys_.clear();
  keys_.clear();
  start_.clear();
}

// 使用一个 FilterPolicy 和一个 filter block 构造一个 FilterBlockReader。
// 注意两个参数生命期非常关键，因为直接存的地址，它们不能先于此处构造的对象死掉。
FilterBlockReader::FilterBlockReader(const FilterPolicy* policy,
                                     const Slice& contents)
    : policy_(policy),
      data_(nullptr),
      offset_(nullptr),
      num_(0),
      base_lg_(0) {
  size_t n = contents.size();
  // filter block 最后 5 个字节一次是，offset array 的起始偏移量（4 字节），base 的 log 值（1 字节）。
  if (n < 5) return;  // 1 byte for base_lg_ and 4 for start of offset array
  base_lg_ = contents[n-1]; // 读取 base log 值
  uint32_t last_word = DecodeFixed32(contents.data() + n - 5); // 读取 offset array 起始偏移量
  if (last_word > n - 5) return;
  data_ = contents.data(); // filter block 起始地址
  offset_ = data_ + last_word; // offset array 起始地址
  num_ = (n - 5 - last_word) / 4; // offset array 元素（每个占 4 字节）个数
}

// 通过过滤器查询 key 是否在以 block_offset 为起始地址的 block 中
bool FilterBlockReader::KeyMayMatch(uint64_t block_offset, const Slice& key) {
  // 计算 filter 的时候是每隔 base 大小为一个区间，每个区间对应一个 filter。
  // 起始偏移量落在对应区间内的 blocks，会将其全部 keys 作为整体输入计算一个 filter。
  // 下面这一步相当于除以 base 取商，从而得到 block_offset 对应的 filter offset array 数组索引。
  uint64_t index = block_offset >> base_lg_;
  if (index < num_) {
    // 计算第 i 个 filter 起止地址
    uint32_t start = DecodeFixed32(offset_ + index*4);
    // 这里不要担心溢出什么的，即使 index == N-1 得到的 limit 就是 offset array 起始地址，
    // 正好是 filter-N-1 结束地址下一个字节。
    uint32_t limit = DecodeFixed32(offset_ + index*4 + 4);
    if (start <= limit && limit <= static_cast<size_t>(offset_ - data_)) {
      Slice filter = Slice(data_ + start, limit - start);
      return policy_->KeyMayMatch(key, filter);
    } else if (start == limit) {
      // Empty filters do not match any keys
      return false;
    }
  }

  // 如果出错则认为 key 存在，这个是 false positive，符合预期。
  return true;  // Errors are treated as potential matches
}

}

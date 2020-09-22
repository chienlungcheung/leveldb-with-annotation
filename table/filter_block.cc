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

// 确认是否要为偏移量为 block_offset 的 block 生成新的 filters,
// 判断条件是如果其起始偏移超出当前 filter 覆盖的范围
// (即超出范围 [filter_offsets_.size() - 1 * 2KB, filter_offsets_.size() * 2KB - 1])
// 则说明需要为其构造新的 filter 了; 否则什么也不做.
void FilterBlockBuilder::StartBlock(uint64_t block_offset) {
  // 计算以 block_offset 为起始地址的 block 对应的 filter offset array 索引
  // 每两 KB 数据就要生成一个 filter, 如果 block size 超过 2KB, 则会生成多个 filters
  uint64_t filter_index = (block_offset / kFilterBase);
  assert(filter_index >= filter_offsets_.size());
  // filter 是一个接一个构造的, 对应的索引数组也是对应着逐渐增长的, 
  // 而非一次性构造好往里面填, 毕竟不知道要生成多少个 filters
  while (filter_index > filter_offsets_.size()) {
    // 下个 block 不能跟当前 block 共用 filter, 则为当前 block 创建 filter, 并清空
    // 相关状态以为下个 block 所用.
    // 这里虽然是个循环, 但是因为每次生成 filter 都会情况相关状态, 所以下个循环并不会再生成 filter 了.
    GenerateFilter();
  }
}

// 向 keys 中增加一个 key, 同时将 key 在 keys 中偏移量保存到 start 向量中
void FilterBlockBuilder::AddKey(const Slice& key) {
  Slice k = key;
  start_.push_back(keys_.size());
  keys_.append(k.data(), k.size());
}

// 计算 filter offset array 并追加到 result_, 最后 result_ 保存的就是一个完整的 filter block
Slice FilterBlockBuilder::Finish() {
  // 为最后一个 data block 计算对应的 filter
  if (!start_.empty()) { 
    GenerateFilter();
  }

  // Append array of per-filter offsets
  // result 中本来保存的是一个接一个 filters,
  // 因为下面马上会把每个 filter 在 result 中对应的的起始地址也追加到 result 中,
  // 所以这里提前记录全部 filters 的总字节数, 这个值也是第一个 filter 的偏移量在 result
  // 中的起始地址.
  const uint32_t array_offset = result_.size();
  // 将每个 filter 在 result 中的起始偏移量编码追加到 result
  for (size_t i = 0; i < filter_offsets_.size(); i++) {
    PutFixed32(&result_, filter_offsets_[i]);
  }

  // 将 filter offset array 的起始偏移量编码追加到 result, 用于反序列化时区分 filters 与 filters 偏移量数组
  PutFixed32(&result_, array_offset);
  // 将 base 的 log 值追加到 result, 用于反序列化时计算 base
  result_.push_back(kFilterBaseLg);  // Save encoding parameter in result
  // 到此为止, result 已经是一个完整的 filter block 了, 将其封装为 Slice 后返回
  return Slice(result_);
}

// 由于下个 block 不能与当前写满的 block 共用 filter 了, 
// 所以未当前写满的 block 对应的 keys 生成一个 filter.
// 同时会并清空相关状态以为下个 block 所用.
void FilterBlockBuilder::GenerateFilter() {
  // key 的个数
  const size_t num_keys = start_.size();
  if (num_keys == 0) {
    // Fast path if there are no keys for this filter
    // 没有 key 需要计算 filter, 则直接把计算的上一个的 filter 的结束地址
    // 作为本次要计算的 filter 的在 filter  block 中的起始偏移量. 
    // 这么做的目的是方便后面在 Finish() 中直接获取每个 filter 对应的起始偏移量, 
    // 然后编码到 filter block. 
    // 这里有个疑问: 
    //    比如第一个 block 大小为 4KB, 则其在 StartBlock() 中
    //    计算的 filter_index 为 2, 此时 filter_offsets_.size() 为 0, 则需要
    //    调用 GenerateFilter() 两次, 第一次不会进入这个判断, 第二次会进入, 因为 keys 都处理完了,
    //    那添加的这个偏移量与第二个 block (从 1 开始计数) filter 
    //    在 result 中的偏移量就重复记录了, 但对外看来是 3 个 filter, 与实际个数 2 并不匹配, 会不会出错呢?
    // 解答见下面的 leveldb::FilterBlockReader::KeyMayMatch 方法.
    filter_offsets_.push_back(result_.size());
    return;
  }

  // Make list of keys from flattened key structure
  // 将 keys 大小放到 start 中作为最后一个 key 的结束地址, 
  // 这样每个 key 都能用同样算法进行处理而不会溢出
  start_.push_back(keys_.size());  // Simplify length computation 方便后面这个 start_[i+1] - start_[i] 计算
  tmp_keys_.resize(num_keys);
  // 将字符串 keys 保存的每个 key 提取出来封装成 Slice 并放到 tmp_keys 中
  for (size_t i = 0; i < num_keys; i++) {
    // 第 i 个 key 在 keys 中的起始地址
    const char* base = keys_.data() + start_[i]; 
    // 第 i 个 key 的长度
    size_t length = start_[i+1] - start_[i];
    // 将第 i 个 key 封装成 Slice 并保存到 tmp_keys 用于后续计算 filter 
    tmp_keys_[i] = Slice(base, length); 
  }

  // Generate filter for current set of keys and append to result_.
  // tmp_keys_ 对应的 filter 在 result 中的偏移量保存到 filter_offsets_
  filter_offsets_.push_back(result_.size());
  // 根据 tmp_keys_ 计算 filter 并追加到 result
  policy_->CreateFilter(&tmp_keys_[0], static_cast<int>(num_keys), &result_);

  // 清空与当前 data block 对应数据相关的状态以方便为下个 data block 计算 filter 使用
  tmp_keys_.clear();
  keys_.clear();
  start_.clear();
}

// 使用一个 FilterPolicy 和一个 filter block 构造一个 FilterBlockReader. 
// 注意两个参数生命期非常关键, 因为直接存的地址, 它们不能先于此处构造的对象死掉. 
FilterBlockReader::FilterBlockReader(const FilterPolicy* policy,
                                     const Slice& contents)
    : policy_(policy),
      data_(nullptr),
      offset_(nullptr),
      num_(0),
      base_lg_(0) {
  size_t n = contents.size();
  // filter block 最后 5 个字节依次是 filter offset array 的起始偏移量(4 字节), base 的 log 值(1 字节). 
  if (n < 5) return;  // 1 byte for base_lg_ and 4 for start of offset array
  base_lg_ = contents[n-1]; // 读取 base log 值
  uint32_t last_word = DecodeFixed32(contents.data() + n - 5); // 读取 offset array 起始偏移量
  if (last_word > n - 5) return;
  data_ = contents.data(); // filter block 起始地址
  offset_ = data_ + last_word; // offset array 起始地址
  num_ = (n - 5 - last_word) / 4; // offset array 元素(每个占 4 字节)个数
}

// 通过过滤器查询 key 是否在以 block_offset 为起始地址的 block 中
bool FilterBlockReader::KeyMayMatch(uint64_t block_offset, const Slice& key) {
  // 计算 filter 的时候是每隔 base 大小为一个区间, 每个区间对应一个 filter. 
  // 起始偏移量落在对应区间内的 blocks, 会将其全部 keys 作为整体输入计算一个 filter. 
  // 下面这一步相当于除以 base 取商, 从而得到 block_offset 对应的 filter offset array 数组索引. 
  // 注意, 这里的算法正好解答了 leveldb::FilterBlockBuilder::GenerateFilter() 的疑问, 针对多出来
  //      那个重复的 filter offset, 会被自动跳过.
  uint64_t index = block_offset >> base_lg_;
  if (index < num_) {
    // 计算第 i 个 filter 起止地址
    uint32_t start = DecodeFixed32(offset_ + index*4);
    // 这里不要担心溢出什么的, 即使 index == N-1 得到的 limit 就是 offset array 起始地址, 
    // 正好是 filter-<N-1> 内容结束地址下一个字节. 
    uint32_t limit = DecodeFixed32(offset_ + index*4 + 4);
    if (start <= limit && limit <= static_cast<size_t>(offset_ - data_)) {
      Slice filter = Slice(data_ + start, limit - start);
      return policy_->KeyMayMatch(key, filter);
    } else if (start == limit) {
      // Empty filters do not match any keys
      return false;
    }
  }

  // 如果出错则认为 key 存在, 这个是 false positive, 符合预期. 
  return true;  // Errors are treated as potential matches
}

}

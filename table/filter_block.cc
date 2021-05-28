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

// 为前一个已写入 table 文件的 data block 生成
// filter, 生成完毕后重置当前 FilterBlockBuilder 的状态为生成下一个
// filter 做准备.
void FilterBlockBuilder::StartBlock(uint64_t block_offset) {
  // 计算以 block_offset 为起始地址的 block 对应的 filter 在
  // filter-offset 数组中的索引.
  // 默认每两 KB 数据就要生成一个 filter, 
  // 如果 block size 超过 2KB, 则会生成多个 filters.
  uint64_t filter_index = (block_offset / kFilterBase);
  assert(filter_index >= filter_offsets_.size());
  // filter 是一个接一个构造的, 对应的索引数组也是对应着逐渐增长的, 
  // 而非一次性构造好往里面填, 毕竟不知道要生成多少个 filters
  while (filter_index > filter_offsets_.size()) {
    // 这里虽然是个循环, 但是因为每次生成 filter 
    // 都会清空相关状态(keys_, start_ 等等), 
    // 所以下个循环并不会再生成 filter 了, 具体见
    // GenerateFilter() 的 if 部分.
    GenerateFilter();
  }
}

// 向 keys_ 中增加一个 key, 同时将 key 在 keys 中
// 起始偏移量保存到 start_ 向量中.
void FilterBlockBuilder::AddKey(const Slice& key) {
  Slice k = key;
  start_.push_back(keys_.size());
  keys_.append(k.data(), k.size());
}

// 计算最后一个 filter 对应的偏移量, 然后将 
// filter-offset array 并追加到 result_.
// 执行结束, result_ 保存的就是一个完整的 filter block
Slice FilterBlockBuilder::Finish() {
  // 若 keys_ 不为空需要为其生成一个 filter
  if (!start_.empty()) { 
    GenerateFilter();
  }

  // 下面马上会把每个 filter 在 result_ 中对应的
  // 起始地址也编码追加到 result_ 中, 这样前面是 filters, 
  // 后面是 filters 的起始偏移量, 那这两部分反序列化时候怎么区分呢?
  // 我们提前记录追加 filters 总字节数就可以了.
  // array_offset 保存全部 filters 的总字节数, 
  // 这个值在追加完全部 filters 
  // 的起始地址后也会被追加到 result_ 作为最后一个元素, 
  // 也是反序列化解析入口.
  const uint32_t array_offset = result_.size();
  // 将每个 filter 在 result_ 中的起始偏移量编码追加到 result_
  for (size_t i = 0; i < filter_offsets_.size(); i++) {
    PutFixed32(&result_, filter_offsets_[i]);
  }

  // 将 filter-offset array 的起始偏移量编码追加到 result_, 
  // 用于反序列化时区分 filters 与 filters 偏移量数组.
  PutFixed32(&result_, array_offset);
  // 将 base 的 log 值追加到 result, 用于反序列化时计算 base
  result_.push_back(kFilterBaseLg);
  // 到此为止, result_ 已经是一个完整的 filter block 了, 
  // 将其封装为 Slice 后返回.
  return Slice(result_);
}

// 由于即将生成的 block 不能与当前已写入 table 文件的 block 
// 的 keys 共用 filter 了, 所以为当前已写入 table 文件的 block 的 
// keys_ 生成一个 filter. 生成完毕后清空当前 FilterBlockBuilder 
// 相关相关状态以为下个 filter 计算所用.
void FilterBlockBuilder::GenerateFilter() {
  // keys_ 为空, 无须生成新的 filter.
  const size_t num_keys = start_.size();
  if (num_keys == 0) {
    // 没有 key 需要计算 filter, 则直接把上一个 filter 
    // 的结束地址(每个 filter 都是一个字符串, 所以保存到 result_ 
    // 时候既有起始地址又有结束地址)填充到 filter-offset 数组中,
    // 这么做一方面为了对齐(方便 FilterBlockReader::KeyMayMatch() 
    // 直接通过移位计算 filter 索引), 另一方面方便计算 filter 结束偏移量(
    // 就是 FilterBlockReader::KeyMayMatch() 计算 limit 的步骤).
    filter_offsets_.push_back(result_.size());
    return;
  }

  // 将扁平化的 keys_ 转换为一个 key 列表.
  // 将 keys_ 大小放到 start_ 中作为最后一个 key 的结束地址, 
  // 这样下面可以直接用 start_[i+1] - start_[i] 计算
  // 每个 key 长度.
  start_.push_back(keys_.size());  
  tmp_keys_.resize(num_keys);
  // 将字符串 keys_ 保存的每个 key 提取出来封装
  // 成 Slice 并放到 tmp_keys 列表中
  for (size_t i = 0; i < num_keys; i++) {
    // 第 i 个 key 在 keys 中的起始地址
    const char* base = keys_.data() + start_[i]; 
    // 第 i 个 key 的长度
    size_t length = start_[i+1] - start_[i];
    // 将第 i 个 key 封装成 Slice 并保存到 tmp_keys 
    // 用于后续计算 filter 
    tmp_keys_[i] = Slice(base, length); 
  }

  // 为当前的 key 集合生成 filter.
  // 先将新生成的 filter 在 result_ 中的
  // 起始偏移量保存到 filter_offsets_.
  filter_offsets_.push_back(result_.size());
  // 根据 tmp_keys_ 计算 filter 并追加到 result
  policy_->CreateFilter(&tmp_keys_[0], static_cast<int>(num_keys), &result_);

  // 重置当前 FilterBlockBuilder 相关的状态以方便为
  // 下个 data block 计算 filter 使用.
  tmp_keys_.clear();
  keys_.clear();
  start_.clear();
}

// 反序列化 filter block.
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
  // filter block 最后 5 个字节依次是: 
  // - filters 部分的结束偏移量(4 字节), 
  //   它是 filters 和 filter-offset 数组的分隔符,
  //   所以它同时也是 filter-offset 的起始偏移量.
  // - base 的 log 值(1 字节). 
  if (n < 5) return;
  // 读取 base log 值
  base_lg_ = contents[n-1]; 
  // 读取 offset array 起始偏移量
  uint32_t last_word = DecodeFixed32(contents.data() + n - 5); 
  if (last_word > n - 5) return;
  // filter block 起始地址
  data_ = contents.data(); 
  // offset array 起始地址
  offset_ = data_ + last_word; 
  // offset array 中元素(每个偏移量用 4 字节无符号数表示)的个数
  num_ = (n - 5 - last_word) / 4; 
}

// 通过过滤器查询 key 是否在以 block_offset 为起始地址的 block 中
bool FilterBlockReader::KeyMayMatch(uint64_t block_offset, const Slice& key) {
  // 计算 filter 的时候是每隔 base 大小为一个区间, 
  // 每个区间对应一个 filter. 
  // 起始偏移量落在对应区间内的 blocks, 
  // 会将其全部 keys 作为整体输入计算一个 filter. 
  
  // 注意, 这里的算法正好解答了 
  // leveldb::FilterBlockBuilder::GenerateFilter() 的疑问, 
  // 针对多出来那个重复的 filter offset, 会被自动跳过.
  // 下面这一步相当于除以 base 取商, 从而得到 block_offset 
  // 对应的 filter-offset array 数组索引. 
  uint64_t index = block_offset >> base_lg_;
  if (index < num_) {
    // 计算计算索引为 index 的 filter 的 offset, 
    // 具体为先定位到保存目标 filter offset 的地址(每个地址长度为 4 字节), 
    // 然后将其解码为一个无符号 32 位数.
    uint32_t start = DecodeFixed32(offset_ + index*4);
    // 下面这个计算分两种情况理解:
    // - 当 index < num_ - 1 时, 又要分为两个子情况
    //   - 如果 GenerateFilter() 方法在生成索引为 index 的 filter 时,
    //     至少多填充了一次 filter 结束偏移量到 filter-offset 数组, 那么
    //     该操作等价于取出索引为 index 的 filter 的结束偏移量
    //    (注意 GenerateFilter() 方法填充 filter 结束地址到
    //   filter-offset 的情况)
    //   - 如果 GenerateFilter() 方法在生成索引为 index 的 filter 时, 
    //     仅为该 filter 在 filter-offset 数组填入了起始偏移量, 那么该操作
    //     相当于计算索引为 index+1 的 filter 的起始 offset.
    // - 当 index == num_ - 1 时, 也要再分为两个子情况
    //   - 同 index < num_ - 1 时的第一个子情况.
    //   - 如果 GenerateFilter() 方法在生成索引为 index 的 filter 时, 
    //     仅为该 filter 在 filter-offset 数组填入了起始偏移量, 那么该操作
    //     等价于直接取出索引为 index 的 filter 的结束偏移量. 
    //     为什么看起来和第一个子情况一样? 其实不然. 因为 filter block 构成分四块, 
    //     分别是"一系列filters+filters-offset数组+filters和filters-offset
    //     数组的分隔符+base的log值", 这里提到的分隔符用的就是
    //     最后一个 filter 的结束地址). 
    // 不管是哪种情况, limit - start 都是索引为 index 的 filter 的长度.
    uint32_t limit = DecodeFixed32(offset_ + index*4 + 4);
    // 注意 start 和 limit 都是相对于 data_ 的 offset 而非绝对地址.
    if (start <= limit && limit <= static_cast<size_t>(offset_ - data_)) {
      // 取出 filter
      Slice filter = Slice(data_ + start, limit - start);
      // 在 filter 中查询 key 是否存在
      return policy_->KeyMayMatch(key, filter);
    } else if (start == limit) {
      // 如果 start == limit 说明索引为 index 的 filter 为空
      return false;
    }
  }

  // 如果出错则认为 key 存在, 这个是 false positive, 
  // 符合人们对过滤器的预期. 通过后续在 data block 查询进一步确认.
  return true;
}

}

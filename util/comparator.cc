// Copyright (c) 2011 The LevelDB Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file. See the AUTHORS file for names of contributors.

#include <algorithm>
#include <cstdint>
#include <string>

#include "leveldb/comparator.h"
#include "leveldb/slice.h"
#include "util/logging.h"
#include "util/no_destructor.h"

namespace leveldb {

Comparator::~Comparator() { }

namespace {
// 一个基于字典序进行逐字节比较的内置 comparator 实现
class BytewiseComparatorImpl : public Comparator {
 public:
  BytewiseComparatorImpl() { }

  virtual const char* Name() const {
    return "leveldb.BytewiseComparator";// 注意 "leveldb." 开头的名字被 leveldb 保留了，如果客户端实现自己的 comparator 不要用这种命名方式
  }

  virtual int Compare(const Slice& a, const Slice& b) const {
    return a.compare(b);
  }

  virtual void FindShortestSeparator(
      std::string* start,
      const Slice& limit) const {
    // Find length of common prefix
    size_t min_length = std::min(start->size(), limit.size());
    size_t diff_index = 0;
    while ((diff_index < min_length) &&
           ((*start)[diff_index] == limit[diff_index])) { // 寻找最长共同前缀，diff_index 是共同前缀长度
      diff_index++;
    }

    // 如果输入参数中两个参数，一个是另一个的前缀，肯定是 start 是 limit 的前缀，
    // 则不需要进行缩短，保持 start 内容不变。
    if (diff_index >= min_length) {
      // Do not shorten if one string is a prefix of the other
    } else {
      // 从 start 取出第一个与 limit 互异的字节并转换为 8 位无符号数放到 diff_byte
      uint8_t diff_byte = static_cast<uint8_t>((*start)[diff_index]);
      // 第一条件防止后面递增的时候溢出变为 0 导致递增反而值变小；
      // diff_byte 肯定与 limit[diff_index] 不等，而且肯定是前者小于后者，这个是参数语义以及上面寻找前缀决定的，
      // 第二个条件是递增的前提，否则 diff_byte + 1 == limit[diff_index] 而且 diff_index 是 limit 最后一个字符，
      // 那么递增后 start 大于等于 limit，违背了限制条件结果需要落在 [start, limit)。
      if (diff_byte < static_cast<uint8_t>(0xff) &&
          diff_byte + 1 < static_cast<uint8_t>(limit[diff_index])) {
        (*start)[diff_index]++; // 将 start 中第一个与 limit 互异的字节加 1
        start->resize(diff_index + 1); // 将 start 截断，长度为 diff_index + 1，此时 start 比之前的值大，而且长度更短。
        assert(Compare(*start, limit) < 0);
      }
    }
  }

  virtual void FindShortSuccessor(std::string* key) const {
    // Find first character that can be incremented
    // 如果 key 每个字节都是 0xff，则原样返回 key，因为它的最短的后继字符串就是它自己。
    // 找到 key 中第一个不是 0xff 的字节，将其递增 1，然后从此处截断 key, 比如，
    // 若 key 为 【0xff,0x01,0x12】，则处理完毕后，key 变为 【0xff,0x02】
    size_t n = key->size();
    for (size_t i = 0; i < n; i++) {
      const uint8_t byte = (*key)[i];
      if (byte != static_cast<uint8_t>(0xff)) {
        (*key)[i] = byte + 1;
        key->resize(i+1);
        return;
      }
    }
    // *key is a run of 0xffs.  Leave it alone.
  }
};
}  // namespace

const Comparator* BytewiseComparator() {
  static NoDestructor<BytewiseComparatorImpl> singleton;
  return singleton.get();
}

}  // namespace leveldb

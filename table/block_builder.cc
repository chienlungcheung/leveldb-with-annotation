// Copyright (c) 2011 The LevelDB Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file. See the AUTHORS file for names of contributors.
//
// BlockBuilder generates blocks where keys are prefix-compressed:
//
// When we store a key, we drop the prefix shared with the previous
// string.  This helps reduce the space requirement significantly.
// Furthermore, once every K keys, we do not apply the prefix
// compression and store the entire key.  We call this a "restart
// point".  The tail end of the block stores the offsets of all of the
// restart points, and can be used to do a binary search when looking
// for a particular key.  Values are stored as-is (without compression)
// immediately following the corresponding key.
//
// An entry for a particular key-value pair has the form:
//     shared_bytes: varint32
//     unshared_bytes: varint32
//     value_length: varint32
//     key_delta: char[unshared_bytes]
//     value: char[value_length]
// shared_bytes == 0 for restart points.
//
// The trailer of the block has the form:
//     restarts: uint32[num_restarts]
//     num_restarts: uint32
// restarts[i] contains the offset within the block of the ith restart point.

#include "table/block_builder.h"

#include <algorithm>
#include <assert.h>
#include "leveldb/comparator.h"
#include "leveldb/table_builder.h"
#include "util/coding.h"

namespace leveldb {

BlockBuilder::BlockBuilder(const Options* options)
    : options_(options),
      restarts_(),
      counter_(0),
      finished_(false) {
  assert(options->block_restart_interval >= 1);
  // 第一个 restart point 在 block 中的偏移量为 0
  restarts_.push_back(0);       // First restart point is at offset 0
}

void BlockBuilder::Reset() {
  buffer_.clear();
  restarts_.clear();
  restarts_.push_back(0);       // First restart point is at offset 0
  counter_ = 0;
  finished_ = false;
  last_key_.clear();
}

size_t BlockBuilder::CurrentSizeEstimate() const {
  // 原始数据所占空间的大小 + restarts 数组所占空间大小 + restart 数组长度所占空间
  return (buffer_.size() +                        // Raw data buffer
          restarts_.size() * sizeof(uint32_t) +   // Restart array
          sizeof(uint32_t));                      // Restart array length
}

Slice BlockBuilder::Finish() {
  /**
   * 先将 restarts 数组编码后追加到 buffer，
   * 然后将 restarts 数组长度编码后追加到 buffer 并将 finished 置位，
   * 最后根据 buffer 构造一个新的 slice 返回（注意该 slice 引用的内存是 buffer，所以生命期同 builder，除非 builder 调用了 Reset）
   */
  // Append restart array
  for (size_t i = 0; i < restarts_.size(); i++) {
    PutFixed32(&buffer_, restarts_[i]);
  }
  PutFixed32(&buffer_, restarts_.size());
  finished_ = true;
  return Slice(buffer_);
}

void BlockBuilder::Add(const Slice& key, const Slice& value) {
  Slice last_key_piece(last_key_);
  // 断言没有调用过 Finish 方法
  assert(!finished_);
  // 断言自上个 restart 之后追加的 key 的个数没有超过配置的两个 restart points 之间 keys 的个数
  assert(counter_ <= options_->block_restart_interval);
  // 断言当前要追加的 key 要大于任何之前追加到 buffer 中的 key
  assert(buffer_.empty() // No values yet?
         || options_->comparator->Compare(key, last_key_piece) > 0);
  size_t shared = 0;
  // 如果自上个 restart 之后追加的 key 的个数小于所配置的两个 restart points 之间 keys 的个数，
  // 计算当前要追加的 key 与上次追加的 key 的公共前缀长度。
  if (counter_ < options_->block_restart_interval) {
    // See how much sharing to do with previous string
    const size_t min_length = std::min(last_key_piece.size(), key.size());
    while ((shared < min_length) && (last_key_piece[shared] == key[shared])) {
      shared++;
    }
  } else {
    // 否则，新增一个 restart point，而且该 restart 的第一个数据项的 key 不进行压缩。
    // - restart 就是一个 offset，具体值为当前 buffer 所占空间大小。
    // - restart 后第一个数据项的 key 不进行压缩，即不计算与前一个 key 的公共前缀了，而是把这个 key 整个保存起来，
    //  但是本 “restart” 段，从这个 key 开始后面的 keys 都要进行压缩。
    // Restart compression
    restarts_.push_back(buffer_.size());
    counter_ = 0;
  }
  const size_t non_shared = key.size() - shared;

  // Add "<shared><non_shared><value_size>" to buffer_
  //
  // buffer 里面的每个记录的格式为：
  // <varint32 类型的当前 key 与上个 key 公共前缀长度>
  // <varint32 类型的当前 key 长度减去公共前缀后的长度>
  // <varint32 类型的当前 value 的长度>
  // <与前一个 key 公共前缀之后的部分>
  // <value>
  PutVarint32(&buffer_, shared);
  PutVarint32(&buffer_, non_shared);
  PutVarint32(&buffer_, value.size());

  // Add string delta to buffer_ followed by value
  buffer_.append(key.data() + shared, non_shared);
  buffer_.append(value.data(), value.size());

  // Update state
  last_key_.resize(shared);
  last_key_.append(key.data() + shared, non_shared); // 将 last_key 更新为当前 key
  assert(Slice(last_key_) == key);
  counter_++; // 将自上个 restart 之后的记录数加一
}

}  // namespace leveldb

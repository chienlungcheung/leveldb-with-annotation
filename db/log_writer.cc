// Copyright (c) 2011 The LevelDB Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file. See the AUTHORS file for names of contributors.

#include "db/log_writer.h"

#include <stdint.h>
#include "leveldb/env.h"
#include "util/coding.h"
#include "util/crc32c.h"

namespace leveldb {
namespace log {

// 计算每个 record type 对应的 crc 值
static void InitTypeCrc(uint32_t* type_crc) {
  for (int i = 0; i <= kMaxRecordType; i++) {
    char t = static_cast<char>(i);
    type_crc[i] = crc32c::Value(&t, 1);
  }
}

Writer::Writer(WritableFile* dest)
    : dest_(dest),
      block_offset_(0) {
  InitTypeCrc(type_crc_); // 提前把每个 record  type 的 crc 算好
}

Writer::Writer(WritableFile* dest, uint64_t dest_length)
    : dest_(dest), block_offset_(dest_length % kBlockSize) {
  InitTypeCrc(type_crc_); // 提前把每个 record  type 的 crc 算好
}

Writer::~Writer() {
}

// 将 slice 中保存的 record 追加到文件中
Status Writer::AddRecord(const Slice& slice) {
  const char* ptr = slice.data();
  size_t left = slice.size(); // record 剩余待写入长度

  // Fragment the record if necessary and emit it.  Note that if slice
  // is empty, we still want to iterate once to emit a single
  // zero-length record
  //
  // 如有必要则将 record 分片后写入文件。如果 slice 内容为空，则我们仍将会写入一个长度为 0 的 record 到文件中。
  Status s;
  bool begin = true;
  do {
    const int leftover = kBlockSize - block_offset_; // 当前 block 剩余空间大小
    assert(leftover >= 0);
    if (leftover < kHeaderSize) { // 如果当前 block 剩余空间不足容纳 record 的 header 则剩余空间作为 trailer 填充 0
      // Switch to a new block
      if (leftover > 0) {
        // Fill the trailer (literal below relies on kHeaderSize being 7)
        assert(kHeaderSize == 7);
        dest_->Append(Slice("\x00\x00\x00\x00\x00\x00", leftover)); // 最终填充多少 0 由 leftover 决定，最大 6 字节
      }
      block_offset_ = 0;
    }

    // Invariant: we never leave < kHeaderSize bytes in a block.
    // 到这一步，block （可能因为不足 kHeaderSize 在上面已经切换到了下个 block）最终剩余字节必定大约等于 kHeaderSize
    assert(kBlockSize - block_offset_ - kHeaderSize >= 0);

    // 除了待写入 header，当前 block 还剩多大空间，可能为 0；
    // block 最后剩下空间可能只够写入一个新 record 的 header 了
    const size_t avail = kBlockSize - block_offset_ - kHeaderSize;
    const size_t fragment_length = (left < avail) ? left : avail; // 可以写入当前 block 的 record data 剩余内容的长度，可能为 0

    RecordType type;
    const bool end = (left == fragment_length); // 判断是否将 record 剩余内容分片
    if (begin && end) {
      // 如果该 record 内容第一次写入文件，而且，
      // 如果 block 剩余空间可以容纳 record data 全部内容，
      // 则写入一个 full 类型 fragment
      type = kFullType;
    } else if (begin) {
      // 如果该 record 内容第一写入文件，而且，
      // 如果 block 剩余空间无法容纳 record data 全部内容，
      // 则写入一个 first 类型 fragment。
      // 注意，此时是 record 第一次写入即它是一个新 record，该 block 剩余空间可能只够容纳 header 了，
      // 则在 block 尾部写入一个 FIRST 类型 header，record data 不写入，等下次循环会切换到下个 block，
      // 然后又会重新写入一个非 FIRST 类型的 header （注意下面会将 begin 置为 false）而不是紧接着在新 block 只写入 data 部分。
      type = kFirstType;
    } else if (end) {
      // 如果这不是该 record 内容第一写入文件，而且，
      // 如果 block 剩余空间可以容纳 record data 剩余内容，
      // 则写入一个 last 类型 fragment
      type = kLastType;
    } else {
      // 如果这不是该 record 内容第一写入文件，而且，
      // 如果 block 剩余空间无法容纳 record data 剩余内容，
      // 则写入一个 middle 类型 fragment
      type = kMiddleType;
    }

    s = EmitPhysicalRecord(type, ptr, fragment_length);
    ptr += fragment_length;
    left -= fragment_length;
    begin = false; // 即使当前 block 剩余空间只够写入一个新 record 的 FIRST 类型 header，record 也算写入过了
  } while (s.ok() && left > 0); // 写入不出错且 record 再无剩余内容则写入完毕
  return s;
}

// 将一个类型为 t 的 fragment 写入文件。
// 该 fragment 内容由 ptr 指向，长度为 n 字节。
Status Writer::EmitPhysicalRecord(RecordType t, const char* ptr, size_t n) {
  // fragment 大小必须能够被 16 位无符号整数表示
  assert(n <= 0xffff);  // Must fit in two bytes
  assert(block_offset_ + kHeaderSize + n <= kBlockSize); // 要写入的内容不能超过当前 block 剩余空间大小

  // Format the header
  char buf[kHeaderSize];
  buf[4] = static_cast<char>(n & 0xff); // length 低 8 位安排在低地址位置
  buf[5] = static_cast<char>(n >> 8); // 然后写入 length 高 8 位安排在高地址位置
  buf[6] = static_cast<char>(t); // type 紧随 length 之后 1 字节

  // Compute the crc of the record type and the payload.
  // 计算 type 和 data 的 crc 并编码安排在最前面 4 个字节
  uint32_t crc = crc32c::Extend(type_crc_[t], ptr, n);
  crc = crc32c::Mask(crc);                 // Adjust for storage
  EncodeFixed32(buf, crc);

  // Write the header and the payload
  Status s = dest_->Append(Slice(buf, kHeaderSize)); // 写入 header
  if (s.ok()) {
    s = dest_->Append(Slice(ptr, n)); // 写入 data
    if (s.ok()) {
      s = dest_->Flush();
    }
  }
  block_offset_ += kHeaderSize + n; // 当前 block 剩余空间起始偏移量。注意，这里不管 header 和 data 是否写成功。
  return s;
}

}  // namespace log
}  // namespace leveldb

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

// 将 slice 中保存的数据封装为 record 追加到 log 文件中,
// 这些数据可能因为太多而分为若干 record 写入到 log 文件中.
Status Writer::AddRecord(const Slice& slice) {
  const char* ptr = slice.data();
  // data 剩余部分长度, 初始值为其原始长度
  size_t left = slice.size(); 

  // 如有必要则将 record 分片后写入文件. 
  // 如果 slice 内容为空, 则我们仍将会写入一个长度为 0 的 record 到文件中. 
  Status s;
  bool begin = true;
  do {
    // 当前 block 剩余空间大小
    const int leftover = kBlockSize - block_offset_; 
    assert(leftover >= 0);
    // 如果当前 block 剩余空间不足容纳 record 的 header(7 字节) 
    // 则剩余空间作为 trailer 填充 0, 然后切换到新的 block.
    if (leftover < kHeaderSize) { 
      if (leftover > 0) {
        assert(kHeaderSize == 7);
        // 最终填充多少 0 由 leftover 决定, 最大 6 字节
        dest_->Append(Slice("\x00\x00\x00\x00\x00\x00", leftover)); 
      }
      block_offset_ = 0;
    }

    // 到这一步, block (可能因为不足 kHeaderSize 在上面已经切换到了下个 block)
    // 最终剩余字节必定大约等于 kHeaderSize
    assert(kBlockSize - block_offset_ - kHeaderSize >= 0);

    // block 当前剩余空闲字节数.
    // 除了待写入 header, 当前 block 还剩多大空间, 可能为 0; 
    // block 最后剩下空间可能只够写入一个新 record 的 header 了
    const size_t avail = kBlockSize - block_offset_ - kHeaderSize;
    // 可以写入当前 block 的 record data 剩余内容的长度, 可能为 0
    const size_t fragment_length = (left < avail) ? left : avail;

    RecordType type; 
    // 判断是否将 record 剩余内容分片
    const bool end = (left == fragment_length);
    if (begin && end) {
      // 如果该 record 内容第一次写入文件, 而且, 
      // 如果 block 剩余空间可以容纳 record data 全部内容, 
      // 则写入一个 full 类型 record
      type = kFullType;
    } else if (begin) {
      // 如果该 record 内容第一写入文件, 而且, 
      // 如果 block 剩余空间无法容纳 record data 全部内容, 
      // 则写入一个 first 类型 record. 
      // 注意, 此时是 record 第一次写入即它是一个新 record, 
      //    该 block 剩余空间可能只够容纳 header 了, 
      //    则在 block 尾部写入一个 FIRST 类型 header, record data 不写入, 
      //    等下次循环会切换到下个 block, 然后又会重新写入一个
      //    非 FIRST 类型的 header (注意下面会将 begin 置为 false)
      //    而不是紧接着在新 block 只写入 data 部分. 
      type = kFirstType;
    } else if (end) {
      // 如果这不是该 record 内容第一写入文件, 而且, 
      // 如果 block 剩余空间可以容纳 record data 剩余内容, 
      // 则写入一个 last 类型 record
      type = kLastType;
    } else {
      // 如果这不是该 record 内容第一写入文件, 而且, 
      // 如果 block 剩余空间无法容纳 record data 剩余内容, 
      // 则写入一个 middle 类型 record
      type = kMiddleType;
    }

    // 将类型为 type, data 长度为 fragment_length 的 record 写入 log 文件.
    s = EmitPhysicalRecord(type, ptr, fragment_length);
    ptr += fragment_length;
    left -= fragment_length;
    // 即使当前 block 剩余空间只够写入一个新 record 的 FIRST 类型 header, 
    // record 也算写入过了
    begin = false; 
    // 写入不出错且 record 再无剩余内容则写入完毕
  } while (s.ok() && left > 0); 
  return s;
}

// 将一个类型为 t, data 长度为 n 的 record 写入 log 文件. 
// 该 record data 部分的内容由 ptr 指向, 可写入 data 长度为 n 字节. 
Status Writer::EmitPhysicalRecord(RecordType t, const char* ptr, size_t n) {
  // data 大小必须能够被 16 位无符号整数表示, 因为 record 的 length 字段只有两字节
  assert(n <= 0xffff);
  assert(block_offset_ + kHeaderSize + n <= kBlockSize); // 要写入的内容不能超过当前 block 剩余空间大小

  // buf 用于组装 record header
  char buf[kHeaderSize];
  // 将数据长度编码到 length 字段, 小端字节序
  // length 低 8 位安排在低地址位置
  buf[4] = static_cast<char>(n & 0xff); 
  // 然后写入 length 高 8 位安排在高地址位置
  buf[5] = static_cast<char>(n >> 8); 
  // 将 type 编码到 type 字段, type 紧随 length 之后 1 字节
  buf[6] = static_cast<char>(t); 

  // 计算 type 和 data 的 crc 并编码安排在最前面 4 个字节
  uint32_t crc = crc32c::Extend(type_crc_[t], ptr, n);
  crc = crc32c::Mask(crc);
  // 将 crc 写入到 header 前四个字节
  EncodeFixed32(buf, crc);

  // 写入 header
  Status s = dest_->Append(Slice(buf, kHeaderSize)); 
  if (s.ok()) {
    // 写入 payload
    s = dest_->Append(Slice(ptr, n));
    if (s.ok()) {
      // 刷入文件
      s = dest_->Flush();
    }
  }
  // 当前 block 剩余空间起始偏移量. 
  // 注意, 这里不管 header 和 data 是否写成功. 
  block_offset_ += kHeaderSize + n; 
  return s;
}

}  // namespace log
}  // namespace leveldb

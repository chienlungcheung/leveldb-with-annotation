// Copyright (c) 2011 The LevelDB Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file. See the AUTHORS file for names of contributors.
//
// Log format information shared by reader and writer.
// See ../doc/log_format.md for more detail.

#ifndef STORAGE_LEVELDB_DB_LOG_FORMAT_H_
#define STORAGE_LEVELDB_DB_LOG_FORMAT_H_

namespace leveldb {
// log 文件内容是一系列 blocks，每个 block 大小为 32KB。唯一的例外就是，log 文件末尾可能包含一个不完整的 block。
//
// 每个 block 由一系列 records 构成：
//
//    block := record* trailer? // 即 0 或多个 records，0 或 1 个 trailer，总大小为 4 + 2 + 1 + length + trailer 大小
//    record :=
//    // 下面的 type 和 data[] 的 crc32c 校验和，小端字节序
//    checksum: uint32     // crc32c of type and data[] ; little-endian
//    // 下面的 data[] 的长度，小端字节序
//    length: uint16       // little-endian
//    // 类型，FULL、FIRST、MIDDLE、LAST 取值之一
//    type: uint8          // One of FULL, FIRST, MIDDLE, LAST
//    // 数据
//    data: uint8[length]
// 如果一个 block 剩余字节不超过 6 个，则不会在这个剩余空间构造任何 record，因为大小不合适。
// 这些剩余空间构成 trailer，应该被 reader 略过。
namespace log {

enum RecordType {
  // Zero is reserved for preallocated files
  kZeroType = 0,

  kFullType = 1,

  // For fragments
  kFirstType = 2,
  kMiddleType = 3,
  kLastType = 4
};
static const int kMaxRecordType = kLastType;

static const int kBlockSize = 32768;

// Header is checksum (4 bytes), length (2 bytes), type (1 byte).
// 每个 record 的 header 由 checksum (4 bytes), length (2 bytes), type (1 byte) 构成。
static const int kHeaderSize = 4 + 2 + 1;

}  // namespace log
}  // namespace leveldb

#endif  // STORAGE_LEVELDB_DB_LOG_FORMAT_H_

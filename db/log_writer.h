// Copyright (c) 2011 The LevelDB Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file. See the AUTHORS file for names of contributors.

#ifndef STORAGE_LEVELDB_DB_LOG_WRITER_H_
#define STORAGE_LEVELDB_DB_LOG_WRITER_H_

#include <stdint.h>
#include "db/log_format.h"
#include "leveldb/slice.h"
#include "leveldb/status.h"

namespace leveldb {

class WritableFile;

namespace log {

class Writer {
 public:
  // Create a writer that will append data to "*dest".
  // "*dest" must be initially empty.
  // "*dest" must remain live while this Writer is in use.
  //
  // 创建一个 writer 用于追加数据到 dest 指向的文件。
  // dest 指向的文件初始必须为空文件；dest 生命期不能短于 writer。
  explicit Writer(WritableFile* dest);

  // Create a writer that will append data to "*dest".
  // "*dest" must have initial length "dest_length".
  // "*dest" must remain live while this Writer is in use.
  //
  // 创建一个 writer 用于追加数据到 dest 指向的文件。
  // dest 指向文件初始长度必须为 dest_length；dest 生命期不能短于 writer。
  Writer(WritableFile* dest, uint64_t dest_length);

  ~Writer();

  Status AddRecord(const Slice& slice);

 private:
  WritableFile* dest_; // 指向要写入的 log file
  int block_offset_;       // Current offset in block 当前 block 待填充字节的偏移量

  // crc32c values for all supported record types.  These are
  // pre-computed to reduce the overhead of computing the crc of the
  // record type stored in the header.
  //
  // 每个 record type 对应的 crc。
  // 这些值都是提前算好的，因为 type 都是已知的，没必要为每个 record 再挨个算一遍。
  uint32_t type_crc_[kMaxRecordType + 1];

  Status EmitPhysicalRecord(RecordType type, const char* ptr, size_t length);

  // No copying allowed
  Writer(const Writer&);
  void operator=(const Writer&);
};

}  // namespace log
}  // namespace leveldb

#endif  // STORAGE_LEVELDB_DB_LOG_WRITER_H_

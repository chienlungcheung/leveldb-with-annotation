// Copyright (c) 2011 The LevelDB Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file. See the AUTHORS file for names of contributors.

#ifndef STORAGE_LEVELDB_DB_LOG_READER_H_
#define STORAGE_LEVELDB_DB_LOG_READER_H_

#include <stdint.h>

#include "db/log_format.h"
#include "leveldb/slice.h"
#include "leveldb/status.h"

namespace leveldb {

class SequentialFile;

namespace log {

class Reader {
 public:
  // Interface for reporting errors.
  // 用于汇报错误的接口
  class Reporter {
   public:
    virtual ~Reporter();

    // Some corruption was detected.  "size" is the approximate number
    // of bytes dropped due to the corruption.
    virtual void Corruption(size_t bytes, const Status& status) = 0;
  };

  // Create a reader that will return log records from "*file".
  // "*file" must remain live while this Reader is in use.
  //
  // If "reporter" is non-null, it is notified whenever some data is
  // dropped due to a detected corruption.  "*reporter" must remain
  // live while this Reader is in use.
  //
  // If "checksum" is true, verify checksums if available.
  //
  // The Reader will start reading at the first record located at physical
  // position >= initial_offset within the file.
  //
  // 创建一个 Reader 来从 file 中读取和解析 records，读取的第一个 record 的起始位置位于文件 initial_offset 或其之后的物理地址。
  // 如果 reporter 不为空，则在检测到数据损坏时汇报要丢弃的数据估计大小。
  // 如果 checksum 为 true，则在可行的条件比对校验和。
  // 注意，file 和 reporter 的生命期不能短于 Reader 对象。
  Reader(SequentialFile* file, Reporter* reporter, bool checksum,
         uint64_t initial_offset);

  ~Reader();

  // Read the next record into *record.  Returns true if read
  // successfully, false if we hit end of the input.  May use
  // "*scratch" as temporary storage.  The contents filled in *record
  // will only be valid until the next mutating operation on this
  // reader or the next mutation to *scratch.
  //
  // 读取下一个 record 到 *record 中。如果读取成功，返回 true；遇到文件尾返回 false。
  // 直到在该 reader 上执行修改操作或者针对 *scratch 执行修改操作，填充到 *record 都是有效的。
  // 如果当前读取的 record 没有被分片，那就用不到 *scratch 参数来为 *record 做底层存储了；其它情况
  // 需要借助 *scratch 来拼装分片的 record data 部分，最后封装为一个 Slice 赋值给 *record。
  bool ReadRecord(Slice* record, std::string* scratch);

  // Returns the physical offset of the last record returned by ReadRecord.
  //
  // Undefined before the first call to ReadRecord.
  //
  // 返回通过 ReadRecord 获取到的最后一个 record 的物理偏移量。
  // 调用 ReadRecord 之前该方法返回值未定义。
  uint64_t LastRecordOffset();

 private:
  SequentialFile* const file_; // 指向 log 文件的常量指针
  Reporter* const reporter_; // 读取 log 数据时遇到损坏使用该对象进行汇报，常量指针
  bool const checksum_; // 该 Reader 是否针对文件数据进行校验和比对
  char* const backing_store_; // 从 log file 读取一整个 block 放到 backing_store_，然后将 backing_store_ 封装到 buffer_ 中
  Slice buffer_; // 一整个 block 数据的封装
  bool eof_;   // Last Read() indicated EOF by returning < kBlockSize 最后一个读操作通过返回一个小于 kBlockSize 的值表示已经到达文件尾

  // Offset of the last record returned by ReadRecord.
  // 上一次调用 ReadRecord 方法返回的 record 的起始偏移量，注意这个 record 是逻辑的。
  // 我们管一个 record 分片叫 fragment 也叫物理 record，全部分片构成一个逻辑 record。
  uint64_t last_record_offset_;
  // Offset of the first location past the end of buffer_.
  uint64_t end_of_buffer_offset_; // 迄今为止读入的全部数据的末尾地址的下一个字节，这个是相对于整个 log file 的

  // Offset at which to start looking for the first record to return
  uint64_t const initial_offset_; // 在文件中寻找第一个 record 的起始地址

  // True if we are resynchronizing after a seek (initial_offset_ > 0). In
  // particular, a run of kMiddleType and kLastType records can be silently
  // skipped in this mode
  //
  // initial_offset_ > 0 时，resyncing_ 为 true。
  // 为啥呢？
  // 因为如果 initial_offset 指向的那个 record 类型如果是 MIDDLE，则说明该 record 不完整，所以需要重新同步，
  // 实际操作就是跳过 initial_offset 起始的那些 MIDDLE 和 LAST 类型 record 直至一个 FIRST 类型的 record。
  bool resyncing_;

  // Extend record types with the following special values
  enum { // record type 扩展
    kEof = kMaxRecordType + 1,
    // Returned whenever we find an invalid physical record.
    // Currently there are three situations in which this happens:
    // * The record has an invalid CRC (ReadPhysicalRecord reports a drop)
    // * The record is a 0-length record (No drop is reported)
    // * The record is below constructor's initial_offset (No drop is reported)
    //
    // 当遇到无效的物理 record 时返回该值。
    // 当前有三个场景会导致该情况的发生：
    // * 该 record 的 crc 无效（ReadPhysicalRecord 方法会报告数据丢弃）
    // * 该 record 长度为 0（报告无数据丢弃）
    // * 该 record 起始地址小于构造方法中传入的 initial_offset （报告无数据丢弃）
    kBadRecord = kMaxRecordType + 2
  };

  // Skips all blocks that are completely before "initial_offset_".
  //
  // Returns true on success. Handles reporting.
  //
  // 跳过 initial_offset_ 之前的全部 blocks
  bool SkipToInitialBlock();

  // Return type, or one of the preceding special values
  // 读取一个 block 并将 block 的 data 部分保存到 result。
  // 返回 record 类型，或者前面定义的特殊扩展值
  unsigned int ReadPhysicalRecord(Slice* result);

  // Reports dropped bytes to the reporter.
  // buffer_ must be updated to remove the dropped bytes prior to invocation.
  // 把丢弃的数据大小报告给 reporter。
  // 在调用报告之前必须先从 buffer_ 中移除掉丢弃的数据。
  void ReportCorruption(uint64_t bytes, const char* reason);
  void ReportDrop(uint64_t bytes, const Status& reason);

  // No copying allowed
  Reader(const Reader&);
  void operator=(const Reader&);
};

}  // namespace log
}  // namespace leveldb

#endif  // STORAGE_LEVELDB_DB_LOG_READER_H_

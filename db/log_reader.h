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

// 用于反序列化 log 文件.
class Reader {
 public:
  // 用于报告错误的接口
  class Reporter {
   public:
    virtual ~Reporter();

    // 检测到数据损坏, bytes 是要丢弃的字节数.
    virtual void Corruption(size_t bytes, const Status& status) = 0;
  };

  // 创建一个 Reader 来从 file 中读取和解析 records, 
  // 读取的第一个 record 的起始位置位于文件 initial_offset 或其之后的物理地址. 
  //
  // 如果 reporter 不为空, 则在检测到数据损坏时报告要丢弃的数据估计大小. 
  //
  // 如果 checksum 为 true, 则在可行的条件比对校验和.
  // 
  // 注意, file 和 reporter 的生命期不能短于 Reader 对象. 
  Reader(SequentialFile* file, Reporter* reporter, bool checksum,
         uint64_t initial_offset);

  ~Reader();

  // 读取下一个 record 到 *record 中. 
  // 如果读取成功, 返回 true; 遇到文件尾返回 false. 
  // 直到在该 reader 上执行修改操作或者针对 *scratch 执行修改操作, 
  // 填充到 *record 都是有效的. 
  // 如果当前读取的 record 没有被分片, 那就用不到 *scratch 
  // 参数来为 *record 做底层存储了; 其它情况
  // 需要借助 *scratch 来拼装分片的 record data 部分, 
  // 最后封装为一个 Slice 赋值给 *record. 
  bool ReadRecord(Slice* record, std::string* scratch);

  // 返回通过 ReadRecord 获取到的最后一个 record 的物理偏移量. 
  // 调用 ReadRecord 之前该方法返回值未定义. 
  uint64_t LastRecordOffset();

 private:
  // 指向 log 文件的常量指针
  SequentialFile* const file_;
  // 读取 log 数据时遇到损坏使用该对象进行报告, 常量指针 
  Reporter* const reporter_;
  // 该 Reader 是否针对文件数据进行校验和比对 
  bool const checksum_; 
  // 从 log file 读取一整个 block 放到 backing_store_, 
  // 然后将 backing_store_ 封装到 buffer_ 中
  char* const backing_store_;
  // 一整个 block 数据的封装, 底层存储为 backing_store_
  Slice buffer_;
  // 最后一个读操作通过返回一个小于 kBlockSize 的值表示已经到达文件尾 
  bool eof_;   

  // 上一次调用 ReadRecord 方法返回的 record 的起始偏移量, 
  // 注意这个 record 是逻辑的. 
  uint64_t last_record_offset_;
  // log file 待读取字节位置, 同时也是 log file 已读取数据长度.
  uint64_t end_of_buffer_offset_; 

  // Offset at which to start looking for the first record to return
  // 在文件中寻找第一个 record 的起始地址
  uint64_t const initial_offset_; 

  // initial_offset_ > 0 时, resyncing_ 为 true. 
  // 为啥呢? 
  // 因为如果 initial_offset 指向的那个 record 类型如果是
  // MIDDLE, 则说明该 record 不完整, 所以需要重新同步, 
  // 实际操作就是跳过 initial_offset 起始的那些 MIDDLE
  // 和 LAST 类型 record 直至一个 FIRST 类型的 record. 
  bool resyncing_;

  // Extend record types with the following special values
  enum { // record type 扩展
    kEof = kMaxRecordType + 1,
    // 当遇到无效的物理 record 时返回该值. 
    // 当前有三个场景会导致该情况的发生: 
    // * 该 record 的 crc 无效(ReadPhysicalRecord 方法会报告数据丢弃)
    // * 该 record 长度为 0(报告无数据丢弃)
    // * 该 record 起始地址小于构造方法中传入的 initial_offset (报告无数据丢弃)
    kBadRecord = kMaxRecordType + 2
  };

  // 跳过 initial_offset_ 之前的全部 blocks.
  // 成功跳过返回 true.
  bool SkipToInitialBlock();

  // 读取一个 record 并将其 data 部分保存到 result, 
  // 同时返回该 record 的 type. 
  unsigned int ReadPhysicalRecord(Slice* result);

  // 把丢弃的数据大小报告给 reporter. 
  // 在调用报告之前必须先从 buffer_ 中移除掉丢弃的数据. 
  void ReportCorruption(uint64_t bytes, const char* reason);
  void ReportDrop(uint64_t bytes, const Status& reason);

  // No copying allowed
  Reader(const Reader&);
  void operator=(const Reader&);
};

}  // namespace log
}  // namespace leveldb

#endif  // STORAGE_LEVELDB_DB_LOG_READER_H_

// Copyright (c) 2011 The LevelDB Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file. See the AUTHORS file for names of contributors.

#include "db/log_reader.h"

#include <stdio.h>
#include "leveldb/env.h"
#include "util/coding.h"
#include "util/crc32c.h"

namespace leveldb {
namespace log {

Reader::Reporter::~Reporter() {
}

Reader::Reader(SequentialFile* file, Reporter* reporter, bool checksum,
               uint64_t initial_offset)
    : file_(file),
      reporter_(reporter),
      checksum_(checksum),
      backing_store_(new char[kBlockSize]),
      buffer_(),
      eof_(false),
      last_record_offset_(0),
      end_of_buffer_offset_(0),
      initial_offset_(initial_offset),
      // 如果对 record 起始地址有要求, 需要设置该标识
      resyncing_(initial_offset > 0) {
}

Reader::~Reader() {
  delete[] backing_store_;
}

bool Reader::SkipToInitialBlock() {
  // initial_offset_ 落在了某个 block, 计算 initial_offset_ 相对于该 block 起始位置的偏移量
  const size_t offset_in_block = initial_offset_ % kBlockSize; 
  // initial_offset_ 落在了某个 block, 计算该 block 在 log file 中的起始地址
  uint64_t block_start_location = initial_offset_ - offset_in_block; 

  // 如果我们可能落在了某个 block 的 trailer 空间里, 则直接跳过这部分空间. 
  // 注意 offset_in_block 取值范围为 [0, kBlockSize - 1]
  // trailer 最大只可能是区间 [kBlockSize - 6, kBlockSize - 1]
  if (offset_in_block > kBlockSize - 6) {
    // 如果 offset_in_block 落在了 block 后 6 个字节, 不管有无 trailer 都直接跳到下个 block,
    // 因为这个空间不足与容纳一个 record header(7 字节) 
    block_start_location += kBlockSize;
  }

  end_of_buffer_offset_ = block_start_location;

  if (block_start_location > 0) {
    // 将文件指针移动到我们要读取的第一个 record 所在的 block 在 log file 中的起始地址
    Status skip_status = file_->Skip(block_start_location); 
    if (!skip_status.ok()) {
      ReportDrop(block_start_location, skip_status);
      return false;
    }
  }

  return true;
}

bool Reader::ReadRecord(Slice* record, std::string* scratch) {
  // last_record_offset_ 表示上一次调用 ReadRecord 方法返回的 record 的起始偏移量, 注意这个 record 是逻辑的. 
  // initial_offset_ 表示用户创建 Reader 时指定的在文件中寻找第一个 record 的起始地址.
  // 如果条件成立, 表示当前方法是首次被调用.
  if (last_record_offset_ < initial_offset_) {
    // 跳到我们要读取的第一个 block 起始位置
    if (!SkipToInitialBlock()) {
      return false;
    }
  }

  scratch->clear();
  record->clear();
  // 指示正在处理的 record 是否被分片了
  bool in_fragmented_record = false; 

  // 记录我们正在读取的逻辑 record 的起始偏移量. 初值为 0 无实际意义仅为编译器不发警告. 
  // 为啥叫逻辑 record 呢？
  // 因为 block 大小限制, 所以 record 可能被分成多个分片(fragment). 
  // 我们管 fragment 叫物理 record, 一个或多个物理 record 构成一个逻辑 record. 
  uint64_t prospective_record_offset = 0;

  Slice fragment;
  while (true) {
    // 从文件读取一个物理 record 并将其 data 部分保存到 fragment, 同时返回该 record 的 type.
    const unsigned int record_type = ReadPhysicalRecord(&fragment);

    // 计算返回的当前 record 在 log file 中的起始地址=
    //    当前文件待读取位置-buffer剩余字节数-刚读取的record头大小-刚读取record数据部分大小
    // end_of_buffer_offset_ 表示 log file 待读取字节位置, 或者, log file 已读取数据长度.
    // buffer_ 表示是对一整个 block 数据的封装, 底层存储为 backing_store_, 
    //    每次执行 ReadPhysicalRecord 时会移动 buffer_ 指针.
    uint64_t physical_record_offset =
        end_of_buffer_offset_ - buffer_.size() - kHeaderSize - fragment.size(); 

    // resyncing_ 用于跳过起始地址不符合 initial_offset_ 的 record,
    // 如果为 true 表示目前还在定位第一个满足条件的逻辑 record 中.
    // 与 initial_offset_ 的比较判断在上面 ReadPhysicalRecord 中进行.
    if (resyncing_) {
      // 只要数据没有损坏或到达文件尾, 而且返回的 record_type 只要
      // 不是 kBadRecord(返回该类型其中一个情况就是起始地址不满足条件)
      // 就说明当前 record 起始地址已经大于 initial_offset_ 了,
      // 但是如果当前 record 的 type 为 middle 或者 last, 
      // 那么逻辑上这个 record 仍然与不符合 initial_offset_ 的
      // 类型为 first 的 record 同属一个逻辑 record, 所以当前 record 也不是我们要的.
      if (record_type == kMiddleType) {
        continue;
      } else if (record_type == kLastType) {
        resyncing_ = false;
        continue;
      } else {
        // 如果是 full 类型的 record, 而且这个 record 起始地址不小于 inital_offset_(否则
        //    ReadPhysicalRecord 返回的类型就是 kBadRecord 而非 full), 满足条件了, 关掉标识.
        // 如果返回 kBadRecord/kEof(没什么可读了)/未知类型(但是起始位置满足要求), 
        //    这里也会关掉该标识.
        resyncing_ = false;
      }
    }

    // 注意, 下面 switch 有的 case 是 return, 有的是 break.
    switch (record_type) {
      case kFullType:
        if (in_fragmented_record) {
          // 早期版本 writer 实现存在 bug. 即如果上一个 block 末尾保存的是一个 FIRST 类型的 header, 
          // 那么接下来 block 开头应该是一个 MIDDLE 类型的 record, 但是早期版本写入了 FIRST 类型或者
          // FULL 类型的 record. 
          if (!scratch->empty()) {
            ReportCorruption(scratch->size(), "partial record without end(1)");
          }
        }
        prospective_record_offset = physical_record_offset;
        scratch->clear();
        // 赋值构造
        // FULL 类型 record 不用借助 scratch 拼装了
        *record = fragment; 
        last_record_offset_ = prospective_record_offset;
        // 读取到一个完整逻辑 record, 完成任务.
        return true;
      // 注意, 只有 first 类型的 record 起始地址满足大于 inital_offset_ 的时候
      // 才会返回其真实类型 first, 其它情况哪怕是 first 返回也是 kBadRecord.
      case kFirstType:
        if (in_fragmented_record) {
          // 早期版本 writer 实现存在 bug. 即如果上一个 block 末尾保存的是一个 FIRST 类型的 header, 
          // 那么接下来 block 开头应该是一个 MIDDLE 类型的 record, 但是早期版本写入了 FIRST 类型或者
          // FULL 类型的 record. 
          if (!scratch->empty()) {
            ReportCorruption(scratch->size(), "partial record without end(2)");
          }
        }
        // FIRST 类型物理 record 起始地址也是对应逻辑 record 的起始地址
        prospective_record_offset = physical_record_offset;
        // 非 FULL 类型 record 需要借助 scratch 拼装成一个完整的 record data 部分.
        // 注意只有 first 时采用 assign, first 后面的分片要用 append
        scratch->assign(fragment.data(), fragment.size());
        // 除了 FULL 类型 record, 都说明当前读取的 record 被分片了, 还需要后续继续读取.
        in_fragmented_record = true; 
        // 刚读了 first, 没读完, 继续.
        break;

      case kMiddleType:
        // 都存在 MIDDLE 了, 竟然还说当前 record 没分片, 报错. 
        if (!in_fragmented_record) { 
          ReportCorruption(fragment.size(),
                           "missing start of fragmented record(1)");
        } else {
          // 非 FULL 类型 record 需要借助 scratch 拼装成一个完整的 record data 部分, 
          // FIRST 类型已经打底了, MIDDLE 直接追加即可. 
          scratch->append(fragment.data(), fragment.size());
        }
        // 还是 middle, 没读完, 继续.
        break;

      case kLastType:
        // 都存在 LAST 了, 竟然还说当前 record 没分片, 矛盾. 
        if (!in_fragmented_record) { 
          ReportCorruption(fragment.size(),
                           "missing start of fragmented record(2)");
        } else {
          // 非 FULL 类型 record 需要借助 scratch 拼装成一个完整的 record data 部分, 
          // FIRST 类型已经打底了, LAST 直接追加即可. 
          scratch->append(fragment.data(), fragment.size());
          *record = Slice(*scratch);
          last_record_offset_ = prospective_record_offset;
          // 读完了, 完成任务.
          return true;
        }
        break;

      case kEof:
        // 如果都读到文件尾部了, 逻辑 record 还没读全, 那就是文件损坏了. 
        if (in_fragmented_record) {
          // 可能由于 writer 写完一个物理 record 后挂掉了, 我们不把这种情况作为数据损坏, 直接忽略整个逻辑 record.
          // 数据损坏, 丢掉之前可能已经解析的数据 
          scratch->clear();
        }
        // 文件尾了, 读到读不到都拜拜.
        return false;

      case kBadRecord:
        // 逻辑 record 有分片已经被读取过了, 但是本次读取物理 record 遇到了错误
        if (in_fragmented_record) { 
          ReportCorruption(scratch->size(), "error in middle of record");
          in_fragmented_record = false;
          // 数据损坏, 丢掉之前可能已经解析的数据
          scratch->clear();
        }
        // 遇到错误也不中止, 继续读取数据进行解析直到读取完整逻辑 record 的目标达成
        break;

      default: {
        char buf[40];
        snprintf(buf, sizeof(buf), "unknown record type %u", record_type);
        ReportCorruption(
            (fragment.size() + (in_fragmented_record ? scratch->size() : 0)),
            buf);
        in_fragmented_record = false;
        scratch->clear();
        // 遇到错误也不中止, 继续读取数据进行解析直到读取完整逻辑 record 的目标达成
        break;
      }
    }
  }
  return false;
}

uint64_t Reader::LastRecordOffset() {
  return last_record_offset_;
}

void Reader::ReportCorruption(uint64_t bytes, const char* reason) {
  ReportDrop(bytes, Status::Corruption(reason));
}

void Reader::ReportDrop(uint64_t bytes, const Status& reason) {
  if (reporter_ != nullptr &&
      end_of_buffer_offset_ - buffer_.size() - bytes >= initial_offset_) {
    reporter_->Corruption(static_cast<size_t>(bytes), reason);
  }
}

// 读取一个 record 并将其 data 部分保存到 result, 同时返回该 record 的 type. 
// 返回的 type 为下面几种之一:
// - kEof, 到达文件尾
// - kBadRecord, 当前 record 损坏, 或者当前物理 record 起始地址小于用户指定的起始地址 inital_offset_(此时其实际 type 可能为 first) 
// - first/middle/last/full 之一(注意, 除了 first/full, 其它类型对应物理 record 起始地址虽然不小于用户指定的 inital_offset_, 但是其所归属的逻辑 record 的起始地址可能不满足要求, 所以此时这两类物理 record 也会被 `Reader::ReadRecord` 方法跳过.)
unsigned int Reader::ReadPhysicalRecord(Slice* result) {
  // 跳出循环需要满足下面条件之一:
  // - 文件损坏
  // - 无有效数据(非 tailer)且到达文件尾
  // - 读到了一个有效 block(然后解析其中的 record)
  while (true) {
    // 先确认要不要读取一个新的 blcok.
    // buffer_ 底层指针会向前移动, 所以其 size 是动态的. 
    // 如果 buffer 剩余内容字节数小于 kHeaderSize 且不为空, 
    // 表示 buffer 里面剩余字节是个 trailer, 可以跳过它去读取解析下个 block 了;
    // 否则, 跳过 if 继续从该 block 解析 record.
    if (buffer_.size() < kHeaderSize) {
      if (!eof_) { // 如果未到文件尾
        // Last read was a full read, so this is a trailer to skip
        buffer_.clear();
        // 从 log file 读取一整个 block 放到 backing_store_, 
        // 然后将 backing_store_ 封装到 buffer_ 中. 
        Status status = file_->Read(kBlockSize, &buffer_, backing_store_);
        // 更新 end_of_buffer_offset_ 至迄今从 log 读取最大位置下一个字节
        end_of_buffer_offset_ += buffer_.size(); 
        // 如果 log 文件损坏
        if (!status.ok()) {
          buffer_.clear();
          // 一个 block 被丢掉
          ReportDrop(kBlockSize, status);
          // 读文件失败我们认为到达文件尾; 
          // 注意, file_->Read 读到文件尾不会报错因为这是正常情况.
          // 只有遇到错误才会报错. 
          eof_ = true; 
          return kEof;
        } else if (buffer_.size() < kBlockSize) {
          // 如果读取的 block 数据小于 block 容量, 则肯定到达 log 文件尾部了, 处理其中的 records.
          eof_ = true; 
        }
        continue;
      } else {
        // 注意, 如果 buffer_ 非空, 则其内容为一个位于文件尾的截断的 record header. 
        // 这可能是因为 writer 写 header 时崩溃导致的. 
        // 我们不会把这种情况当做错误, 而是当做读作文件尾来处理. 
        buffer_.clear();
        return kEof;
      }
    }

    /**
     * 注意, 一个 block 可以包含多个 records, 
     * 但是 block 最后一个 record 可能只包含 header(这是由于 block 最后只剩下 7 个字节)
     */
    // 解析 record 的 header.
    // record header, 由 checksum (4 bytes), length (2 bytes), type (1 byte) 构成. 
    const char* header = buffer_.data(); 
    /**
     * 下面两步骤是解析 length 的两个字节, 小端字节序, 所以需要拼, 具体见 Writer::EmitPhysicalRecord
     */
    // xxxx|(xx|x|x)xxxxx 将左边括号内容转换为无符号 32 位数, 并取出最后 8 位即括号左起第一个 x
    const uint32_t a = static_cast<uint32_t>(header[4]) & 0xff;
    // xxxx|x(x|x|xx)xxxx 将左边括号内容转换为无符号 32 位数, 并取出最后 8 位即括号左起第一个 x
    const uint32_t b = static_cast<uint32_t>(header[5]) & 0xff;
    // xxxx|xx|(x)|xxxxxx 读取左边括号内容即 record type
    const unsigned int type = header[6]; 
    // b 和 a 拼接构成了 length
    const uint32_t length = a | (b << 8); 
    // 如果解析出的 length 加上 header 长度大于 buffer_ 剩余数据长度, 
    // 则说明数据损坏了, 比如 length 被篡改了. 
    if (kHeaderSize + length > buffer_.size()) {
      size_t drop_size = buffer_.size();
      buffer_.clear();
      // 如果未到文件尾, 报告 length 损坏. 
      if (!eof_) { 
        ReportCorruption(drop_size, "bad record length");
        return kBadRecord;
      }

      // 如果已经到了文件尾, 即当前读取 block 为 log file 最后一个 block.
      // 由于 length  有问题我们也没必要也没办法读取 data 部分, 我们假设这种情况原因是
      // writer 写数据时崩溃了. 这种情况我们不作为错误去报告, 而是当做到达文件尾了. 
      return kEof;
    }

    if (type == kZeroType && length == 0) {
      // 跳过 0 长度的 record, 而且不会报告数据丢弃. 
      // 因为这些 records 产生的原因是 env_posix.cc 中基于 mmap 的写入代码在执行时会预分配文件区域. 
      buffer_.clear();
      return kBadRecord;
    }

    // Check crc
    if (checksum_) {
      // 读取 crc
      uint32_t expected_crc = crc32c::Unmask(DecodeFixed32(header)); 
      // crc 是基于 type 和 data 来计算的
      uint32_t actual_crc = crc32c::Value(header + 6, 1 + length); 
      if (actual_crc != expected_crc) {
        // crc 校验失败, 可能是 length 字段出错, 数据损坏, 丢弃这个 block 的剩余部分
        size_t drop_size = buffer_.size();
        buffer_.clear();
        ReportCorruption(drop_size, "checksum mismatch");
        return kBadRecord;
      }
    }

    // header 解析完毕, 将当前 record 从 buffer_ 中移除(通过向前移动 buffer_ 底层存储指针实现)
    buffer_.remove_prefix(kHeaderSize + length); 

    // 当前解析出的 record 起始地址小于用户指定的起始地址, 则跳过这个 record. 
    if (end_of_buffer_offset_ - buffer_.size() - kHeaderSize - length < initial_offset_) {
      result->clear();
      return kBadRecord;
    }
    
    // 将该 record 的 data 部分返回
    *result = Slice(header + kHeaderSize, length); 
    return type;
  }
}

}  // namespace log
}  // namespace leveldb

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
      resyncing_(initial_offset > 0) {
}

Reader::~Reader() {
  delete[] backing_store_;
}

bool Reader::SkipToInitialBlock() {
  const size_t offset_in_block = initial_offset_ % kBlockSize; // initial_offset_ 落在了某个 block，计算 initial_offset_ 相对于该 block 起始位置的偏移量
  uint64_t block_start_location = initial_offset_ - offset_in_block; // initial_offset_ 落在了某个 block，计算该 block 在 log file 中的起始地址

  // Don't search a block if we'd be in the trailer
  // 如果我们可能落在了某个 block 的 trailer 空间里，则直接跳过这部分空间。
  // 注意 offset_in_block 取值范围为 [0, kBlockSize - 1]
  if (offset_in_block > kBlockSize - 6) { // trailer 最大只可能是区间 [kBlockSize - 6, kBlockSize - 1]
    block_start_location += kBlockSize; // 如果 offset_in_block 落在了 block 后 6 个字节，不管有无 trailer 都直接跳到下个 block
  }

  end_of_buffer_offset_ = block_start_location;

  // Skip to start of first block that can contain the initial record
  if (block_start_location > 0) {
    Status skip_status = file_->Skip(block_start_location); // 将文件指针移动到我们要读取的第一个 block 在 log file 的起始地址
    if (!skip_status.ok()) {
      ReportDrop(block_start_location, skip_status);
      return false;
    }
  }

  return true;
}

bool Reader::ReadRecord(Slice* record, std::string* scratch) {
  if (last_record_offset_ < initial_offset_) { // 表示这是第一次读取 record
    if (!SkipToInitialBlock()) { // 跳到我们要读取的第一个 block 起始位置
      return false;
    }
  }

  scratch->clear();
  record->clear();
  bool in_fragmented_record = false; // 指示正在处理的 record 是否被分片了
  // Record offset of the logical record that we're reading
  // 0 is a dummy value to make compilers happy
  //
  // 记录我们正在读取的逻辑 record 的起始偏移量。初值为 0 是为了让编译器高兴。
  // 为啥叫逻辑 record 呢？
  // 因为 record 可能被分片，所以我们管 fragment 叫物理 record，1 或 多个
  // 物理 record 构成一个逻辑 record。
  uint64_t prospective_record_offset = 0;

  Slice fragment;
  while (true) {
    const unsigned int record_type = ReadPhysicalRecord(&fragment);

    // ReadPhysicalRecord may have only had an empty trailer remaining in its
    // internal buffer. Calculate the offset of the next physical record now
    // that it has returned, properly accounting for its header size.
    //
    // ReadPhysicalRecord 处理完了，buffer 里面可能只剩下一个 trailer 了。计算下个 record 的地址
    //
    uint64_t physical_record_offset =
        end_of_buffer_offset_ - buffer_.size() - kHeaderSize - fragment.size(); // ReadPhysicalRecord 读取的 record 的起始地址

    // 下面处理逻辑见 resyncing_ 声明部分注释
    if (resyncing_) {
      if (record_type == kMiddleType) {
        continue;
      } else if (record_type == kLastType) {
        resyncing_ = false;
        continue;
      } else {
        resyncing_ = false;
      }
    }

    switch (record_type) {
      case kFullType:
        if (in_fragmented_record) {
          // Handle bug in earlier versions of log::Writer where
          // it could emit an empty kFirstType record at the tail end
          // of a block followed by a kFullType or kFirstType record
          // at the beginning of the next block.
          //
          // 早期版本 writer 实现存在 bug。即如果上一个 block 末尾保存的是一个 FIRST 类型的 header，
          // 那么接下来 block 开头应该是一个 MIDDLE 类型的 record，但是早期版本写入了 FIRST 类型或者
          // FULL 类型的 record。
          if (!scratch->empty()) {
            ReportCorruption(scratch->size(), "partial record without end(1)");
          }
        }
        prospective_record_offset = physical_record_offset;
        scratch->clear();
        *record = fragment; // FULL 类型 record 不用借助 scratch 拼装了
        last_record_offset_ = prospective_record_offset;
        return true;

      case kFirstType:
        if (in_fragmented_record) {
          // Handle bug in earlier versions of log::Writer where
          // it could emit an empty kFirstType record at the tail end
          // of a block followed by a kFullType or kFirstType record
          // at the beginning of the next block.
          if (!scratch->empty()) {
            ReportCorruption(scratch->size(), "partial record without end(2)");
          }
        }
        // FIRST 类型物理 record 起始地址也是对应逻辑 record 的起始地址
        prospective_record_offset = physical_record_offset;
        // 非 FULL 类型 record 需要借助 scratch 拼装成一个完整的 record data 部分
        scratch->assign(fragment.data(), fragment.size());
        in_fragmented_record = true; // 除了 FULL 类型 record，都说明当前读取的 record 被分片了
        break;

      case kMiddleType:
        if (!in_fragmented_record) { // 都存在 MIDDLE 了，竟然还说当前 record 没分片，矛盾。
          ReportCorruption(fragment.size(),
                           "missing start of fragmented record(1)");
        } else {
          // 非 FULL 类型 record 需要借助 scratch 拼装成一个完整的 record data 部分，
          // FIRST 类型已经打底了，MIDDLE 直接追加即可。
          scratch->append(fragment.data(), fragment.size());
        }
        break;

      case kLastType:
        if (!in_fragmented_record) { // 都存在 LAST 了，竟然还说当前 record 没分片，矛盾。
          ReportCorruption(fragment.size(),
                           "missing start of fragmented record(2)");
        } else {
          // 非 FULL 类型 record 需要借助 scratch 拼装成一个完整的 record data 部分，
          // FIRST 类型已经打底了，LAST 直接追加即可。
          scratch->append(fragment.data(), fragment.size());
          *record = Slice(*scratch);
          last_record_offset_ = prospective_record_offset;
          return true;
        }
        break;

      case kEof:
        if (in_fragmented_record) { // 如果都读到文件尾部了，逻辑 record 还没读全，那就是 bug 了。
          // This can be caused by the writer dying immediately after
          // writing a physical record but before completing the next; don't
          // treat it as a corruption, just ignore the entire logical record.
          //
          // 可能由于 writer 写完一个物理 record 后挂掉了，我们不把这种情况作为数据损坏，直接忽略整个逻辑 record。
          scratch->clear();
        }
        return false;

      case kBadRecord:
        if (in_fragmented_record) { // 读取物理 record 遇到了错误
          ReportCorruption(scratch->size(), "error in middle of record");
          in_fragmented_record = false;
          scratch->clear();
        }
        break;

      default: {
        char buf[40];
        snprintf(buf, sizeof(buf), "unknown record type %u", record_type);
        ReportCorruption(
            (fragment.size() + (in_fragmented_record ? scratch->size() : 0)),
            buf);
        in_fragmented_record = false;
        scratch->clear();
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

unsigned int Reader::ReadPhysicalRecord(Slice* result) {
  while (true) {
    // buffer_ 底层指针会向前移动，所以其 size 是动态的。
    // 如果小于 kHeaderSize，表示 buffer 里面剩余字节是个 trailer，可以跳过，去解析下个 block 了。
    if (buffer_.size() < kHeaderSize) {
      if (!eof_) { // 如果未到文件尾
        // Last read was a full read, so this is a trailer to skip
        buffer_.clear();
        // 从 log file 读取一整个 block 放到 backing_store_，然后将 backing_store_ 封装到 buffer_ 中。
        Status status = file_->Read(kBlockSize, &buffer_, backing_store_);
        end_of_buffer_offset_ += buffer_.size(); // 更新 end_of_buffer_offset_ 至迄今读取最大位置下一个字节
        if (!status.ok()) {
          buffer_.clear();
          ReportDrop(kBlockSize, status);
          eof_ = true; // 读文件失败我们认为到达文件尾；注意，file_->Read 读到文件尾也不会报错因为这是正常情况，只有遇到错误才会报错。
          return kEof;
        } else if (buffer_.size() < kBlockSize) {
          eof_ = true; // 如果读取的 block 数据小于一个 block 大小，则肯定到达文件尾了
        }
        continue;
      } else {
        // Note that if buffer_ is non-empty, we have a truncated header at the
        // end of the file, which can be caused by the writer crashing in the
        // middle of writing the header. Instead of considering this an error,
        // just report EOF.
        //
        // 注意，如果 buffer_ 非空，则其内容为一个位于文件尾的截断的 record header。
        // 这可能是因为 writer 写 header 时崩溃导致的。
        // 我们不会把这种情况当做错误，而是当做读作文件尾来处理。
        buffer_.clear();
        return kEof;
      }
    }

    /**
     * 注意，一个 block 可以包含多个 records，但是 block 最后一个 record 可能只包含 header（这是由于 block 最后只剩下 7 个字节）
     */
    // Parse the header
    const char* header = buffer_.data(); // 即 record header，由 checksum (4 bytes), length (2 bytes), type (1 byte) 构成。
    /**
     * 下面两步骤是解析 length 的两个字节，类小端字节序，所以需要拼，具体见 Writer::EmitPhysicalRecord
     */
    // xxxx|(xx|x|x)xxxxx 将左边括号内容转换为无符号 32 位数，并取出最后 8 位即括号左起第一个 x
    const uint32_t a = static_cast<uint32_t>(header[4]) & 0xff;
    // xxxx|x(x|x|xx)xxxx 将左边括号内容转换为无符号 32 位数，并取出最后 8 位即括号左起第一个 x
    const uint32_t b = static_cast<uint32_t>(header[5]) & 0xff;
    const unsigned int type = header[6]; // xxxx|xx|（x）|xxxxxx 读取左边括号内容即 record type
    const uint32_t length = a | (b << 8); // b 和 a 拼接构成了 length
    // 应该小于（block 尾部是一个 trailer 时）等于。
    // 注意即使 block 最后只有一个 7 字节长 header，因为 length 应为 0 所以也不应该大于。
    if (kHeaderSize + length > buffer_.size()) {
      size_t drop_size = buffer_.size();
      buffer_.clear();
      if (!eof_) { // 如果未到文件尾，报告 length 损坏。
        ReportCorruption(drop_size, "bad record length");
        return kBadRecord;
      }
      // If the end of the file has been reached without reading |length| bytes
      // of payload, assume the writer died in the middle of writing the record.
      // Don't report a corruption.
      //
      // 如果已经到了文件尾，即当前读取 block 为 log file 最后一个 block，
      // 而且还未读取长度为 length 的 data 部分，这肯定出问题了，我们假设是
      // writer 写数据时崩溃了，这种情况我们不作为错误去报告，而是当做到达文件尾了。
      return kEof;
    }

    if (type == kZeroType && length == 0) {
      // Skip zero length record without reporting any drops since
      // such records are produced by the mmap based writing code in
      // env_posix.cc that preallocates file regions.
      //
      // 跳过 0 长度的 record，而且不作为数据丢弃错误进行报告。
      // 因为这些 records 是通过在 env_posix.cc 中基于 mmap 的写入代码预分配的文件区域而生成的。
      buffer_.clear();
      return kBadRecord;
    }

    // Check crc
    if (checksum_) {
      uint32_t expected_crc = crc32c::Unmask(DecodeFixed32(header)); // 读取 crc
      uint32_t actual_crc = crc32c::Value(header + 6, 1 + length); // crc 是基于 type 和 data 来计算的
      if (actual_crc != expected_crc) { // crc 校验失败，数据损坏，丢弃这个 block
        // Drop the rest of the buffer since "length" itself may have
        // been corrupted and if we trust it, we could find some
        // fragment of a real log record that just happens to look
        // like a valid log record.
        size_t drop_size = buffer_.size();
        buffer_.clear();
        ReportCorruption(drop_size, "checksum mismatch");
        return kBadRecord;
      }
    }

    buffer_.remove_prefix(kHeaderSize + length); // header 解析完毕，移动到 data 末尾的下一个字节

    // Skip physical record that started before initial_offset_
    // 真费劲，小于号左边结果其实就是 block 中存储的 record 在 log file 中的起始地址，
    // 如果这个地址小于用户传进来的要读取 record 的初始地址，则跳过这个 record。
    if (end_of_buffer_offset_ - buffer_.size() - kHeaderSize - length <
        initial_offset_) {
      result->clear();
      return kBadRecord;
    }

    *result = Slice(header + kHeaderSize, length); // 将该 record data 作为一个 fragment 部分返回
    return type;
  }
}

}  // namespace log
}  // namespace leveldb

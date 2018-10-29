// Copyright (c) 2011 The LevelDB Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file. See the AUTHORS file for names of contributors.

#include "table/format.h"

#include "leveldb/env.h"
#include "port/port.h"
#include "table/block.h"
#include "util/coding.h"
#include "util/crc32c.h"

namespace leveldb {

// 将一个 BlocckHandle 编码为 varint64 并写到 dst 指向内存位置
void BlockHandle::EncodeTo(std::string* dst) const {
  // Sanity check that all fields have been set
  assert(offset_ != ~static_cast<uint64_t>(0));
  assert(size_ != ~static_cast<uint64_t>(0));
  PutVarint64(dst, offset_);
  PutVarint64(dst, size_);
}

// 将 input 指向的内容解码为一个 BlockHandle。
// 成功返回 OK；否则返回 not-OK.
Status BlockHandle::DecodeFrom(Slice* input) {
  if (GetVarint64(input, &offset_) &&
      GetVarint64(input, &size_)) {
    return Status::OK();
  } else {
    return Status::Corruption("bad block handle");
  }
}

// 将一个 Footer 编码写入到 dst 指向内存，
// 包括将两个 BlockHandle 分别编码写入内存，
// 然后通过 string::resize 做 padding，
// 最后将 8 字节魔数按照小端模式追加进来。
void Footer::EncodeTo(std::string* dst) const {
  const size_t original_size = dst->size(); // dst 本来为空才行，否则两个 BlockHandle 恰好为 40 字节那后面 resize 可能丢数据。
  metaindex_handle_.EncodeTo(dst);
  index_handle_.EncodeTo(dst);
  dst->resize(2 * BlockHandle::kMaxEncodedLength);  // Padding
  PutFixed32(dst, static_cast<uint32_t>(kTableMagicNumber & 0xffffffffu));
  PutFixed32(dst, static_cast<uint32_t>(kTableMagicNumber >> 32));
  assert(dst->size() == original_size + kEncodedLength);
  (void)original_size;  // Disable unused variable warning.
}

// 从 input 指向内存解码出一个 Footer，
// 先解码最后 8 字节的魔数（按照小端模式），
// 然后一次解码两个 BlockHandle。
Status Footer::DecodeFrom(Slice* input) {
  const char* magic_ptr = input->data() + kEncodedLength - 8;
  const uint32_t magic_lo = DecodeFixed32(magic_ptr);
  const uint32_t magic_hi = DecodeFixed32(magic_ptr + 4);
  const uint64_t magic = ((static_cast<uint64_t>(magic_hi) << 32) |
                          (static_cast<uint64_t>(magic_lo)));
  if (magic != kTableMagicNumber) {
    return Status::Corruption("not an sstable (bad magic number)");
  }

  Status result = metaindex_handle_.DecodeFrom(input);
  if (result.ok()) {
    result = index_handle_.DecodeFrom(input);
  }
  if (result.ok()) { // 此时 input 包含的数据只有可能的 padding 0 了
    // We skip over any leftover data (just padding for now) in "input"
    const char* end = magic_ptr + 8;
    *input = Slice(end, input->data() + input->size() - end); // todo Slice 第二个参数为负数，生成这样的对象的目的为何呢？
  }
  return result;
}

Status ReadBlock(RandomAccessFile* file,
                 const ReadOptions& options,
                 const BlockHandle& handle,
                 BlockContents* result) {
  result->data = Slice();
  result->cachable = false;
  result->heap_allocated = false;

  /**
   * 解析 block
   */
  // Read the block contents as well as the type/crc footer.
  // See table_builder.cc for the code that built this structure.
  size_t n = static_cast<size_t>(handle.size()); // 要读取的 block 的大小
  char* buf = new char[n + kBlockTrailerSize]; // 每个 block 后面跟着它的 type （1 字节）和 crc （4 字节）
  Slice contents;
  // handle.offset() 指向对应 block 在文件里的起始偏移量
  Status s = file->Read(handle.offset(), n + kBlockTrailerSize, &contents, buf);
  if (!s.ok()) {
    delete[] buf;
    return s;
  }
  if (contents.size() != n + kBlockTrailerSize) {
    delete[] buf;
    return Status::Corruption("truncated block read");
  }

  /**
   * 校验 crc
   */
  // Check the crc of the type and the block contents
  const char* data = contents.data();    // Pointer to where Read put the data
  if (options.verify_checksums) {
    // 将 block 的 CRC 解除掩码并与重新计算的 CRC 进行比对
    const uint32_t crc = crc32c::Unmask(DecodeFixed32(data + n + 1));
    const uint32_t actual = crc32c::Value(data, n + 1);
    if (actual != crc) {
      delete[] buf;
      s = Status::Corruption("block checksum mismatch");
      return s;
    }
  }

  /**
   * 解析 type，并根据 type 解析 block data
   */
  // type 表示 block 的压缩状态
  switch (data[n]) {
    case kNoCompression:
      if (data != buf) {
        // File implementation gave us pointer to some other data.
        // Use it directly under the assumption that it will be live
        // while the file is open.
        delete[] buf;
        result->data = Slice(data, n);
        result->heap_allocated = false;
        result->cachable = false;  // Do not double-cache
      } else {
        result->data = Slice(buf, n);
        result->heap_allocated = true;
        result->cachable = true;
      }

      // Ok
      break;
    case kSnappyCompression: {
      size_t ulength = 0;
      // 获取 snappy 压缩前的数据的大小以分配内存
      if (!port::Snappy_GetUncompressedLength(data, n, &ulength)) {
        delete[] buf;
        return Status::Corruption("corrupted compressed block contents");
      }
      char* ubuf = new char[ulength];
      // 将 snappy 压缩过的数据解压缩到上面分配的内存中
      if (!port::Snappy_Uncompress(data, n, ubuf)) {
        delete[] buf;
        delete[] ubuf;
        return Status::Corruption("corrupted compressed block contents");
      }
      delete[] buf;
      result->data = Slice(ubuf, ulength);
      result->heap_allocated = true;
      result->cachable = true;
      break;
    }
    default:
      delete[] buf;
      return Status::Corruption("bad block type");
  }

  return Status::OK();
}

}  // namespace leveldb

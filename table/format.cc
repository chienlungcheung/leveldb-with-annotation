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

// 将 input 指向的内容解码为一个 BlockHandle. 
// 成功返回 OK; 否则返回 not-OK.
Status BlockHandle::DecodeFrom(Slice* input) {
  if (GetVarint64(input, &offset_) &&
      GetVarint64(input, &size_)) {
    return Status::OK();
  } else {
    return Status::Corruption("bad block handle");
  }
}

// 将一个 Footer 编码写入到 dst 指向内存, 
// 包括将两个 BlockHandle 分别编码写入内存, 
// 然后通过 string::resize 做 padding, 
// 最后将 8 字节魔数按照小端模式追加进来. 
void Footer::EncodeTo(std::string* dst) const {
  const size_t original_size = dst->size(); // dst 本来为空才行, 否则两个 BlockHandle 恰好为 40 字节那后面 resize 可能丢数据. 
  metaindex_handle_.EncodeTo(dst);
  index_handle_.EncodeTo(dst);
  dst->resize(2 * BlockHandle::kMaxEncodedLength);  // Padding
  PutFixed32(dst, static_cast<uint32_t>(kTableMagicNumber & 0xffffffffu));
  PutFixed32(dst, static_cast<uint32_t>(kTableMagicNumber >> 32));
  assert(dst->size() == original_size + kEncodedLength);
  (void)original_size;  // Disable unused variable warning.
}

// 从 input 指向内存解码出一个 Footer, 
// 先解码最后 8 字节的魔数(按照小端模式), 
// 然后一次解码两个 BlockHandle. 
Status Footer::DecodeFrom(Slice* input) {
  // 1 按照小端模式解析末尾 8 字节的魔数
  const char* magic_ptr = input->data() + kEncodedLength - 8;
  const uint32_t magic_lo = DecodeFixed32(magic_ptr);
  const uint32_t magic_hi = DecodeFixed32(magic_ptr + 4);
  const uint64_t magic = ((static_cast<uint64_t>(magic_hi) << 32) |
                          (static_cast<uint64_t>(magic_lo)));
  if (magic != kTableMagicNumber) {
    return Status::Corruption("not an sstable (bad magic number)");
  }

  // 2 解析 meta-index block 的 handle
  // (包含 meta index block 起始偏移量及其长度)
  Status result = metaindex_handle_.DecodeFrom(input);
  if (result.ok()) {
    // 3 解析 index block 的 handle
    // (包含 index block 起始偏移量及其长度)
    result = index_handle_.DecodeFrom(input);
  }
  if (result.ok()) { 
    // 4 跳过 padding
    // meta-index handle + data-index handle + padding + 魔数.
    // 此时 input 包含的数据只剩下可能的 padding 0 了, 跳过.
    // end 为 footer 尾部
    const char* end = magic_ptr + 8;
    // 第二个参数为值为 0. 生成下面这个 slice 后面没有使用. 
    *input = Slice(end, input->data() + input->size() - end); 
  }
  return result;
}

// 从 file 去读 handle 指向的 block:
// - 读取整个块, 包含数据+压缩类型(1 字节)+crc(4 字节)
// - 校验 crc: 重新计算 crc 并与保存 crc 比较
// - 解析压缩类型, 根据压缩类型对数据进行解压缩
// - 将 block 数据部分保存到 BlockContents 中
// 失败返回 non-OK; 成功则将数据填充到 *result 并返回 OK. 
Status ReadBlock(RandomAccessFile* file,
                 const ReadOptions& options,
                 const BlockHandle& handle,
                 BlockContents* result) {
  result->data = Slice();
  result->cachable = false;
  result->heap_allocated = false;

  /**
   * 解析 block.
   * 读取 block 内容以及 type 和 crc. 
   * 具体见 table_builder.cc 中构造这个结构的代码.
   */
  // 要读取的 block 的大小
  size_t n = static_cast<size_t>(handle.size()); 
  // 每个 block 后面紧跟着它的压缩类型 type (1 字节)和 crc (4 字节)
  char* buf = new char[n + kBlockTrailerSize]; 
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
   * 校验 type 和 block 内容加在一起对应的 crc
   */
  const char* data = contents.data();
  if (options.verify_checksums) {
    // 读取 block 末尾的 crc(始于第 n+1 字节)
    const uint32_t crc = crc32c::Unmask(DecodeFixed32(data + n + 1));
    // 计算 block 前 n+1 字节的 crc
    const uint32_t actual = crc32c::Value(data, n + 1);
    // 比较保存的 crc 和实际计算的 crc 是否一致
    if (actual != crc) {
      delete[] buf;
      s = Status::Corruption("block checksum mismatch");
      return s;
    }
  }

  /**
   * 解析 type, 并根据 type 解析 block data
   */
  // type 表示 block 的压缩状态
  switch (data[n]) {
    case kNoCompression:
      if (data != buf) {
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

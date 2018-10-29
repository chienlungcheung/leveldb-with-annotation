// Copyright (c) 2011 The LevelDB Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file. See the AUTHORS file for names of contributors.

#ifndef STORAGE_LEVELDB_TABLE_FORMAT_H_
#define STORAGE_LEVELDB_TABLE_FORMAT_H_

#include <string>
#include <stdint.h>
#include "leveldb/slice.h"
#include "leveldb/status.h"
#include "leveldb/table_builder.h"

//leveldb sstable 文件格式:
//
//<beginning_of_file>
//[data block 1]
//[data block 2]
//...
//[data block N]
//[meta block 1]
//...
//[meta block K]
//[metaindex block]
//[index block]
//[Footer]        (fixed size; starts at file_size - sizeof(Footer))
//<end_of_file>
//
//Footer 格式：
//metaindex_handle: char[p];     // 指向上面 [metaindex block] 的 BlockHandle
//index_handle:     char[q];     // 指向上面 [index block] 的 BlockHandle
//padding:          char[40-p-q];// 两个 BlockHandle 最大 40 字节，不足则补零凑够 40 字节
//magic:            fixed64;     // == 0xdb4775248b80fb57 (little-endian)
namespace leveldb {

class Block;
class RandomAccessFile;
struct ReadOptions;

// BlockHandle is a pointer to the extent of a file that stores a data
// block or a meta block.
// 一个 BlockHandle 是一个指针，它指向一个文件的范围，该文件存储着 data block 或者 meta block。
// 它包括两部分：offset 和 size，分别表示所指向的 block 在文件中的偏移量和大小。
class BlockHandle {
 public:
  BlockHandle();

  // The offset of the block in the file.
  // 所指向的 block 在文件中的偏移量
  uint64_t offset() const { return offset_; }
  void set_offset(uint64_t offset) { offset_ = offset; }

  // The size of the stored block
  // 所指向的 block 的大小
  uint64_t size() const { return size_; }
  void set_size(uint64_t size) { size_ = size; }

  void EncodeTo(std::string* dst) const;
  Status DecodeFrom(Slice* input);

  // Maximum encoding length of a BlockHandle
  // 一个 BlockHandle 编码后最大长度（varint64 最多需要 10 个字节存储）
  enum { kMaxEncodedLength = 10 + 10 };

 private:
  uint64_t offset_;
  uint64_t size_;
};

// Footer encapsulates the fixed information stored at the tail
// end of every table file.
//
// Footer 封装一个固定长度的信息，它位于每个 table 文件的末尾。
//
// 在每个 sstable 文件的末尾是一个固定长度的 footer，它包含了一个指向 metaindex block 的 BlockHandle
// 和一个指向 index block 的 BlockHandle 以及一个 magic number。
class Footer {
 public:
  Footer() { }

  // The block handle for the metaindex block of the table
  //
  // 与 metaindex 块的 BlockHandle 相关的 getter/setter
  const BlockHandle& metaindex_handle() const { return metaindex_handle_; }
  void set_metaindex_handle(const BlockHandle& h) { metaindex_handle_ = h; }

  // The block handle for the index block of the table
  //
  // 与 index 块的 BlockHandle 相关的 getter/setter
  const BlockHandle& index_handle() const {
    return index_handle_;
  }
  void set_index_handle(const BlockHandle& h) {
    index_handle_ = h;
  }

  void EncodeTo(std::string* dst) const;
  Status DecodeFrom(Slice* input);

  // Encoded length of a Footer.  Note that the serialization of a
  // Footer will always occupy exactly this many bytes.  It consists
  // of two block handles and a magic number.
  //
  // Footer 长度编码后的长度。注意，它就固定这么长。
  // Footer 包含了一个 metaindex_handle、一个 index_handle、以及一个魔数。
  enum {
    kEncodedLength = 2*BlockHandle::kMaxEncodedLength + 8 // Footer 长度，两个 BlockHandle 最大长度 + 固定的 8 字节魔数
  };

 private:
  BlockHandle metaindex_handle_;
  BlockHandle index_handle_;
};

// kTableMagicNumber was picked by running
//    echo http://code.google.com/p/leveldb/ | sha1sum
// and taking the leading 64 bits.
//
// Footer 的魔数通过运行命令 'echo http://code.google.com/p/leveldb/ | sha1sum' 获得，
// 并且存在 Footer 的前 64 位。
static const uint64_t kTableMagicNumber = 0xdb4775248b80fb57ull;

// 1-byte type + 32-bit crc
//
// 每个 block 的 trailer 由两部分构成：1 字节的 type（对应 block 的压缩类型），和 32 位的 crc。
// 共 5 字节。
static const size_t kBlockTrailerSize = 5;

struct BlockContents {
  Slice data;           // Actual contents of data 真正的数据内容
  bool cachable;        // True iff data can be cached 当且仅当数据可以被缓存的时候为 true
  bool heap_allocated;  // True iff caller should delete[] data.data() 当且仅当调用者应该调用 delete[] 释放 data.data() 的时候为 true
};

// Read the block identified by "handle" from "file".  On failure
// return non-OK.  On success fill *result and return OK.
//
// 根据 options 从 file 中读取由 handle 指向的 block 并存储到 result 中。
// 失败返回 non-OK；
// 成功则将数据填充到 *result 并返回 OK。
Status ReadBlock(RandomAccessFile* file,
                 const ReadOptions& options,
                 const BlockHandle& handle,
                 BlockContents* result);

// Implementation details follow.  Clients should ignore,

inline BlockHandle::BlockHandle()
    : offset_(~static_cast<uint64_t>(0)),
      size_(~static_cast<uint64_t>(0)) {
}

}  // namespace leveldb

#endif  // STORAGE_LEVELDB_TABLE_FORMAT_H_

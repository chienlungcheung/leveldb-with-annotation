// Copyright (c) 2011 The LevelDB Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file. See the AUTHORS file for names of contributors.

#ifndef STORAGE_LEVELDB_TABLE_BLOCK_BUILDER_H_
#define STORAGE_LEVELDB_TABLE_BLOCK_BUILDER_H_

#include <vector>

#include <stdint.h>
#include "leveldb/slice.h"

namespace leveldb {

struct Options;

// BlockBuilder 负责生成 block，在 block 中的数据项的 keys 使用了前缀压缩:
// 当我们存储一个 key 的时候，会把这个 key 与 block 中排在它前面那个 key 的前缀部分去掉（具体做法是不记录具体前缀字符串只记录前缀长度）。
// 这可以帮助我们显著减少空间需求（因为 keys 都是排序过的，排在一起的前缀都是比较相近的，而且相似前缀可能还比较长）。
// 而且，具体实践中，我们并不是只把 block 的第一个 key 保存为完整的后面的全部用前缀压缩，而是每隔 K 个 keys 我们就保存一个完整的 key，
// 然后从这个完整的 key 开始继续使用前缀压缩直到需要保存下一个完整的 key。这些保存完整 keys 的地方，我们管他们叫 restart points。
// 在 block 结尾处保存着一个全部 restart points 对应的偏移量数组，这个数组可以被用于进行二分查找以快速定位 block 中某个具体的 key。
// 与 keys 不同，values 仍然保存的是未压缩的原值；而且每个 value 紧跟在其对应的 key 之后。
//
// block 中每个数据项的格式如下：
//     shared_bytes: varint32（与前一个 key 共享的前缀的长度，varint32 类型）
//     unshared_bytes: varint32（当前 key 除去共享前缀后的长度，varint32 类型）
//     value_length: varint32（当前 key 对应的 value 的长度，varint32 类型）
//     key_delta: char[unshared_bytes]（当前 key 除去共享前缀后的字节）
//     value: char[value_length]（当前 key 对应的 value 的数据）
// shared_bytes == 0 for restart points. 注意，如果该数据项位于 restart 处，则 shared_bytes 为 0.
//
// block 结尾处有个 trailer，格式如下：
//     restarts: uint32[num_restarts]（保存 restart points 在 block 内偏移量的数组）
//     num_restarts: uint32（restart points 偏移量数组大小）
// restarts[i] 保存的是第 i 个 restart point 在 block 内的偏移量。
class BlockBuilder {
 public:
  explicit BlockBuilder(const Options* options);

  // Reset the contents as if the BlockBuilder was just constructed.
  //
  // 重置 BlockBuilder 对象状态信息，就像该对象刚刚被创建时一样。
  void Reset();

  // REQUIRES: Finish() has not been called since the last call to Reset().
  // REQUIRES: key is larger than any previously added key
  //
  // 追加一个数据项到 buffer。
  // 前提：自从上次调用 Reset() 还未调用过 Finish()；参数 key 要大于任何之前已经添加过的数据项的 key。
  void Add(const Slice& key, const Slice& value);

  // Finish building the block and return a slice that refers to the
  // block contents.  The returned slice will remain valid for the
  // lifetime of this builder or until Reset() is called.
  //
  // 该方法会基于该 builder 构建一个 block，然后返回一个指向 block 内容的 slice。
  // 返回的 slice 生命期同 builder，除非 builder 调用了 Reset() 方法。
  Slice Finish();

  // Returns an estimate of the current (uncompressed) size of the block
  // we are building.
  //
  // 返回当前正在构造的 block 的（未压缩）大小的估计值
  size_t CurrentSizeEstimate() const;

  // Return true iff no entries have been added since the last Reset()
  //
  // 自从上次 reset 后，当且仅当没有任何数据项被添加到该 BlockBuilder 时返回 true
  bool empty() const {
    return buffer_.empty();
  }

 private:
  const Options*        options_;
  // 存储要构造的 block 内容的缓冲区
  std::string           buffer_;      // Destination buffer
  // 存储要构造的 block 的全部 restart points（即每个 restart point 在 block 中的偏移量，第一个 restart point 偏移量为 0）
  std::vector<uint32_t> restarts_;    // Restart points
  // 自从 restart 之后新添加的数据项个数
  int                   counter_;     // Number of entries emitted since restart
  // 标识方法 Finish() 是否被调用过了
  bool                  finished_;    // Has Finish() been called?
  // 该 BlockBuilder 上次调用 Add 时追加的那个 key
  std::string           last_key_;

  // No copying allowed
  BlockBuilder(const BlockBuilder&);
  void operator=(const BlockBuilder&);
};

}  // namespace leveldb

#endif  // STORAGE_LEVELDB_TABLE_BLOCK_BUILDER_H_

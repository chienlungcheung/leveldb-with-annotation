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

// BlockBuilder 被用来构造 sstable 文件里的 block, 
// 注意, 该类用于构造 block 的序列化形式,
// 也就是构造 sstable 时候使用; 反序列化用的是 Block 类.
// 在 block 中的数据项的 keys 使用了前缀压缩:
// 当我们存储一个 key 的时候, 会采用前缀压缩. 具体来说, 存储某个 key 的时候
// 先计算它和前一个数据项 key 的公共前缀长度, 公共前缀不再重复存储而是仅记录
// 一个长度(shared), 由于 block 保存的数据是按 key 有序的, 排在一起的前缀
// 都是比较相近的, 而且相似前缀可能还比较长所以该策略可以大幅节省存储空间. 
// 而且, 具体实践中, 我们并不是只把 block 的第一个 key 保存为完整的后面的
// 全部用前缀压缩, 而是每隔 K 个 keys 我们就保存一个完整的 key(该 key 
// 所在数据项被叫做 restart point), 然后从这个完整的 key 开始继续使用
// 前缀压缩直到需要保存下一个完整的 key. 
// 这些保存完整 keys 的地方, 我们管他们叫 restart points. 在 block 
// 结尾处保存着全部 restart points 对应的偏移量数组, 这个数组可以被
// 用于进行二分查找以快速定位 block 中某个具体的 key. 
// 与 keys 不同, values 仍然保存的是未压缩的原值; 而且每个 value 
// 紧跟在其对应的 key 之后. 
//
// Block 布局如下: 
// 0. 每个 block 包含的数据有"数据项 + restart array + restart number"
//
// 1. block 中每个数据项的格式如下: 
// - shared_bytes: varint32(与前一个 key 共享的前缀的长度, varint32 类型)
// - unshared_bytes: varint32(当前 key 除去共享前缀后的长度, varint32 类型)
// - value_length: varint32(当前 key 对应的 value 的长度, varint32 类型)
// - key_delta: char[unshared_bytes](当前 key 除去共享前缀后的字节内容)
// - value: char[value_length](当前 key 对应的 value 的数据内容)
// 注意, 如果该数据项位于 restart 处, 则 shared_bytes 等于 0.
// (在构建 block 的时候会每隔一段设置一个 restart point, 
// 位于 restart point 的数据项的 key 不会进行前缀压缩, 此项之后
// 的数据项会相对于前一个数据项进行前缀压缩直至下一个 restart  point.) 
//
// 2. block 结尾处有个 trailer, 格式如下: 
// - restarts: uint32[num_restarts](保存 restart points 在 block 内偏移量的数组)
// - num_restarts: uint32(restart points 偏移量数组大小)
// restarts[i] 保存的是第 i 个 restart point 在 block 内的偏移量.  
class BlockBuilder {
 public:
  explicit BlockBuilder(const Options* options);

  // 重置 BlockBuilder 对象状态信息, 就像该对象刚刚被创建时一样. 
  void Reset();

  // 与 Finish() 分工, 负责追加一个数据项到 buffer. 
  // 前提: 
  // 1. 自从上次调用 Reset() 还未调用过 Finish(); 
  // 2. 参数 key 要大于任何之前已经添加过的数据项的 key,
  // 因为这是一个 append 类型操作. 
  void Add(const Slice& key, const Slice& value);

  // 该方法负责 block 构建的收尾工作, 具体是将 restart points
  // 数组及其长度追加到 block 的数据内容之后完成构建工作, 最后返回
  // 一个指向 block 全部内容的 slice. 
  // 返回的 slice 生命期同当前 builder, 若 builder 调用了
  // Reset() 方法则返回的 slice 失效. 
  Slice Finish();

  // 返回当前正在构造的 block (未压缩)大小的估计值
  size_t CurrentSizeEstimate() const;

  // 自从上次 reset 后, 当且仅当没有任何数据项被添加
  // 到该 BlockBuilder 时返回 true.
  bool empty() const {
    return buffer_.empty();
  }

 private:
  const Options*        options_;
  // 存储目标 block 内容的缓冲区
  std::string           buffer_;
  // 存储目标 block 的全部 restart points
  // (即每个 restart point 在 block 中的偏移量, 
  // 第一个 restart point 偏移量为 0)
  std::vector<uint32_t> restarts_;
  // 某个 restart 之后新添加的数据项个数
  int                   counter_;
  // 标识方法 Finish() 是否被调用过了
  bool                  finished_;
  // 该 BlockBuilder 上次调用 Add 时追加的那个 key
  std::string           last_key_;

  // 不允许拷贝操作
  BlockBuilder(const BlockBuilder&);
  void operator=(const BlockBuilder&);
};

}  // namespace leveldb


#endif  // STORAGE_LEVELDB_TABLE_BLOCK_BUILDER_H_

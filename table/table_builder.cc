// Copyright (c) 2011 The LevelDB Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file. See the AUTHORS file for names of contributors.

#include "leveldb/table_builder.h"

#include <assert.h>
#include "leveldb/comparator.h"
#include "leveldb/env.h"
#include "leveldb/filter_policy.h"
#include "leveldb/options.h"
#include "table/block_builder.h"
#include "table/filter_block.h"
#include "table/format.h"
#include "util/coding.h"
#include "util/crc32c.h"

namespace leveldb {

struct TableBuilder::Rep {
  Options options;
  Options index_block_options;
  // table 对应的文件指针
  WritableFile* file; 
  // table 文件当前的最大偏移量
  uint64_t offset; 
  Status status;
  // 用于构造 data block
  BlockBuilder data_block; 
  // 用于构造 index block
  BlockBuilder index_block; 
  // 最近一次成功调用 Add 添加的 key
  std::string last_key; 
  // 当前 table 中全部 data block entries 个数
  int64_t num_entries; 
  // 指示是否调用过 Finish() 或 Abandon()
  bool closed;          
  // 构造 filter block
  FilterBlockBuilder* filter_block; 

  // 直到当追加下一个 data block 第一个 key 的时候, 我们才会将
  // 当前 data block 对应的 index 数据项追加到 index block,  
  // 这样可以让我们在 index block 使用更短的 key. 
  // 举个例子, 假设当前 data block 最后一个
  // key "the quick brown fox", 下一个 data block 
  // 首个 key "the who" 之间, 
  // 则我们可以为当前 data block 的 index 数据项设置
  // key "the r", 因为它 >= 当前 data block 全部 key, 
  // 而且 < 接下来 data block 的全部 key. 
  //
  // 不变式: 仅当 data_block 为空的时候(已经被 flush 过了)
  // r->pending_index_entry 才为 true. 
  // 注意该变量初始值为 false, 也就是写满(大小可配置)并 flush 过一个
  // data block 时候才会写入它对应的 index entry, 
  // 因为此时 index entry 才知道 data block 最大 key 是什么.
  bool pending_index_entry;
  // 与 pending_index_entry 配套, 
  // 指向当前 data block 的 BlockHandle,
  // 会被追加到 index block 中.
  BlockHandle pending_handle;

  std::string compressed_output;

  Rep(const Options& opt, WritableFile* f)
      : options(opt),
        index_block_options(opt),
        file(f),
        offset(0),
        data_block(&options),
        index_block(&index_block_options),
        num_entries(0),
        closed(false),
        filter_block(opt.filter_policy == nullptr ? nullptr
                     : new FilterBlockBuilder(opt.filter_policy)),
        pending_index_entry(false) {
    // index block 的 key 不需要做前缀压缩, 
    // 所以把该值设置为 1, 表示每个 restart 段长度为 1.
    index_block_options.block_restart_interval = 1; 
  }
};

TableBuilder::TableBuilder(const Options& options, WritableFile* file)
    : rep_(new Rep(options, file)) {
  if (rep_->filter_block != nullptr) {
    rep_->filter_block->StartBlock(0);
  }
}

TableBuilder::~TableBuilder() {
  // 析构之前必须调用 Finish()
  assert(rep_->closed);  
  delete rep_->filter_block;
  delete rep_;
}

Status TableBuilder::ChangeOptions(const Options& options) {
  // 注意, 如果 Options 增加了新字段, 
  // 这里会检查捕获那些我们不期望构建 Table 过程中动态修改的字段. 
  if (options.comparator != rep_->options.comparator) { 
    // 不允许动态修改 comparator
    return Status::InvalidArgument("changing comparator while building table");
  }

  // Note that any live BlockBuilders point to rep_->options and therefore
  // will automatically pick up the updated options.
  rep_->options = options;
  rep_->index_block_options = options;
  rep_->index_block_options.block_restart_interval = 1;
  return Status::OK();
}

void TableBuilder::Add(const Slice& key, const Slice& value) {
  Rep* r = rep_;
  // 确保之前没有调用过 Finish() 或者 Abandon()
  assert(!r->closed); 
  if (!ok()) return;
  // 如果该条件成立则说明之前调用过 Add 添加过数据了
  if (r->num_entries > 0) { 
    // 确保待添加的 key 大于之前已添加过的全部 keys
    assert(r->options.comparator->Compare(key, Slice(r->last_key)) > 0); 
  }

  // 需要构造一个新的 data block
  if (r->pending_index_entry) {
    // 与上面紧邻的这个判断条件构成不变式, 为空表示
    // 已经将写满的 data block flush 到文件了.
    assert(r->data_block.empty());
    // 为 pending index entry 选一个合适的 key.
    // 下面这个函数调用结束, last_key 可能不变, 也可能长度更短但是值更大, 
    // 但不会 >= 要追加的 key. 因为进入该方法之前关于两个参数
    // 已经有了一个约束: 第一个字符串肯定小于第二个字符串, 这个上面有断言保证了.
    r->options.comparator->FindShortestSeparator(&r->last_key, key);
    // 用于存储序列化后的 BlockHandle
    std::string handle_encoding;
    // 将刚刚 flush 过的 data block 对应的 BlockHandle 序列化
    r->pending_handle.EncodeTo(&handle_encoding);
    // data index block 构造相关:
    // 为刚刚 flush 过的 data block 在 index block 增加一个数据项, 
    // last_key 肯定大于等于其全部所有的 keys 且小于新的 
    // data block 的第一个 key.
    r->index_block.Add(r->last_key, Slice(handle_encoding));
    // 增加过 index entry 后, 可以将其置为 false 了.
    r->pending_index_entry = false;
  }
  
  // meta block 构造相关:
  // 如果该 table 存在 filter block, 则将该 key 加入.
  // (filter block 可以用于快速定位 key 是否存在于 table 中).
  // 加入的 key 在 FilterBlockBuilder 中使用.
  if (r->filter_block != nullptr) {
    r->filter_block->AddKey(key);
  }

  // 用新 key 更新 last_key
  r->last_key.assign(key.data(), key.size());
  r->num_entries++;
  // data block 相关:
  // 将 key,value 添加到 data block 中
  r->data_block.Add(key, value); 

  const size_t estimated_block_size = r->data_block.CurrentSizeEstimate();
  // 如果当前 data block 大小的估计值大于设定的阈值, 
  // 则将该 data block 写入文件
  if (estimated_block_size >= r->options.block_size) {
    Flush();
  }
}

void TableBuilder::Flush() {
  Rep* r = rep_;
  assert(!r->closed);
  if (!ok()) return;
  if (r->data_block.empty()) return;
  assert(!r->pending_index_entry);
  // 将 data block 压缩并落盘, 在该方法中 data_block 会调用 Reset()
  WriteBlock(&r->data_block, &r->pending_handle); 
  if (ok()) {
    // flush 一个 data block, 就要在 index block 为其增加一个指针.
    r->pending_index_entry = true;
    r->status = r->file->Flush();
  }
  if (r->filter_block != nullptr) {
    // 为已经刷入文件的 data block 计算 filter, 同时
    // 为新的 data block 计算 filter 做准备, 
    // r->offset 为下个 block 在 table 的起始地址, 
    // 该值在上面 WriteBlock 调用 WriteRawBlock 时会进行更新.
    r->filter_block->StartBlock(r->offset); 
  }
}

void TableBuilder::WriteBlock(BlockBuilder* block, BlockHandle* handle) {
  // sstable 文件包含一系列 blocks, 每个 block 由下面几部分构成:
  //    block_data: uint8[n]
  //    type: uint8
  //    crc: uint32
  assert(ok());
  Rep* r = rep_;
  Slice raw = block->Finish();

  // 要写入文件的内容
  Slice block_contents; 
  CompressionType type = r->options.compression;
  // TODO(postrelease): Support more compression options: zlib?
  switch (type) {
    case kNoCompression:
      block_contents = raw;
      break;

    case kSnappyCompression: {
      std::string* compressed = &r->compressed_output;
      if (port::Snappy_Compress(raw.data(), raw.size(), compressed) &&
          compressed->size() < raw.size() - (raw.size() / 8u)) {
        block_contents = *compressed;
      } else {
        // 不支持 Snappy 或者压缩率小于 12.5%, 则不进行压缩了
        block_contents = raw;
        type = kNoCompression;
      }
      break;
    }
  }
  
  // 将 block 内容写入文件
  WriteRawBlock(block_contents, type, handle); 
  r->compressed_output.clear();
  // block 清空, 准备复用构建下个 block
  block->Reset(); 
}
 
void TableBuilder::WriteRawBlock(const Slice& block_contents,
                                 CompressionType type,
                                 BlockHandle* handle) {
  Rep* r = rep_;
  // 该 block 在 table 中的起始偏移量
  handle->set_offset(r->offset); 
  // 该 block 大小
  handle->set_size(block_contents.size()); 
  // 将 block 数据内容写入文件
  r->status = r->file->Append(block_contents); 
  if (r->status.ok()) {
    // 构造 trailer, 包含 block 压缩类型 和 crc
    char trailer[kBlockTrailerSize];
    trailer[0] = type;
    uint32_t crc = crc32c::Value(block_contents.data(), block_contents.size());
    // 扩展 crc 以覆盖 block type
    crc = crc32c::Extend(crc, trailer, 1);
    EncodeFixed32(trailer+1, crc32c::Mask(crc));
    // 把该 block 的 trailer (包含压缩类型和 crc) 写入文件
    r->status = r->file->Append(Slice(trailer, kBlockTrailerSize)); 
    if (r->status.ok()) {
      // 下个 block 在 table 中的起始偏移量
      r->offset += block_contents.size() + kBlockTrailerSize;
    }
  }
}

Status TableBuilder::status() const {
  return rep_->status;
}

Status TableBuilder::Finish() {
  Rep* r = rep_;
  // 1 这个 Flush 是兜底, 因为 Add() 末尾处调用
  // Flush 有个前提就是 data block 满足大小要求.
  Flush();
  assert(!r->closed);
  // 调用了 Finish
  r->closed = true;

  BlockHandle filter_block_handle, metaindex_block_handle, index_block_handle;

  // 2 如果存在 filter block, 则将其写入文件; 
  // 写完后, filter_block_handle 保存着该 block 
  // 对应的 BlockHandle 信息, 包括起始偏移量和大小
  if (ok() && r->filter_block != nullptr) {
    // 写完 data blocks 该写入 filter block 了.
    // 注意 filter block 写入时不进行压缩.
    WriteRawBlock(r->filter_block->Finish(), kNoCompression, 
    // 将计算出 filter block 对应的 BlockHandle 
    // 信息保存在 filter_block_handle 中.
                  &filter_block_handle); 
  }

  // 3 filter block 就是 table_format.md 中提到的 
  // meta block, 写完 meta block 该写它对应的索引
  // metaindex block 到文件中了.
  if (ok()) {
    // 虽然 meta block 有独立的 FilterBlockBuilder, 
    // 但是其对应的 index block 用的还是通用的 
    // BlockBuilder.
    BlockBuilder meta_index_block(&r->options);
    if (r->filter_block != nullptr) { 
      // 如果 meta block 存在, 则将其对应的 key 
      // 和 BlockHandle 写入 metaindex block,
      // 具体为 <filter.Name, filter 数据地址>.
      std::string key = "filter.";
      key.append(r->options.filter_policy->Name());
      std::string handle_encoding;
      filter_block_handle.EncodeTo(&handle_encoding);
      meta_index_block.Add(key, handle_encoding);
    }
    // 将 metaindex block 写入文件
    WriteBlock(&meta_index_block, &metaindex_block_handle); 
  }

  // 4 将 index block 写入 table 文件, 它里面保存的
  // 都是 data block 对应的 BlockHandle.
  if (ok()) {
    // 最后构建的 data block 对应的 index block entry 还没有写入
    if (r->pending_index_entry) { 
      r->options.comparator->FindShortSuccessor(&r->last_key);
      std::string handle_encoding;
      r->pending_handle.EncodeTo(&handle_encoding);
      // 写入最后构建的 data block 对应的 index block entry
      r->index_block.Add(r->last_key, Slice(handle_encoding)); 
      r->pending_index_entry = false;
    }
    WriteBlock(&r->index_block, &index_block_handle);
  }

  // 5 最后将末尾的 Footer 写入 table 文件
  if (ok()) {
    Footer footer;
    // 设置 meta index block 的指针到 footer
    footer.set_metaindex_handle(metaindex_block_handle);
    // 设置 data index block 的指针到 footer
    footer.set_index_handle(index_block_handle);
    std::string footer_encoding;
    footer.EncodeTo(&footer_encoding);
    r->status = r->file->Append(footer_encoding);
    if (r->status.ok()) {
      r->offset += footer_encoding.size();
    }
  }
  return r->status;
}

void TableBuilder::Abandon() {
  Rep* r = rep_;
  assert(!r->closed);
  r->closed = true;
}

uint64_t TableBuilder::NumEntries() const {
  return rep_->num_entries;
}

uint64_t TableBuilder::FileSize() const {
  return rep_->offset;
}

}  // namespace leveldb

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
  WritableFile* file; // table 对应的要写入的文件
  uint64_t offset; // table 文件当前的最大偏移量
  Status status;
  BlockBuilder data_block; // 构造 data block
  BlockBuilder index_block; // 构造 index block
  std::string last_key; // 最近一次成功调用 Add 添加的 key
  int64_t num_entries; // 当前 table 中全部 data block entries 个数
  bool closed;          // Either Finish() or Abandon() has been called. 指示是否调用过 Finish() 或 Abandon()
  FilterBlockBuilder* filter_block; // 构造 filter block

  // We do not emit the index entry for a block until we have seen the
  // first key for the next data block.  This allows us to use shorter
  // keys in the index block.  For example, consider a block boundary
  // between the keys "the quick brown fox" and "the who".  We can use
  // "the r" as the key for the index block entry since it is >= all
  // entries in the first block and < all entries in subsequent
  // blocks.
  //
  // Invariant: r->pending_index_entry is true only if data_block is empty.
  //
  // 直到看到下一个 data block 第一个 key, 我们才会发射当前 data block 对应的 index 数据项. 
  // 这样可以让我们在 index block 使用更短的 key. 举个例子, 假设当前 data block 最后一个
  // key "the quick brown fox", 下一个 data block 首个 key "the who" 之间, 
  // 则我们可以为该 data block 的 index 数据项设置 key "the r", 因为它 >= 当前 data block 全部 key, 
  // 而且 < 接下来 data block 的全部 key. 
  //
  // 不变式: 仅当 data_block 为空的时候 r->pending_index_entry 才为 true. 
  bool pending_index_entry;
  // 指向当前正在构造的 data block 的 BlockHandle
  BlockHandle pending_handle;  // Handle to add to index block

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
    index_block_options.block_restart_interval = 1; // index block 的 key 不需要做前缀压缩, 所以把该值设置为 1, 表示每个 restart 段长度为 1
  }
};

TableBuilder::TableBuilder(const Options& options, WritableFile* file)
    : rep_(new Rep(options, file)) {
  if (rep_->filter_block != nullptr) {
    rep_->filter_block->StartBlock(0);
  }
}

TableBuilder::~TableBuilder() {
  assert(rep_->closed);  // Catch errors where caller forgot to call Finish()
  delete rep_->filter_block;
  delete rep_;
}

Status TableBuilder::ChangeOptions(const Options& options) {
  // Note: if more fields are added to Options, update
  // this function to catch changes that should not be allowed to
  // change in the middle of building a Table.
  //
  // 注意, 如果 Options 增加了新字段, 
  // 这里要进行同步维护以捕获那些我们不期望构建 Table 过程中动态修改的字段. 
  if (options.comparator != rep_->options.comparator) { // 不允许动态修改 comparator
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
  assert(!r->closed); // 确保之前没有调用过 Finish() 或者 Abandon()
  if (!ok()) return;
  if (r->num_entries > 0) { // 如果该条件成立则说明之前调用过 Add 添加过数据了
    assert(r->options.comparator->Compare(key, Slice(r->last_key)) > 0); // 确保待添加的 key 大于之前已添加过的全部 keys
  }

  // 需要构造一个新的 data block
  if (r->pending_index_entry) { 
    assert(r->data_block.empty());
    // 函数调用结束, last_key 长度可能更短但是值可能更大但不会 >= key
    r->options.comparator->FindShortestSeparator(&r->last_key, key);
    std::string handle_encoding;
    r->pending_handle.EncodeTo(&handle_encoding);
    // 为前一个 data block 在 index block 增加一个数据项, 
    // last_key 肯定大于等于其全部所有的 keys 且小于新的 data block 的第一个 key
    r->index_block.Add(r->last_key, Slice(handle_encoding));
    r->pending_index_entry = false;
  }
  
  // 如果该 table 存在 filter block, 则将该 key 加入
  if (r->filter_block != nullptr) {
    r->filter_block->AddKey(key);
  }

  r->last_key.assign(key.data(), key.size());
  r->num_entries++;
  // 将 key,value 添加到 data block 中
  r->data_block.Add(key, value); 

  const size_t estimated_block_size = r->data_block.CurrentSizeEstimate();
  // 如果当前 data block 估计大小大于设定阈值, 则将该 data block 写入文件
  if (estimated_block_size >= r->options.block_size) {
    Flush();
  }
}

// 将当前构造好的 data block 写入文件
void TableBuilder::Flush() {
  Rep* r = rep_;
  assert(!r->closed);
  if (!ok()) return;
  if (r->data_block.empty()) return;
  assert(!r->pending_index_entry);
  // 将 data block 压缩并落盘, 在该方法中 data_block 会调用 Reset()
  WriteBlock(&r->data_block, &r->pending_handle); 
  if (ok()) {
    r->pending_index_entry = true;
    r->status = r->file->Flush();
  }
  if (r->filter_block != nullptr) {
    // 为新的 data block 计算 filter 做准备, 
    // r->offset 为下个 block 起始地址, 该值在上面 WriteBlock 调用 WriteRawBlock 时会进行更新.
    r->filter_block->StartBlock(r->offset); 
  }
}

// 将 block 内容根据设置进行压缩, 然后写入文件
void TableBuilder::WriteBlock(BlockBuilder* block, BlockHandle* handle) {
  // File format contains a sequence of blocks where each block has:
  //    block_data: uint8[n]
  //    type: uint8
  //    crc: uint32
  assert(ok());
  Rep* r = rep_;
  Slice raw = block->Finish();

  Slice block_contents; // 要写入文件的内容
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
        // Snappy not supported, or compressed less than 12.5%, so just
        // store uncompressed form
        //
        // 不支持 Snappy 或者压缩率小于 12.5%, 则不进行压缩了
        block_contents = raw;
        type = kNoCompression;
      }
      break;
    }
  }
  WriteRawBlock(block_contents, type, handle); // 将 block 内容写入文件
  r->compressed_output.clear();
  // block 清空, 准备复用处理下个 block
  block->Reset(); 
}

// 将 block 及其 trailer 写入 table 对应的文件, 并将 block 对应的 BlockHandle 内容保存到 handle 中. 
// 写失败时该方法只将错误状态记录到 r->status, 不做其它任何处理. 
void TableBuilder::WriteRawBlock(const Slice& block_contents,
                                 CompressionType type,
                                 BlockHandle* handle) {
  Rep* r = rep_;
  handle->set_offset(r->offset); // 该 block 在 table 中的起始偏移量
  handle->set_size(block_contents.size()); // 该 block 大小
  r->status = r->file->Append(block_contents); // 将 block 数据内容写入文件
  if (r->status.ok()) {
    char trailer[kBlockTrailerSize];
    trailer[0] = type;
    uint32_t crc = crc32c::Value(block_contents.data(), block_contents.size());
    crc = crc32c::Extend(crc, trailer, 1);  // Extend crc to cover block type
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
  // 写 data blocks
  Flush();
  assert(!r->closed);
  // 调用了 Finish
  r->closed = true;

  BlockHandle filter_block_handle, metaindex_block_handle, index_block_handle;

  // Write filter blocks
  // 如果存在 filter block, 则将其写入文件; 
  // 写完后, filter_block_handle 保存着该 block 对应的 BlockHandle 信息, 包括起始偏移量和大小
  if (ok() && r->filter_block != nullptr) {
    WriteRawBlock(r->filter_block->Finish(), kNoCompression, // filter block 不进行压缩
                  &filter_block_handle); // 将计算出 filter block 对应的 BlockHandle 信息保存在 filter_block_handle 中
  }

  // Write metaindex block
  // filter block 就是 table_format.md 中提到的 meta block, 写完 meta block
  // 该写 metaindex block 到文件中了
  if (ok()) {
    BlockBuilder meta_index_block(&r->options);
    if (r->filter_block != nullptr) { // 如果 meta block 存在, 则将其对应的 key 和 BlockHandle 写入 metaindex block
      // Add mapping from "filter.Name" to location of filter data
      std::string key = "filter.";
      key.append(r->options.filter_policy->Name());
      std::string handle_encoding;
      filter_block_handle.EncodeTo(&handle_encoding);
      meta_index_block.Add(key, handle_encoding);
    }

    // TODO(postrelease): Add stats and other meta blocks
    WriteBlock(&meta_index_block, &metaindex_block_handle); // 将 metaindex block 写入文件
  }

  // Write index block
  // 将 index block 写入 table 文件, 它里面保存的都是 data block 对应的 BlockHandle
  if (ok()) {
    if (r->pending_index_entry) { // 最后构建的 data block 对应的 index block entry 还没有写入
      r->options.comparator->FindShortSuccessor(&r->last_key);
      std::string handle_encoding;
      r->pending_handle.EncodeTo(&handle_encoding);
      r->index_block.Add(r->last_key, Slice(handle_encoding)); // 写入最后构建的 data block 对应的 index block entry
      r->pending_index_entry = false;
    }
    WriteBlock(&r->index_block, &index_block_handle);
  }

  // Write footer
  // 将末尾的 Footer 写入 table 文件
  if (ok()) {
    Footer footer;
    footer.set_metaindex_handle(metaindex_block_handle);
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

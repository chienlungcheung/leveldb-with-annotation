// Copyright (c) 2011 The LevelDB Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file. See the AUTHORS file for names of contributors.

#include "leveldb/table.h"

#include "leveldb/cache.h"
#include "leveldb/comparator.h"
#include "leveldb/env.h"
#include "leveldb/filter_policy.h"
#include "leveldb/options.h"
#include "table/block.h"
#include "table/filter_block.h"
#include "table/format.h"
#include "table/two_level_iterator.h"
#include "util/coding.h"

namespace leveldb {

// 存储 table 中各个元素的结构
struct Table::Rep {
  ~Rep() {
    delete filter;
    delete [] filter_data;
    delete index_block;
  }

  Options options;
  Status status;
  RandomAccessFile* file; // table 对应的文件
  uint64_t cache_id; // 如果该 table 具备对应的 block_cache，该值与 block 在 table 中的起始偏移量一起构成 key，value 为 block
  FilterBlockReader* filter; // 解析出来的 filter block
  const char* filter_data; // filter block 原始数据

  // 从 table Footer 取出来的，指向 table 的 metaindex block
  BlockHandle metaindex_handle;  // Handle to metaindex_block: saved from footer
  Block* index_block; // index block 原始数据，保存的是每个 data block 的 BlockHandle
};

Status Table::Open(const Options& options,
                   RandomAccessFile* file,
                   uint64_t size,
                   Table** table) {
  /**
   * 先解析 table 文件结尾的 Footer
   */
  *table = nullptr;
  if (size < Footer::kEncodedLength) { // 每个 table 文件末尾是一个固定长度的 footer
    return Status::Corruption("file is too short to be an sstable");
  }

  // 读取 footer
  char footer_space[Footer::kEncodedLength];
  Slice footer_input;
  Status s = file->Read(size - Footer::kEncodedLength, Footer::kEncodedLength,
                        &footer_input, footer_space);
  if (!s.ok()) return s;

  // 解析 footer
  Footer footer;
  s = footer.DecodeFrom(&footer_input);
  if (!s.ok()) return s;

  /**
   * 根据已解析的 Footer，解析 index block 到 index_block_contents
   */
  // 读取 index block，它的 BlockHandle 存储在 footer 里面
  // Read the index block
  BlockContents index_block_contents;
  if (s.ok()) {
    ReadOptions opt;
    if (options.paranoid_checks) {
      opt.verify_checksums = true;
    }
    s = ReadBlock(file, opt, footer.index_handle(), &index_block_contents);
  }

  if (s.ok()) {
    // We've successfully read the footer and the index block: we're
    // ready to serve requests.
    // 已经成功读取了 Footer 和 index block，我们已经准备好了响应客户端请求了。
    Block* index_block = new Block(index_block_contents);
    Rep* rep = new Table::Rep;
    rep->options = options;
    rep->file = file;
    rep->metaindex_handle = footer.metaindex_handle();
    rep->index_block = index_block;
    rep->cache_id = (options.block_cache ? options.block_cache->NewId() : 0);
    rep->filter_data = nullptr;
    rep->filter = nullptr;
    *table = new Table(rep);
    (*table)->ReadMeta(footer);
  }

  return s;
}

// 解析 table 的 metaindex block（需要先解析 table 的 footer）;
// 然后根据解析出来的 metaindex block 解析 filter block.
// 这就是我们要的元数据。
void Table::ReadMeta(const Footer& footer) {
  if (rep_->options.filter_policy == nullptr) {
    return;  // Do not need any metadata
  }

  /**
   * 根据 Footer 保存的 metaindex BlockHandle 解析 metaindex block 到 meta 中
   */
  // TODO(sanjay): Skip this if footer.metaindex_handle() size indicates
  // it is an empty block.
  ReadOptions opt;
  if (rep_->options.paranoid_checks) {
    opt.verify_checksums = true; // 如果开启了偏执检查，则校验每个 block 的 crc
  }
  BlockContents contents;
  if (!ReadBlock(rep_->file, opt, footer.metaindex_handle(), &contents).ok()) {
    // Do not propagate errors since meta info is not needed for operation
    return;
  }
  Block* meta = new Block(contents);

  Iterator* iter = meta->NewIterator(BytewiseComparator()); // 为 metaindex block 创建一个迭代器
  // 具体见 table_format.md
  // metaindex block 有一个 entry 包含了 FilterPolicy name 到其对应的 filter block 的映射
  std::string key = "filter.";
  key.append(rep_->options.filter_policy->Name());
  iter->Seek(key); // 在 metaindex block 搜寻 key 对应的 entry
  if (iter->Valid() && iter->key() == Slice(key)) {
    ReadFilter(iter->value()); // 找到了，则解析对应的 filter block
  }
  delete iter;
  delete meta;
}

// 解析 table 的 filter block（需要先解析 table 的 metaindex block）
// 解析出的数据放到了 table.rep.filter
void Table::ReadFilter(const Slice& filter_handle_value) {
  Slice v = filter_handle_value;
  BlockHandle filter_handle;
  if (!filter_handle.DecodeFrom(&v).ok()) {
    return;
  }

  // We might want to unify with ReadBlock() if we start
  // requiring checksum verification in Table::Open.
  ReadOptions opt;
  if (rep_->options.paranoid_checks) {
    opt.verify_checksums = true;
  }
  BlockContents block;
  if (!ReadBlock(rep_->file, opt, filter_handle, &block).ok()) {
    return;
  }

  // 如果 blockcontents 中的内存是从堆分配的，需要将其地址赋值给 rep_->filter_data 以方便后续释放（见 ~Rep()）
  if (block.heap_allocated) {
    rep_->filter_data = block.data.data();     // Will need to delete later
  }
  rep_->filter = new FilterBlockReader(rep_->options.filter_policy, block.data);
}

Table::~Table() {
  delete rep_;
}

static void DeleteBlock(void* arg, void* ignored) {
  delete reinterpret_cast<Block*>(arg);
}

static void DeleteCachedBlock(const Slice& key, void* value) {
  Block* block = reinterpret_cast<Block*>(value);
  delete block;
}

static void ReleaseBlock(void* arg, void* h) {
  Cache* cache = reinterpret_cast<Cache*>(arg);
  Cache::Handle* handle = reinterpret_cast<Cache::Handle*>(h);
  cache->Release(handle);
}

// Convert an index iterator value (i.e., an encoded BlockHandle)
// into an iterator over the contents of the corresponding block.
//
// 把一个编码过的 BlockHandle（即 index_value 参数）转换为一个基于对应的 block 的迭代器。
Iterator* Table::BlockReader(void* arg,
                             const ReadOptions& options,
                             const Slice& index_value) {
  Table* table = reinterpret_cast<Table*>(arg);
  Cache* block_cache = table->rep_->options.block_cache;
  Block* block = nullptr;
  Cache::Handle* cache_handle = nullptr;

  BlockHandle handle;
  Slice input = index_value;
  Status s = handle.DecodeFrom(&input);
  // We intentionally allow extra stuff in index_value so that we
  // can add more features in the future.
  // 除了 BlockHandle 信息外，我们允许在 index_value 携带更多数据以在将来支持更多特性。

  if (s.ok()) {
    BlockContents contents;
    // 如果该 table 有 cache 可用，就先在 cache 中查找 index_value 指向的 block 是否在 cache 中了
    if (block_cache != nullptr) {
      char cache_key_buffer[16];
      // cache_id 和 block 在 table 中的偏移量构成了 key
      EncodeFixed64(cache_key_buffer, table->rep_->cache_id);
      EncodeFixed64(cache_key_buffer+8, handle.offset());
      Slice key(cache_key_buffer, sizeof(cache_key_buffer));
      cache_handle = block_cache->Lookup(key);
      if (cache_handle != nullptr) {
        block = reinterpret_cast<Block*>(block_cache->Value(cache_handle)); // 查到的 value 就是 index_value 指向的 block
      } else {
        s = ReadBlock(table->rep_->file, options, handle, &contents); // 如果 block 不在 cache 中，就去 table 对应的文件去读取
        if (s.ok()) {
          block = new Block(contents);
          if (contents.cachable && options.fill_cache) { // 如果用户允许 block 可用被缓存，则将从文件读取的 block 放到 cache 中
            cache_handle = block_cache->Insert(
                key, block, block->size(), &DeleteCachedBlock);
          }
        }
      }
    } else { // table 对象没有 cache 可用，直接从文件读取 block
      s = ReadBlock(table->rep_->file, options, handle, &contents);
      if (s.ok()) {
        block = new Block(contents);
      }
    }
  }

  Iterator* iter;
  if (block != nullptr) { // 如果 index_value 指向的 block 存在，则为其创建一个迭代器
    iter = block->NewIterator(table->rep_->options.comparator);
    // 如果 table 没有配置用于缓存 block 的 cache，则为该 block 在其迭代器中注册名为 DeleteBlock 的清理函数用于在迭代器销毁时释放 block 指向的内存；
    // 如果 table 配置了用于缓存 block 的 cache，则为该 block 在其迭代器中注册名为 ReleaseBlock 的清理函数用于在迭代器销毁时释放 block 在 cache 中对应的 handle；
    if (cache_handle == nullptr) {
      iter->RegisterCleanup(&DeleteBlock, block, nullptr);
    } else {
      iter->RegisterCleanup(&ReleaseBlock, block_cache, cache_handle);
    }
  } else {
    iter = NewErrorIterator(s);
  }
  return iter;
}

// 根据解析获得的 index block，先构造一个 index block 数据项的 iterator，
// 然后在其基础上构造一个双层迭代器，用于遍历全部 data blocks 的数据项。
Iterator* Table::NewIterator(const ReadOptions& options) const {
  return NewTwoLevelIterator(
      rep_->index_block->NewIterator(rep_->options.comparator),
      &Table::BlockReader, const_cast<Table*>(this), options);
}

// 在 table 中查找 k 对应的数据项。
// 如果 table 具有 filter，则用 filter 找；
// 如果没有 filter 则去 data block 里面查找，并且在找到后通过 saver 保存 key/value。
Status Table::InternalGet(const ReadOptions& options, const Slice& k,
                          void* arg,
                          void (*saver)(void*, const Slice&, const Slice&)) {
  Status s;
  // 获取指向 index block 数据项的 iterator
  Iterator* iiter = rep_->index_block->NewIterator(rep_->options.comparator);
  // 在 index block 中寻找第一个 key 大于等于 k 的数据项
  iiter->Seek(k);
  if (iiter->Valid()) {
    Slice handle_value = iiter->value(); // 取出对应的 data block 的 BlockHandle
    FilterBlockReader* filter = rep_->filter;
    BlockHandle handle;
    // 如果有 filter 找起来就快了
    if (filter != nullptr &&
        handle.DecodeFrom(&handle_value).ok() &&
        !filter->KeyMayMatch(handle.offset(), k)) {
      // Not found 没在该 data block 对应的过滤器找到这个 key，肯定不存在
    } else { // 没有 filter 需要在 block 中先二分后线性查找
      Iterator* block_iter = BlockReader(this, options, iiter->value());
      block_iter->Seek(k);
      if (block_iter->Valid()) {
        (*saver)(arg, block_iter->key(), block_iter->value()); // 将找到的 key/value 保存下来，后面迭代器要被释放了
      }
      s = block_iter->status();
      delete block_iter;
    }
  }
  if (s.ok()) {
    s = iiter->status();
  }
  delete iiter;
  return s;
}

// 获取 key 的在 table 里估计偏移量
uint64_t Table::ApproximateOffsetOf(const Slice& key) const {
  // 获取 index block 的迭代器
  Iterator* index_iter =
      rep_->index_block->NewIterator(rep_->options.comparator);
  // 先在 data block 级别寻找目标 data block
  index_iter->Seek(key);
  uint64_t result;
  if (index_iter->Valid()) {
    BlockHandle handle;
    Slice input = index_iter->value();
    Status s = handle.DecodeFrom(&input);
    if (s.ok()) { // 如果存在 data block，则将其 block 起始 offset 返回
      result = handle.offset();
    } else { // 解析 BlockHandle 失败，这种情况不该出现，
      // 如果出现了则返回 metaindex block 起始 offset 作为预估，已经快接近 table 文件尾了。
      // Strange: we can't decode the block handle in the index block.
      // We'll just return the offset of the metaindex block, which is
      // close to the whole file size for this case.
      result = rep_->metaindex_handle.offset();
    }
  } else { // key 比 table 文件任意 key 都大，
    // 返回 metaindex block 起始 offset 作为预估，已经快接近 table 文件尾了。
    // key is past the last key in the file.  Approximate the offset
    // by returning the offset of the metaindex block (which is
    // right near the end of the file).
    result = rep_->metaindex_handle.offset();
  }
  delete index_iter;
  return result;
}

}  // namespace leveldb

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

  // 控制 Table 的一些选项, 比如是否进行缓存等.
  Options options;
  Status status;
  // table 对应的 sstable 文件
  RandomAccessFile* file; 
  // 如果该 table 具备对应的 block_cache, 
  // 该值与 block 在 table 中的起始偏移量一起构成 key, value 为 block
  uint64_t cache_id; 
  // 解析出来的 filter block
  FilterBlockReader* filter; 
  // filter block 原始数据
  const char* filter_data; 

  // 从 table Footer 取出来的, 指向 table 的 metaindex block
  BlockHandle metaindex_handle;
  // index block 原始数据, 保存的是每个 data block 的 BlockHandle
  Block* index_block;
};

// 将 file 表示的 sstable 文件反序列化为 Table 对象, 具体保存
// 实际内容的是 Table::rep_.
//
// 如果成功, 返回 OK 并将 *table 设置为新打开的 table. 
// 当不再使用该 table 时候, 需要调用方负责删除之. 
// 如果初始化 table 出错, 将 *table 设置为 nullptr 并返回 non-OK. 
// 注意, 在 table 打开期间, 调用方要确保数据源即 file 持续有效. 
Status Table::Open(const Options& options,
                   RandomAccessFile* file,
                   uint64_t size,
                   Table** table) {
  /**
   * 1 解析 footer: 它是 sstable 的入口.
   */
  *table = nullptr;
  // 每个 table 文件末尾是一个固定长度的 footer
  if (size < Footer::kEncodedLength) { 
    return Status::Corruption("file is too short to be an sstable");
  }

  char footer_space[Footer::kEncodedLength];
  Slice footer_input;
  // 读取 footer, 放到 footer_input
  Status s = file->Read(size - Footer::kEncodedLength, Footer::kEncodedLength,
                        &footer_input, footer_space);
  if (!s.ok()) return s;

  Footer footer;
  // 解析 footer
  s = footer.DecodeFrom(&footer_input);
  if (!s.ok()) return s;

  /**
   * 2 解析 data-index block:
   * 根据已解析的 Footer, 解析出 index block(它保存了指向全部 data blocks 的索引) 
   * 存储到 index_block_contents.
   */
  BlockContents index_block_contents;
  if (s.ok()) {
    ReadOptions opt;
    if (options.paranoid_checks) {
      opt.verify_checksums = true;
    }
    // 读取 index block, 它对应的 BlockHandle 存储在 footer 里面
    s = ReadBlock(file, opt, footer.index_handle(), &index_block_contents);
  }

  if (s.ok()) {
    // 已经成功读取了 Footer 和 index block, 是时候读取 data 了. 
    Block* index_block = new Block(index_block_contents);
    Rep* rep = new Table::Rep;
    rep->options = options;
    rep->file = file;
    // filter-index block 对应的指针 (二级索引), 解析 footer 时候就拿到了.
    rep->metaindex_handle = footer.metaindex_handle();
    // data-index block 
    // (注意它只是一个索引, 即 data blocks 的索引, 
    //  真正使用的时候是基于 data-index block 做二级迭代器来进行查询,
    //  一级索引跨度大, 二级索引粒度小, 可以快速定位数据,
    //  具体见 Table::NewIterator() 方法)
    rep->index_block = index_block;
    // 如果调用方要求缓存这个 table, 则为其分配缓存 id
    rep->cache_id = (options.block_cache ? options.block_cache->NewId() : 0);
    // 接下来跟 filter 相关的两个成员将在下面 ReadMeta 进行填充.
    rep->filter_data = nullptr;
    rep->filter = nullptr;
    *table = new Table(rep);
    /**
     * 3 解析 meta-index block 和 meta block:
     * 根据已解析的 Footer 所包含的 metaindex block 指针, 
     * 解析出 metaindex block, 再基于此解析出 mate block 
     * 存储到 Table::rep_.
     */
    // 读取并解析 filter block 到 table::rep_, 
    // 它一般为布隆过滤器, 可以加速数据查询过程.
    (*table)->ReadMeta(footer);
  }

  // 是的, 该方法没有解析 data blocks.

  return s;
}

// 解析 table 的 metaindex block(需要先解析 table 的 footer);
// 然后根据解析出来的 metaindex block 解析 meta block(目前 meta block
// 仅有 filter block 一种). 
// 这就是我们要的元数据, 解析出来的元数据会被放到 Table::rep_ 中. 
void Table::ReadMeta(const Footer& footer) {
  // 如果压根就没配置过滤策略, 那么无需解析元数据
  if (rep_->options.filter_policy == nullptr) {
    return;
  }

  /**
   * 根据 Footer 保存的 metaindex BlockHandle 
   * 解析对应的 metaindex block 到 meta 中
   */
  ReadOptions opt;
  if (rep_->options.paranoid_checks) {
    // 如果开启了偏执检查, 则校验每个 block 的 crc
    opt.verify_checksums = true; 
  }
  BlockContents contents;
  // 1 根据 handle 读取 metaindex block (从 rep_ 到 contents)
  if (!ReadBlock(rep_->file, opt, footer.metaindex_handle(), &contents).ok()) {
    // 由于 filter block 不是必须的, 没有 filter 最多就是读得慢一些;
    // 所以出错也不再继续传播, 而是直接返回.
    return;
  }

  // 这个变量叫 metaindex 更合适
  Block* meta = new Block(contents);

  // 为 metaindex block 创建一个迭代器
  Iterator* iter = meta->NewIterator(BytewiseComparator()); 
  // 具体见 table_format.md
  // metaindex block 有一个 entry 包含了 FilterPolicy name 
  // 到其对应的 filter block 的映射
  std::string key = "filter.";
  // filter-policy name 在调用方传进来的配置项中
  key.append(rep_->options.filter_policy->Name());
  // 在 metaindex block 搜寻 key 对应的 meta block 的 handle
  iter->Seek(key); 
  if (iter->Valid() && iter->key() == Slice(key)) {
    // 2 找到了, 迭代器对应的 value 即为 meta block handle,
    // 根据其解析对应的 filter block(就是 meta block), 解析出来的
    // 内容会放到 rep_ 中.
    ReadFilter(iter->value()); 
  }
  delete iter;
  delete meta;
}

// 解析 table 的 filter block(需要先解析 table 的 metaindex block)
// 解析出的数据放到了 table.rep.filter
void Table::ReadFilter(const Slice& filter_handle_value) {
  Slice v = filter_handle_value;
  BlockHandle filter_handle;
  // 解析出 filter block 对应的 blockhandle, 它包含 filter block 
  // 在 sstable 中的偏移量和大小
  if (!filter_handle.DecodeFrom(&v).ok()) {
    return;
  }

  ReadOptions opt;
  if (rep_->options.paranoid_checks) {
    opt.verify_checksums = true;
  }
  BlockContents block;
  // 读取 filter block(从 rep_ 到 block)
  if (!ReadBlock(rep_->file, opt, filter_handle, &block).ok()) {
    return;
  }

  // 如果 blockcontents 中的内存是从堆分配的, 
  // 需要将其地址赋值给 rep_->filter_data 以方便后续释放(见 ~Rep())
  if (block.heap_allocated) {
    rep_->filter_data = block.data.data();
  }
  // 反序列化 filter block (从 block.data 到 FilterBlockReader)
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

// 基于 data block 的 handle (即 index_value 参数) 
// 定位并读取对应的 data block, 然后为该 data block 
// 内容构造一个迭代器. 
// 参数 arg 为 table; 
// 参数 index_value 为指向某个 block 的索引(BlockHandle).
// Table 包含一个 block-cache(一般为 LRU), 定位读取之前会先查
// 该缓存, 不存在才去文件里读, 读出来的 block 会放到
// 前面提到的缓存里.
Iterator* Table::BlockReader(void* arg,
                             const ReadOptions& options,
                             const Slice& index_value) {
  // 参数 arg 为 Table 类型                             
  Table* table = reinterpret_cast<Table*>(arg);
  // 获取该 Table 的对应的 blocks 缓存
  Cache* block_cache = table->rep_->options.block_cache;
  Block* block = nullptr;
  Cache::Handle* cache_handle = nullptr;

  BlockHandle handle;
  Slice input = index_value;
  // 获取 block 索引, 即 BlockHandle
  Status s = handle.DecodeFrom(&input);
  // 除了 BlockHandle 信息外, 我们允许在 index_value 
  // 携带更多数据以在将来支持更多特性. 

  // 如果索引有效, 则从缓存或者文件获取对应的 block
  if (s.ok()) {
    BlockContents contents;
    // 如果该 table 启用了缓存, 就先在 cache 中
    // 查找 index_value 指向的 block 是否在缓存中,
    // 如果命中则可以节省本次查询的时间开销.
    if (block_cache != nullptr) {
      char cache_key_buffer[16];
      // cache_id 和 block 在 table 中的偏移量构成了 cache-key
      EncodeFixed64(cache_key_buffer, table->rep_->cache_id);
      EncodeFixed64(cache_key_buffer+8, handle.offset());
      Slice key(cache_key_buffer, sizeof(cache_key_buffer));
      // 根据索引从缓存中查找 block
      cache_handle = block_cache->Lookup(key);
      if (cache_handle != nullptr) {
        // 查到的 value 就是 index_value 指向的 block
        block = reinterpret_cast<Block*>(block_cache->Value(cache_handle)); 
      } else {
        // 如果 block 不在 cache 中, 就去 table 对应的文件去读取
        s = ReadBlock(table->rep_->file, options, handle, &contents); 
        if (s.ok()) {
          block = new Block(contents);
          // 如果用户允许 block 可被缓存, 则将从文件读取的 block 
          // 放到 table 对应的 cache 中
          if (contents.cachable && options.fill_cache) { 
            // 将 block 插入到 table 对应的缓存
            cache_handle = block_cache->Insert(
                key, block, block->size(), &DeleteCachedBlock);
          }
        }
      }
    } else { 
      // table 禁用缓存, 则直接从文件读取 block
      s = ReadBlock(table->rep_->file, options, handle, &contents);
      if (s.ok()) {
        block = new Block(contents);
      }
    }
  }

  Iterator* iter;
  // 如果 index_value 指向的 block 存在, 则为其创建一个迭代器
  if (block != nullptr) { 
    iter = block->NewIterator(table->rep_->options.comparator);
    // 如果 table 没有配置用于缓存 block 的 cache, 
    //    则为该 block 在其迭代器中注册名为 DeleteBlock 
    //    的清理函数用于在迭代器销毁时释放 block 指向的内存; 
    // 如果 table 配置了用于缓存 block 的 cache,  
    //    则为该 block 在其迭代器中注册名为 ReleaseBlock 
    //    的清理函数用于在迭代器销毁时释放 block 在 cache 中对应的 handle; 
    if (cache_handle == nullptr) {
      iter->RegisterCleanup(&DeleteBlock, block, nullptr);
    } else {
      iter->RegisterCleanup(&ReleaseBlock, block_cache, cache_handle);
    }
  } else {
    // 如果没找到对应的 block, 则返回一个初试状态置错的迭代器
    iter = NewErrorIterator(s);
  }
  return iter;
}

// 先为 data-index block 数据项构造一个迭代器 index_iter, 
// 然后基于 index_iter 查询时, 为其指向的具体 data block 
// 构造一个迭代器 data_iter, 进而可以迭代该 data block 里
// 的全部数据项.
// 这样就构成了一个两级迭代器, 从而实现遍历全部 data blocks 的数据项. 
Iterator* Table::NewIterator(const ReadOptions& options) const {
  return NewTwoLevelIterator(
      rep_->index_block->NewIterator(rep_->options.comparator),
      &Table::BlockReader, const_cast<Table*>(this), options);
}

// 在 table 中查找 k 对应的数据项. 
// 如果 table 具有 filter, 则用 filter 找; 
// 如果没有 filter 则去 data block 里面查找, 
// 并且在找到后通过 saver 保存 key/value. 
// 注意, 针对 data block 的读取和解析发生在这个方法里.
Status Table::InternalGet(const ReadOptions& options, const Slice& k,
                          void* arg,
                          void (*saver)(void*, const Slice&, const Slice&)) {
  Status s;
  // 针对 data index block 构造 iterator
  Iterator* iiter = rep_->index_block->NewIterator(rep_->options.comparator);
  // 在 data index block 中寻找第一个大于等于 k 的数据项, 这个数据项
  // 就是目标 data block 的 handle.
  iiter->Seek(k);
  if (iiter->Valid()) {
    // 取出对应的 data block 的 BlockHandle
    Slice handle_value = iiter->value(); 
    FilterBlockReader* filter = rep_->filter;
    BlockHandle handle;
    // 如果有 filter 找起来就快了, 如果确定
    // 不存在就可以直接反悔了.
    if (filter != nullptr &&
        handle.DecodeFrom(&handle_value).ok() &&
        !filter->KeyMayMatch(handle.offset(), k)) {
      // 没在该 data block 对应的过滤器找到这个 key, 肯定不存在
    } else { 
      // 如果没有 filter, 或者在 filter 中查询时无法笃定
      // key 不存在, 就需要在 block 中进行查找.
      // 看到了没? Open() 方法没有解析任何 data block, 解析
      // 是在这里进行的, 因为这里要查询数据了.
      Iterator* block_iter = BlockReader(this, options, iiter->value());
      block_iter->Seek(k);
      if (block_iter->Valid()) {
        // 将找到的 key/value 保存到输出型参数 arg 中, 
        // 因为后面会将迭代器释放掉.
        (*saver)(arg, block_iter->key(), block_iter->value()); 
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
    if (s.ok()) { // 如果存在 data block, 则将其 block 起始 offset 返回
      result = handle.offset();
    } else { // 解析 BlockHandle 失败, 这种情况不该出现, 
      // 如果出现了则返回 metaindex block 起始 offset 作为预估, 已经快接近 table 文件尾了. 
      // Strange: we can't decode the block handle in the index block.
      // We'll just return the offset of the metaindex block, which is
      // close to the whole file size for this case.
      result = rep_->metaindex_handle.offset();
    }
  } else { // key 比 table 文件任意 key 都大, 
    // 返回 metaindex block 起始 offset 作为预估, 已经快接近 table 文件尾了. 
    // key is past the last key in the file.  Approximate the offset
    // by returning the offset of the metaindex block (which is
    // right near the end of the file).
    result = rep_->metaindex_handle.offset();
  }
  delete index_iter;
  return result;
}

}  // namespace leveldb

// Copyright (c) 2011 The LevelDB Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file. See the AUTHORS file for names of contributors.

#include "db/table_cache.h"

#include "db/filename.h"
#include "leveldb/env.h"
#include "leveldb/table.h"
#include "util/coding.h"

namespace leveldb {

// 封装了 sstable 文件及其对应的 Table 实例.
struct TableAndFile {
  RandomAccessFile* file;
  Table* table;
};

// 一个 deleter, 用于从 Cache 中删除数据项时使用
static void DeleteEntry(const Slice& key, void* value) {
  TableAndFile* tf = reinterpret_cast<TableAndFile*>(value);
  delete tf->table;
  delete tf->file;
  delete tf;
}

static void UnrefEntry(void* arg1, void* arg2) {
  Cache* cache = reinterpret_cast<Cache*>(arg1);
  Cache::Handle* h = reinterpret_cast<Cache::Handle*>(arg2);
  cache->Release(h);
}

TableCache::TableCache(const std::string& dbname,
                       const Options& options,
                       int entries)
    : env_(options.env),
      dbname_(dbname),
      options_(options),
      cache_(NewLRUCache(entries)) {
}

TableCache::~TableCache() {
  delete cache_;
}

// 私有方法.
// 从 cache_ 查找 file_number 对应的 table, 如果查到则将其
// 在 cache_ 对应的指针保存到 handle; 
// 否则, 根据 file_number 读取文件构造一个新的 table, 
// 将其插入到 cache_, 并将结果保存到 handle. 
Status TableCache::FindTable(uint64_t file_number, uint64_t file_size,
                             Cache::Handle** handle) {
  Status s;
  // 将文件号编码到字节数组 buf 中
  char buf[sizeof(file_number)];
  EncodeFixed64(buf, file_number);
  // key 为文件号
  Slice key(buf, sizeof(buf));
  *handle = cache_->Lookup(key);
  // 文件号对应的 Table 文件对象不在 cache 中, 则在磁盘寻找该 Table 文件, 
  // 如果找到则创建其对应的 Table 对象并将其连同文件号保存到 cache 中. 
  if (*handle == nullptr) {
    // 根据 dbname、file_number 还有一个隐含的后缀 .ldb, 
    // 构造一个 table 文件对应的文件名.
    std::string fname = TableFileName(dbname_, file_number);
    RandomAccessFile* file = nullptr;
    Table* table = nullptr;
    // 打开 Table 文件, 随机读模式
    s = env_->NewRandomAccessFile(fname, &file);
    if (!s.ok()) {
      // 不存在该名称的 table 文件, 更换老式的后缀 .sst 再次尝试打开同名的 sstable 文件
      std::string old_fname = SSTTableFileName(dbname_, file_number);
      if (env_->NewRandomAccessFile(old_fname, &file).ok()) {
        s = Status::OK();
      }
    }
    // 根据成功打开的文件, 创建一个 Table 对象并将其地址保存到 table 中
    if (s.ok()) {
      s = Table::Open(options_, file, file_size, &table);
    }

    if (!s.ok()) {
      assert(table == nullptr);
      // sstable 打开失败, 删除之前创建的文件对象
      delete file; 
      // 我们不缓存错误结果, 因为错误可能是暂时的, 或者有人会修复这个文件,
      // 然后我们再自动恢复之.
    } else {
      TableAndFile* tf = new TableAndFile;
      tf->file = file;
      tf->table = table;
      // 将新构造的 table 保存到 cache. 
      // key 为 file_number, value 为 TableAndFile 对象指针
      *handle = cache_->Insert(key, tf, 1, &DeleteEntry);
    }
  }
  return s;
}

// 返回指定 sorted string table 文件对应的迭代器.
// 注意该方法有副作用, 下述.
// 用途: 该方法主要用于在 Version::AddIterators() 遍历
// level 架构中每一个 sstable 文件对应的迭代器, 
// 这些迭代器加上 memtable 的迭代器, 就能遍历整个数据库的内容了.
//
// 具体为:
// 从 table_cache_ 根据 file_number 查找其对应的 table 对象, 
// 若查到则返回其对应迭代器; 否则加载文件(这就是副作用)并生成
// 对应的 table 对象放到 table_cache_ 然后返回
// 新构造的 table 的 iterator.
// 
// 如果 tableptr 非空, 设置 *tableptr 指向返回的 iterator 底下的 Table 对象. 
// 返回的 *tableptr 对象由 cache 所拥有, 所以用户不要删除它; 
// 而且只要 iterator 还活着, 该对象即有效. 
Iterator* TableCache::NewIterator(const ReadOptions& options,
                                  uint64_t file_number,
                                  uint64_t file_size,
                                  Table** tableptr) {
  if (tableptr != nullptr) {
    // 该指针有效, 则清除其内容备用
    // 注意前面有个星号, 跟判断条件不同
    *tableptr = nullptr;
  }

  // Handle 就是 Cache 中存储的数据项的类型
  Cache::Handle* handle = nullptr;
  // 从 cache_ 查找 file_number 对应的 table, 如果查到则将结果保存到 handle; 
  // 否则, 根据 file_number 构造一个新的 table, 
  // 并将其插入到 cache_, 并将结果保存到 handle. 
  Status s = FindTable(file_number, file_size, &handle);
  if (!s.ok()) {
    return NewErrorIterator(s);
  }

  // 取出 table 对象
  Table* table = reinterpret_cast<TableAndFile*>(cache_->Value(handle))->table;
  // 基于该 table 构造一个两级迭代器
  Iterator* result = table->NewIterator(options);
  // 在该迭代器上注册一个负责 GC 的 CleanupFunction 函数, 
  // 该迭代器不再使用的时候该函数负责释放迭代器所属
  // 的 table 在 cache_ 中对应的 handle. 
  result->RegisterCleanup(&UnrefEntry, cache_, handle);
  if (tableptr != nullptr) {
    // 将返回的 table 对象保存到 *tableptr
    *tableptr = table;
  }
  return result;
}

// 从缓存中查找 internal_key 为 k 的数据项. 
// 若对应 sstable 文件不在缓存
// 则会根据 file_number 读取文件生成 Table 实例放到缓存中同时
// 从其中查询 k, 查到后调用 handle_result 进行处理.
// 调用链: DBImpl::Get()->Version::Get()->VersionSet::table_cache_::Get().
Status TableCache::Get(const ReadOptions& options,
                       uint64_t file_number,
                       uint64_t file_size,
                       const Slice& k,
                       void* arg,
                       void (*saver)(void*, const Slice&, const Slice&)) {
  Cache::Handle* handle = nullptr;
  // 取出 sstable 在缓存中对应的 table 实例, 存在 handle 里.
  Status s = FindTable(file_number, file_size, &handle);
  if (s.ok()) {
    Table* t = reinterpret_cast<TableAndFile*>(cache_->Value(handle))->table;
    // 从 table 实例查找 k
    s = t->InternalGet(options, k, arg, saver);
    // 查完了释放
    cache_->Release(handle); 
  }
  return s;
}

void TableCache::Evict(uint64_t file_number) {
  char buf[sizeof(file_number)];
  EncodeFixed64(buf, file_number);
  cache_->Erase(Slice(buf, sizeof(buf)));
}

}  // namespace leveldb

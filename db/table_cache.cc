// Copyright (c) 2011 The LevelDB Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file. See the AUTHORS file for names of contributors.

#include "db/table_cache.h"

#include "db/filename.h"
#include "leveldb/env.h"
#include "leveldb/table.h"
#include "util/coding.h"

namespace leveldb {

struct TableAndFile {
  RandomAccessFile* file;
  Table* table;
};

// 一个 deleter，用于从 Cache 中删除数据项时使用
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

Status TableCache::FindTable(uint64_t file_number, uint64_t file_size,
                             Cache::Handle** handle) {
  Status s;
  char buf[sizeof(file_number)];
  EncodeFixed64(buf, file_number);
  Slice key(buf, sizeof(buf)); // key 为文件号
  *handle = cache_->Lookup(key);
  if (*handle == nullptr) {
    // 根据 dbname、file_number 还有一个隐含的后缀 .ldb ，构造一个 table 文件对应的文件名
    std::string fname = TableFileName(dbname_, file_number);
    RandomAccessFile* file = nullptr;
    Table* table = nullptr;
    // 打开文件，随机读模式
    s = env_->NewRandomAccessFile(fname, &file);
    if (!s.ok()) {
      // 不存在该名称的 table 文件，更换后缀为 .sst 尝试打开同名的 sstable 文件
      std::string old_fname = SSTTableFileName(dbname_, file_number);
      if (env_->NewRandomAccessFile(old_fname, &file).ok()) {
        s = Status::OK();
      }
    }
    if (s.ok()) { // 根据成功打开的文件，打开文件对应的 table
      s = Table::Open(options_, file, file_size, &table);
    }

    if (!s.ok()) {
      assert(table == nullptr);
      delete file; // table 打开失败，删除之前创建的文件对象
      // We do not cache error results so that if the error is transient,
      // or somebody repairs the file, we recover automatically.
    } else {
      TableAndFile* tf = new TableAndFile;
      tf->file = file;
      tf->table = table;
      // 将新构造的 table 保存到 cache。
      // key 为 file_number，value 为 TableAndFile 对象指针
      *handle = cache_->Insert(key, tf, 1, &DeleteEntry);
    }
  }
  return s;
}

Iterator* TableCache::NewIterator(const ReadOptions& options,
                                  uint64_t file_number,
                                  uint64_t file_size,
                                  Table** tableptr) {
  if (tableptr != nullptr) {
    // 该指针有效
    // 注意前面有个星号，跟判断条件不同
    *tableptr = nullptr;
  }

  Cache::Handle* handle = nullptr;
  // 从 cache_ 查找 file_number 对应的 table，如果查到则将结果保存到 handle；
  // 否则，根据 file_number 构造一个新的 table，并将其插入到 cache_，并将结果保存到 handle。
  Status s = FindTable(file_number, file_size, &handle);
  if (!s.ok()) {
    return NewErrorIterator(s);
  }

  // 取出 table 对象
  Table* table = reinterpret_cast<TableAndFile*>(cache_->Value(handle))->table;
  Iterator* result = table->NewIterator(options); // 基于该 table 构造一个双层迭代器
  // 在该迭代器上注册一个 CleanupFunction 函数，
  // 该迭代器不再使用的时候会释放其对应的 table 在 cache_ 中对应的 handle。
  result->RegisterCleanup(&UnrefEntry, cache_, handle);
  if (tableptr != nullptr) {
    *tableptr = table; // 将返回的 table 对象保存到 *tableptr
  }
  return result;
}

Status TableCache::Get(const ReadOptions& options,
                       uint64_t file_number,
                       uint64_t file_size,
                       const Slice& k,
                       void* arg,
                       void (*saver)(void*, const Slice&, const Slice&)) {
  Cache::Handle* handle = nullptr;
  Status s = FindTable(file_number, file_size, &handle);
  if (s.ok()) {
    Table* t = reinterpret_cast<TableAndFile*>(cache_->Value(handle))->table;
    s = t->InternalGet(options, k, arg, saver);
    cache_->Release(handle); // 查完了释放
  }
  return s;
}

void TableCache::Evict(uint64_t file_number) {
  char buf[sizeof(file_number)];
  EncodeFixed64(buf, file_number);
  cache_->Erase(Slice(buf, sizeof(buf)));
}

}  // namespace leveldb

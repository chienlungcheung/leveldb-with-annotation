// Copyright (c) 2011 The LevelDB Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file. See the AUTHORS file for names of contributors.
//
// Thread-safe (provides internal synchronization)

#ifndef STORAGE_LEVELDB_DB_TABLE_CACHE_H_
#define STORAGE_LEVELDB_DB_TABLE_CACHE_H_

#include <string>
#include <stdint.h>
#include "db/dbformat.h"
#include "leveldb/cache.h"
#include "leveldb/table.h"
#include "port/port.h"

namespace leveldb {

class Env;

// 借助底层的 cache_ 实现了一个缓存，key 为 table 文件的 file_number，value 为 Table 对象的一个封装。
class TableCache {
 public:
  TableCache(const std::string& dbname, const Options& options, int entries);
  ~TableCache();

  // Return an iterator for the specified file number (the corresponding
  // file length must be exactly "file_size" bytes).  If "tableptr" is
  // non-null, also sets "*tableptr" to point to the Table object
  // underlying the returned iterator, or to nullptr if no Table object
  // underlies the returned iterator.  The returned "*tableptr" object is owned
  // by the cache and should not be deleted, and is valid for as long as the
  // returned iterator is live.
  //
  // 针对给定的 file_number（对应的文件长度也必须恰好是 file_size 字节数），返回一个其对应 table 的 iterator。
  // 如果 tableptr 非空，设置 *tableptr 指向返回的 iterator 底下的 Table 对象。
  // 返回的 *tableptr 对象由 cache 所拥有，所以用户不要删除它；而且只要 iterator 还活着，该对象即有效。
  Iterator* NewIterator(const ReadOptions& options,
                        uint64_t file_number,
                        uint64_t file_size,
                        Table** tableptr = nullptr);

  // If a seek to internal key "k" in specified file finds an entry,
  // call (*handle_result)(arg, found_key, found_value).
  //
  // 如果在某个特定文件中找到了 internal_key 为 k 的数据项，
  // 则调用 (*handle_result)(arg, found_key, found_value) 进行处理。
  Status Get(const ReadOptions& options,
             uint64_t file_number,
             uint64_t file_size,
             const Slice& k,
             void* arg,
             void (*handle_result)(void*, const Slice&, const Slice&));

  // Evict any entry for the specified file number
  //
  // 从 cache_ 删除 file_number 对应的 table 对象
  void Evict(uint64_t file_number);

 private:
  Env* const env_;
  const std::string dbname_;
  const Options& options_;
  // 一个基于特定淘汰算法（如 LRU）的 Cache
  Cache* cache_;

  // 从 cache_ 查找 file_number 对应的 table，如果查到则将结果保存到 handle；
  // 否则，根据 file_number 构造一个新的 table，并将其插入到 cache_，并将结果保存到 handle。
  Status FindTable(uint64_t file_number, uint64_t file_size, Cache::Handle**);
};

}  // namespace leveldb

#endif  // STORAGE_LEVELDB_DB_TABLE_CACHE_H_

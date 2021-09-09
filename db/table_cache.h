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

// 一个用于缓存磁盘上 sstable 文件对应的 Table 实例的缓存.
//
// 用途:
// 每次用户进行查询操作的时候(DBImpl::Get())可能需要去查询
// 磁盘上的文件, 这就要求有个缓存功能来加速.
// TableCache 会缓存 sstable 文件对应的 Table 实例, 
// 用于加速用户的查询, 否则每次读文件解析
// 就很慢了. 目前在用的缓存策略是 LRU.
// 每个 db 实例都会持有一个 TableCache 实例, 对该缓存的
// 的填充是通过副作用实现的, 即当外部调用 
// DBImpl::Get()->Version::Get()->VersionSet::table_cache_::Get()
// 进行查询的时候, 如果发现 sstable 对应 Table 实例不在缓存
// 就会将其填充进来.
// 
// 实现:
// 它的底层是一个 LRUCache, 其中 key 为 sstable 文件
// 的 file_number, value 为 Table 实例的一个封装. 
class TableCache {
 public:
  TableCache(const std::string& dbname, const Options& options, int entries);
  ~TableCache();

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
  Iterator* NewIterator(const ReadOptions& options,
                        uint64_t file_number,
                        uint64_t file_size,
                        Table** tableptr = nullptr);
 
  // 从缓存中查找 internal_key 为 k 的数据项. 
  // 若对应 sstable 文件不在缓存
  // 则会根据 file_number 读取文件生成 Table 实例放到缓存中同时
  // 从其中查询 k, 查到后调用 handle_result 进行处理.
  // 调用链: DBImpl::Get()->Version::Get()->VersionSet::table_cache_::Get().
  Status Get(const ReadOptions& options,
             uint64_t file_number,
             uint64_t file_size,
             const Slice& k,
             void* arg,
             void (*handle_result)(void*, const Slice&, const Slice&));

  // 从 LRUCache 驱逐 file_number 对应的 table 对象
  void Evict(uint64_t file_number);

 private:
  // 一些环境变量
  Env* const env_;
  // 对应的数据库名字
  const std::string dbname_;
  // 一些控制参数
  const Options& options_;
  // 一个基于特定淘汰算法(如 LRU)的 Cache
  Cache* cache_;

  // 私有方法.
  // 从 cache_ 查找 file_number 对应的 table, 如果查到则将其
  // 在 cache_ 对应的指针保存到 handle; 
  // 否则, 根据 file_number 读取文件构造一个新的 table, 
  // 将其插入到 cache_, 并将结果保存到 handle. 
  Status FindTable(uint64_t file_number, uint64_t file_size, Cache::Handle**);
};

}  // namespace leveldb

#endif  // STORAGE_LEVELDB_DB_TABLE_CACHE_H_

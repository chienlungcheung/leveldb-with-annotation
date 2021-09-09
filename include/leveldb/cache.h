// Copyright (c) 2011 The LevelDB Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file. See the AUTHORS file for names of contributors.
//
// A Cache is an interface that maps keys to values.  It has internal
// synchronization and may be safely accessed concurrently from
// multiple threads.  It may automatically evict entries to make room
// for new entries.  Values have a specified charge against the cache
// capacity.  For example, a cache where the values are variable
// length strings, may use the length of the string as the charge for
// the string.
//
// A builtin cache implementation with a least-recently-used eviction
// policy is provided.  Clients may use their own implementations if
// they want something more sophisticated (like scan-resistance, a
// custom eviction policy, variable cache sizing, etc.)

#ifndef STORAGE_LEVELDB_INCLUDE_CACHE_H_
#define STORAGE_LEVELDB_INCLUDE_CACHE_H_

#include <stdint.h>
#include "leveldb/export.h"
#include "leveldb/slice.h"

namespace leveldb {

class LEVELDB_EXPORT Cache;

/**
 * 创建一个固定大小的基于 LRU 算法的 cache
 */
LEVELDB_EXPORT Cache* NewLRUCache(size_t capacity);

// Cache 就是一个用来保存 <key, value> 数据项, 它内部自带同步设施, 并发安全. 
//
// 如果满了, Cache 可以自动地清除之前的数据为新数据腾地方. 
//
// leveldb 自带的实现是基于 LRU 算法的(ShardedLRUCache), 使用者也可以定制自己的缓存算法. 
//
class LEVELDB_EXPORT Cache {
 public:
  Cache() = default;

  Cache(const Cache&) = delete;
  Cache& operator=(const Cache&) = delete;

  // 析构时调用构造时传入的 deleter 函数销毁每一个数据项. 
  virtual ~Cache();

  // Cache 中存储的数据项的抽象类型, 具体实现参见 LRUHandle
  struct Handle { };

  /**
   * 插入一对 <key, value> 到 cache 中, 同时为这个映射设置
   * 一个对 cache 容量的消耗, 具体使用时候用的是要插入的数据
   * 字节数. 
   *
   * 该方法返回一个 handle, 对应本次插入的映射. 
   * 当调用者不再需要这个映射的时候, 需要调用 this->Release(handle). 
   *
   * 当被插入的数据项不再被需要时, key 和 value 将会被传递给这里指定的 deleter. 
   * @param key 要插入的映射的 key
   * @param value 要插入的映射的 value
   * @param charge 要插入的映射对应的花费
   * @param deleter 要插入的映射对应的 deleter
   * @return 要插入的映射对应的 handle
   */
  virtual Handle* Insert(const Slice& key, void* value, size_t charge,
                         void (*deleter)(const Slice& key, void* value)) = 0;

  /**
   * 如果 cache 中没有针对 key 的映射, 返回 nullptr. 
   * 其它情况返回对应该映射的 handle. 
   * 当不再需要这个映射的时候, 调用者必须调用 this->Release(handle). 
   * @param key 要查询映射的 key
   * @return 要查询的映射对应的 handle
   */
  virtual Handle* Lookup(const Slice& key) = 0;


  /**
   * 先通过 Lookup 查询映射对应的 handle, 然后调用该函数来释放该映射. 
   *
   * 前提一: handle 之前未被释放过.
   * 前提二: handle 必须是通过在 *this 上调用某个方法返回的.
   * @param handle 通过 Lookup 查询到的映射对应的 handle
   */
  virtual void Release(Handle* handle) = 0;

  /**
   * 成功调用 Lookup 后返回的 handle 中封装的 value 可以通过该方法解析. 
   *
   * 前提一: handle 之前未被释放过
   * 前提二: handle 必须是通过在 *this 上调用某个方法返回的
   * @param handle
   * @return
   */
  virtual void* Value(Handle* handle) = 0;

  /**
   * 如果 cache 包含了 key 对应的映射, 删除之. 
   * 注意, 底层的数据项将会继续存在直到现有的指向该数据项的全部 handles 已被释放掉. 
   * @param key 要删除的映射对应的 key
   */
  virtual void Erase(const Slice& key) = 0;

  /**
   * 返回一个新生成的数字 id. 
   * 可能会被共享同一个 cache 的多个客户端用来对键空间进行分区.
   *
   * 典型地用法是, 某个客户端在启动时调用该方法生成一个新 id, 
   * 然后将该 id 作为它的 keys 的前缀. 
   * @return
   */
  virtual uint64_t NewId() = 0;

  /**
   * 移除 cache 中全部不再活跃的数据项. 
   * 内存受限的应用可以调用该方法来减少缓存造成的内存消耗. 
   *
   * 该方法的默认实现什么也不做, 强烈建议在派生类实现中重写该方法. 
   * leveldb 未来版本可能会将该方法修改为一个纯抽象方法. 
   */
  virtual void Prune() {}

  /**
   * 返回 cache 为了存储当前全部元素的总花费的估计值
   * @return
   */
  virtual size_t TotalCharge() const = 0;

 private:
  void LRU_Remove(Handle* e);
  void LRU_Append(Handle* e);
  void Unref(Handle* e);

  struct Rep;
  Rep* rep_;
};

}  // namespace leveldb

#endif  // STORAGE_LEVELDB_INCLUDE_CACHE_H_

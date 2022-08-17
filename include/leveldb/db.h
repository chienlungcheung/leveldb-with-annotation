// Copyright (c) 2011 The LevelDB Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file. See the AUTHORS file for names of contributors.

#ifndef STORAGE_LEVELDB_INCLUDE_DB_H_
#define STORAGE_LEVELDB_INCLUDE_DB_H_

#include <stdint.h>
#include <stdio.h>
#include "leveldb/export.h"
#include "leveldb/iterator.h"
#include "leveldb/options.h"

namespace leveldb {

/**
 * 如果修改了下面的两个值, 需要对应更新 Makefile. 
 */
static const int kMajorVersion = 1;
static const int kMinorVersion = 20;

struct Options;
struct ReadOptions;
struct WriteOptions;
class WriteBatch; // 通过所使用的 Handler 与 MemTable 联系了起来, 后者内部存储结构是一个 SkipList. 

/**
 * 对应 db 特定状态的抽象 handle.
 *
 * 快照是不可变的对象, 因此不使用额外的同步实施就可以被多个线程安全地并发访问.
 */
class LEVELDB_EXPORT Snapshot {
 protected:
  virtual ~Snapshot();
};

/**
 * 定义一个 keys 范围, 左闭右开
 */
struct LEVELDB_EXPORT Range {
  Slice start;          // Included in the range
  Slice limit;          // Not included in the range

  Range() { }
  Range(const Slice& s, const Slice& l) : start(s), limit(l) { }
};

/**
 * 一个 DB 就是一个持久化的有序 kv map. 
 *
 * 一个 DB 可以被多个线程不采用任何外部同步设施进行安全地并发访问. 
 */
class LEVELDB_EXPORT DB {
 public:
  /**
   * 打开一个名为 name 的数据库. 
   *
   * 打开成功, 会把一个指向基于堆内存的数据库指针存储到 *dbptr, 同时返回 OK;
   * 如果打开失败, 存储 nullptr 到 *dbptr 同时返回一个错误状态.
   *
   * 调用者不再使用这个数据库时需要负责释放 *dbptr 指向的内存. 
   * 
   * @param options 控制数据库行为和性能的参数配置
   * @param name 数据库名称
   * @param dbptr 存储指向堆内存中数据库的指针
   * @return
   */
  static Status Open(const Options& options,
                     const std::string& name,
                     DB** dbptr);

  DB() = default; // 显式地要求编译器生成一个默认构造函数

  DB(const DB&) = delete; // 禁止拷贝构造
  DB& operator=(const DB&) = delete; // 禁止赋值构造

  virtual ~DB();

  /**
   * 将 <key, value> 对写入数据库, 成功返回 OK, 失败返回错误状态. 
   * @param options 本次写操作相关的配置参数, 如果有需要可以将该参数中的 sync 置为 true, 不容易丢数据但更慢. 
   * @param key Slice 类型的 key
   * @param value Slice 类型的 value
   * @return 返回类型为 Status
   */
  virtual Status Put(const WriteOptions& options,
                     const Slice& key,
                     const Slice& value) = 0;

  /**
   * 从数据删除指定键为 key 的键值对. 如果 key 不存在不算错. 
   *
   * @param options 本次写操作相关的配置参数, 如果有需要可以将该参数中的 sync 置为 true, 不容易丢数据但更慢. 
   * @param key 要删除数据项对应的 key
   * @return
   */
  virtual Status Delete(const WriteOptions& options, const Slice& key) = 0;

  /**
   * 对数据库进行批量更新操作.
   *
   * 该方法线程安全, 内部自带同步. 
   *
   * @param options 本次写操作相关的配置参数, 如果有需要可以将该参数中的 sync 置为 true, 不容易丢数据但更慢. 
   * @param updates 要进行的批量更新操作
   * @return
   */
  virtual Status Write(const WriteOptions& options, WriteBatch* updates) = 0;

  /**
   * 查询键为 key 的数据项, 如果存在则将对应的 value 地址存储到第二个参数中. 
   *
   * 如果 key 不存在, 第二个参数不变, 返回值为 IsNotFound Status. 
   *
   * @param options 本次读操作对应的配置参数
   * @param key 要查询的 key, Slice 引用类型
   * @param value 存储与 key 对应的值的指针, string 指针类型
   * @return
   */
  virtual Status Get(const ReadOptions& options,
                     const Slice& key, std::string* value) = 0;

  /**
   * 返回基于堆内存的迭代器, 可以用该迭代器遍历整个数据库的内容. 
   * 该函数返回的迭代器初始是无效的(在使用迭代器之前, 调用者必须在其上调用 Seek 方法). 
   *
   * 当不再使用时, 调用者应该释放该迭代器对应的内存, 而且迭代器必须在数据库释放之前进行释放. 
   * @param options 本次读操作对应的配置参数
   * @return
   */
  virtual Iterator* NewIterator(const ReadOptions& options) = 0;

  /**
   * 返回当前 DB 状态的一个快照. 使用该快照创建的全部迭代器将会都指向一个当前 DB 的一个稳定快照. 
   *
   * 当不再使用该快照时, 调用者必须调用 ReleaseSnapshot 将其释放. 
   * @return
   */
  virtual const Snapshot* GetSnapshot() = 0;

  /**
   * 释放一个之前获取的快照, 释放后, 调用者不能再使用该快照了. 
   * @param snapshot 指向要释放的快照的指针
   */
  virtual void ReleaseSnapshot(const Snapshot* snapshot) = 0;

  /**
   * DB 可以通过该方法导出自身状态信息. 如果提供的属性可以被 DB 实现理解, 那么第二个参数将会
   * 存储该属性对应的当前值同时该方法返回 true, 其它情况该方法返回 false. 
   *
   * 合法的属性名称包括: 
   *
   * "leveldb.num-files-at-level<N>" - 返回 level <N> 的文件个数, 其中 <N> 是一个 ASCII 格式的数字. 
   *
   * "leveldb.stats" - 返回多行字符串, 描述该 DB 内部操作相关的统计数据. 
   *
   * "leveldb.sstables" - 返回多行字符串, 描述构成该 DB 的全部 sstable 相关信息. 
   *
   * "leveldb.approximate-memory-usage" - 返回被该 DB 使用的内存字节数近似值
   * @param property 要查询的属性名称
   * @param value 保存属性名称对应的属性值
   * @return
   */
  virtual bool GetProperty(const Slice& property, std::string* value) = 0;

  /**
   * 对于 [0, n-1] 中每个 i, 将位于 [range[i].start .. range[i].limit) 中全部 keys 所占用文件系统空间
   * 近似大小存储到 sizes[i] 中. 
   *
   * 注意, 如果数据被压缩过了, 那么返回的 sizes 存储的就是压缩后数据所占用文件系统空间大小. 
   *
   * 返回结果可能不包含最近刚写入的数据所占用空间. 
   * @param range 指定要查询一组 keys 范围
   * @param n range 和 sizes 两个数组的大小
   * @param sizes 存储查询到的每个 range 对应的文件系统空间近似大小
   */
  virtual void GetApproximateSizes(const Range* range, int n,
                                   uint64_t* sizes) = 0;

  /**
   * 将键范围 [*begin,*end] 对应的底层存储压实, 注意范围是左闭右闭. 
   *
   * 压实过程中, 已经被删除或者被覆盖过的数据会被丢弃, 同时会将数据重新安放以减少后续数据访问操作的成本. 
   * 这个操作是为那些理解底层实现的用户准备的. 
   *
   * 如果 begin==nullptr, 则从第一个键开始; 如果 end==nullptr 则到最后一个键为止. 所以, 如果像下面这样做则意味着压紧整个数据库: 
   *
   * db->CompactRange(nullptr, nullptr);
   * @param begin 起始键
   * @param end 截止键
   */
  virtual void CompactRange(const Slice* begin, const Slice* end) = 0;
};

/**
 * 销毁指定数据库的全部内容, 该方法请慎用. 
 *
 * 注意: 为了保持向后兼容, 如果该方法无法列出数据库文件, 仍会返回 Status::OK() 以掩盖这种失败. 
 * @param name 要销毁的数据库名称
 * @param options 销毁时使用的配置参数
 * @return
 */
LEVELDB_EXPORT Status DestroyDB(const std::string& name,
                                const Options& options);

/**
 * 如果 DB 无法打开, 你可以调用该方法尝试修复尽量多的数据库内容. 
 * 修复操作可能导致部分数据丢失, 所以
 * 针对包含重要信息的数据库, 调用该函数时要小心. 
 * @param dbname 要修复的数据库名称
 * @param options 修复时使用的配置参数
 * @return
 */
LEVELDB_EXPORT Status RepairDB(const std::string& dbname,
                               const Options& options);

}  // namespace leveldb

#endif  // STORAGE_LEVELDB_INCLUDE_DB_H_

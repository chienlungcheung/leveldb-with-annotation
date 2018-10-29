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

// Update Makefile if you change these
/**
 * 如果修改了下面的两个值，需要对应更新 Makefile。
 */
static const int kMajorVersion = 1;
static const int kMinorVersion = 20;

struct Options;
struct ReadOptions;
struct WriteOptions;
class WriteBatch; // 通过所使用的 Handler 与 MemTable 联系了起来，后者内部存储结构是一个 SkipList。

// Abstract handle to particular state of a DB.
// A Snapshot is an immutable object and can therefore be safely
// accessed from multiple threads without any external synchronization.
/**
 * 抽象类，对应某个 DB 特定的状态。
 *
 * 一个快照是不可变的，因此可以不使用额外的同步实施而被多个线程安全地并发访问。
 */
class LEVELDB_EXPORT Snapshot {
 protected:
  virtual ~Snapshot();
};

// A range of keys
/**
 * 定义一个 keys 范围，左闭右开
 */
struct LEVELDB_EXPORT Range {
  Slice start;          // Included in the range
  Slice limit;          // Not included in the range

  Range() { }
  Range(const Slice& s, const Slice& l) : start(s), limit(l) { }
};

// A DB is a persistent ordered map from keys to values.
// A DB is safe for concurrent access from multiple threads without
// any external synchronization.
/**
 * 一个 DB 就是一个持久化的有序 map。
 *
 * 一个 DB 可以被多个线程不采用任何外部同步设施进行安全地并发访问。
 */
class LEVELDB_EXPORT DB {
 public:
  // Open the database with the specified "name".
  // Stores a pointer to a heap-allocated database in *dbptr and returns
  // OK on success.
  // Stores nullptr in *dbptr and returns a non-OK status on error.
  // Caller should delete *dbptr when it is no longer needed.
  /**
   * 打开一个名为 name 的数据库。
   *
   * 打开成功，会把一个指向基于堆内存的数据库指针存储到 *dbptr，同时返回 OK；如果打开失败，
   * 存储 nullptr 到 *dbptr 同时返回一个错误状态。
   *
   * 调用者不再使用这个数据库时需要负责释放 *dbptr 指向的内存。
   * @param options 控制数据库行为和性能的参数配置
   * @param name 数据库名称
   * @param dbptr 存储指向堆内存中数据库的指针
   * @return
   */
  static Status Open(const Options& options,
                     const std::string& name,
                     DB** dbptr);

  DB() = default;

  DB(const DB&) = delete; // 禁止拷贝构造
  DB& operator=(const DB&) = delete; // 禁止赋值构造

  virtual ~DB();

  // Set the database entry for "key" to "value".  Returns OK on success,
  // and a non-OK status on error.
  // Note: consider setting options.sync = true.
  /**
   * 将 <key, value> 对写入数据库，成功返回 OK，失败返回错误状态。
   * @param options 本次写操作相关的配置参数，如果有需要可以将该参数中的 sync 置为 true，不容易丢数据但更慢。
   * @param key Slice 类型的 key
   * @param value Slice 类型的 value
   * @return 返回类型为 Status
   */
  virtual Status Put(const WriteOptions& options,
                     const Slice& key,
                     const Slice& value) = 0;

  // Remove the database entry (if any) for "key".  Returns OK on
  // success, and a non-OK status on error.  It is not an error if "key"
  // did not exist in the database.
  // Note: consider setting options.sync = true.
  /**
   * 从数据删除指定键为 key 的键值对。如果 key 不存在不算错。
   *
   * @param options 本次写操作相关的配置参数，如果有需要可以将该参数中的 sync 置为 true，不容易丢数据但更慢。
   * @param key 要删除数据项对应的 key
   * @return
   */
  virtual Status Delete(const WriteOptions& options, const Slice& key) = 0;

  // Apply the specified updates to the database.
  // Returns OK on success, non-OK on failure.
  // Note: consider setting options.sync = true.
  /**
   * 对数据库进行批量更新写操作.
   *
   * 该方法线程安全，内部自带同步。
   *
   * @param options 本次写操作相关的配置参数，如果有需要可以将该参数中的 sync 置为 true，不容易丢数据但更慢。
   * @param updates 要进行的批量更新操作
   * @return
   */
  virtual Status Write(const WriteOptions& options, WriteBatch* updates) = 0;

  // If the database contains an entry for "key" store the
  // corresponding value in *value and return OK.
  //
  // If there is no entry for "key" leave *value unchanged and return
  // a status for which Status::IsNotFound() returns true.
  //
  // May return some other Status on an error.
  /**
   * 查询键为 key 的数据项，如果存在则将对应的 value 地址存储到第二个参数中。
   *
   * 如果 key 不存在，第二个参数不变，返回值为 IsNotFound Status。
   *
   * @param options 本次读操作对应的配置参数
   * @param key 要查询的 key，Slice 引用类型
   * @param value 存储与 key 对应的值的指针，string 指针类型
   * @return
   */
  virtual Status Get(const ReadOptions& options,
                     const Slice& key, std::string* value) = 0;

  // Return a heap-allocated iterator over the contents of the database.
  // The result of NewIterator() is initially invalid (caller must
  // call one of the Seek methods on the iterator before using it).
  //
  // Caller should delete the iterator when it is no longer needed.
  // The returned iterator should be deleted before this db is deleted.
  /**
   * 返回基于堆内存的迭代器，可以用该迭代器遍历整个数据库的内容。
   * 该函数返回的迭代器初始是无效的（在使用迭代器之前，调用者必须在其上调用 Seek 方法）。
   *
   * 当不再使用时，调用者应该释放该迭代器对应的内存，而且迭代器必须在数据库释放之前进行释放。
   * @param options 本次读操作对应的配置参数
   * @return
   */
  virtual Iterator* NewIterator(const ReadOptions& options) = 0;

  // Return a handle to the current DB state.  Iterators created with
  // this handle will all observe a stable snapshot of the current DB
  // state.  The caller must call ReleaseSnapshot(result) when the
  // snapshot is no longer needed.
  /**
   * 返回当前 DB 状态的一个快照。使用该快照创建的全部迭代器将会都指向一个当前 DB 的一个稳定快照。
   *
   * 当不再使用该快照时，滴啊用着必须调用 ReleaseSnapshot 将其释放。
   * @return
   */
  virtual const Snapshot* GetSnapshot() = 0;

  // Release a previously acquired snapshot.  The caller must not
  // use "snapshot" after this call.
  /**
   * 释放一个之前获取的快照，释放后，调用者不能再使用该快照了。
   * @param snapshot 指向要释放的快照的指针
   */
  virtual void ReleaseSnapshot(const Snapshot* snapshot) = 0;

  // DB implementations can export properties about their state
  // via this method.  If "property" is a valid property understood by this
  // DB implementation, fills "*value" with its current value and returns
  // true.  Otherwise returns false.
  //
  //
  // Valid property names include:
  //
  //  "leveldb.num-files-at-level<N>" - return the number of files at level <N>,
  //     where <N> is an ASCII representation of a level number (e.g. "0").
  //  "leveldb.stats" - returns a multi-line string that describes statistics
  //     about the internal operation of the DB.
  //  "leveldb.sstables" - returns a multi-line string that describes all
  //     of the sstables that make up the db contents.
  //  "leveldb.approximate-memory-usage" - returns the approximate number of
  //     bytes of memory in use by the DB.
  /**
   * DB 实现可以通过该方法导出自身状态相关的属性。如果提供的属性可以被 DB 实现理解，那么第二个参数将会
   * 存储该属性对应的当前值同时该方法返回 true，其它情况该方法返回 false。
   *
   * 合法的属性名称包括：
   *
   * "leveldb.num-files-at-level<N>" - 返回 level <N> 的文件个数，其中 <N> 是一个 ASCII 格式的数字。
   *
   * "leveldb.stats" - 返回多行字符串，描述该 DB 内部操作相关的统计数据。
   *
   * "leveldb.sstables" - 返回多行字符串，描述构成该 DB 的全部 sstable 相关信息。
   *
   * "leveldb.approximate-memory-usage" - 返回被该 DB 使用的内存字节数近似值
   * @param property 要查询的属性名称
   * @param value 保存属性名称对应的属性值
   * @return
   */
  virtual bool GetProperty(const Slice& property, std::string* value) = 0;

  // For each i in [0,n-1], store in "sizes[i]", the approximate
  // file system space used by keys in "[range[i].start .. range[i].limit)".
  //
  // Note that the returned sizes measure file system space usage, so
  // if the user data compresses by a factor of ten, the returned
  // sizes will be one-tenth the size of the corresponding user data size.
  //
  // The results may not include the sizes of recently written data.
  /**
   * 对于 [0, n-1] 中每个 i，将位于 [range[i].start .. range[i].limit) 中全部 keys 所占用文件系统空间
   * 近似大小存储到 sizes[i] 中。
   *
   * 注意，如果数据被压缩过了，那么返回的 sizes 存储的就是压缩后数据所占用文件系统空间大小。
   *
   * 返回结果可能不包含最近刚写入的数据所占用空间。
   * @param range 指定要查询一组 keys 范围
   * @param n range 和 sizes 两个数组的大小
   * @param sizes 存储查询到的每个 range 对应的文件系统空间近似大小
   */
  virtual void GetApproximateSizes(const Range* range, int n,
                                   uint64_t* sizes) = 0;

  // Compact the underlying storage for the key range [*begin,*end].
  // In particular, deleted and overwritten versions are discarded,
  // and the data is rearranged to reduce the cost of operations
  // needed to access the data.  This operation should typically only
  // be invoked by users who understand the underlying implementation.
  //
  // begin==nullptr is treated as a key before all keys in the database.
  // end==nullptr is treated as a key after all keys in the database.
  // Therefore the following call will compact the entire database:
  //    db->CompactRange(nullptr, nullptr);
  /**
   * 将 key 范围 [*begin,*end] 对应的底层存储压紧，注意范围是左闭右闭。
   *
   * 尤其是，压实过程会将已经删除或者复写过的数据会被丢弃，同时会将数据重新安放以减少后续数据访问操作的成本。
   * 这个操作是为那些理解底层实现的用户准备的。
   *
   * 如果 begin==nullptr，则从第一个键开始；如果 end==nullptr 则到最后一个键为止。所以，如果像下面这样做则意味着压紧整个数据库：
   *
   * db->CompactRange(nullptr, nullptr);
   * @param begin 起始键
   * @param end 截止键
   */
  virtual void CompactRange(const Slice* begin, const Slice* end) = 0;
};

// Destroy the contents of the specified database.
// Be very careful using this method.
//
// Note: For backwards compatibility, if DestroyDB is unable to list the
// database files, Status::OK() will still be returned masking this failure.
/**
 * 销毁指定数据库的全部内容，该方法请慎用。
 *
 * 注意：为了保持向后兼容，如果该方法无法列出数据库文件，仍会返回 Status::OK() 以掩盖这种失败。
 * @param name 要销毁的数据库名称
 * @param options 销毁时使用的配置参数
 * @return
 */
LEVELDB_EXPORT Status DestroyDB(const std::string& name,
                                const Options& options);

// If a DB cannot be opened, you may attempt to call this method to
// resurrect as much of the contents of the database as possible.
// Some data may be lost, so be careful when calling this function
// on a database that contains important information.
/**
 * 如果 DB 无法打开，你可以调用该方法尝试复活尽量多的数据库内容。修复操作可能导致部分数据丢失，所以
 * 针对包含重要数据的数据库，要谨慎调用该函数。
 * @param dbname 要修复的数据库名称
 * @param options 修复时使用的配置参数
 * @return
 */
LEVELDB_EXPORT Status RepairDB(const std::string& dbname,
                               const Options& options);

}  // namespace leveldb

#endif  // STORAGE_LEVELDB_INCLUDE_DB_H_

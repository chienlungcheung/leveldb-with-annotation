// Copyright (c) 2011 The LevelDB Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file. See the AUTHORS file for names of contributors.

#ifndef STORAGE_LEVELDB_DB_DB_IMPL_H_
#define STORAGE_LEVELDB_DB_DB_IMPL_H_

#include <deque>
#include <set>
#include "db/dbformat.h"
#include "db/log_writer.h"
#include "db/snapshot.h"
#include "leveldb/db.h"
#include "leveldb/env.h"
#include "port/port.h"
#include "port/thread_annotations.h"

namespace leveldb {

class MemTable;
class TableCache;
class Version;
class VersionEdit;
class VersionSet;

class DBImpl : public DB {
 public:
  DBImpl(const Options& options, const std::string& dbname);
  virtual ~DBImpl();

  // Implementations of the DB interface
  virtual Status Put(const WriteOptions&, const Slice& key, const Slice& value);
  virtual Status Delete(const WriteOptions&, const Slice& key);
  virtual Status Write(const WriteOptions& options, WriteBatch* updates);
  virtual Status Get(const ReadOptions& options,
                     const Slice& key,
                     std::string* value);
  virtual Iterator* NewIterator(const ReadOptions&);
  virtual const Snapshot* GetSnapshot();
  virtual void ReleaseSnapshot(const Snapshot* snapshot);
  virtual bool GetProperty(const Slice& property, std::string* value);
  virtual void GetApproximateSizes(const Range* range, int n, uint64_t* sizes);
  virtual void CompactRange(const Slice* begin, const Slice* end);

  // Extra methods (for testing) that are not in the public DB interface

  // Compact any files in the named level that overlap [*begin,*end]
  void TEST_CompactRange(int level, const Slice* begin, const Slice* end);

  // Force current memtable contents to be compacted.
  Status TEST_CompactMemTable();

  // Return an internal iterator over the current state of the database.
  // The keys of this iterator are internal keys (see format.h).
  // The returned iterator should be deleted when no longer needed.
  Iterator* TEST_NewInternalIterator();

  // Return the maximum overlapping data (in bytes) at next level for any
  // file at a level >= 1.
  int64_t TEST_MaxNextLevelOverlappingBytes();

  // Record a sample of bytes read at the specified internal key.
  // Samples are taken approximately once every config::kReadBytesPeriod
  // bytes.
  void RecordReadSample(Slice key);

 private:
  friend class DB;
  struct CompactionState;
  struct Writer;

  Iterator* NewInternalIterator(const ReadOptions&,
                                SequenceNumber* latest_snapshot,
                                uint32_t* seed);

  Status NewDB();

  // Recover the descriptor from persistent storage.  May do a significant
  // amount of work to recover recently logged updates.  Any changes to
  // be made to the descriptor are added to *edit.
  Status Recover(VersionEdit* edit, bool* save_manifest)
      EXCLUSIVE_LOCKS_REQUIRED(mutex_);

  void MaybeIgnoreError(Status* s) const;

  // Delete any unneeded files and stale in-memory entries.
  void DeleteObsoleteFiles() EXCLUSIVE_LOCKS_REQUIRED(mutex_);

  // 将内存中的 memtable 转换为 sstable 文件并写入到磁盘中.
  // 当且仅当该方法执行成功后, 切换到一组新的 log-file/memtable 组合并且写一个新的描述符.
  // 如果执行失败, 则将错误记录到 bg_error_.
	// 调用该方法之前必须获取相应的锁.
  void CompactMemTable() EXCLUSIVE_LOCKS_REQUIRED(mutex_);

  Status RecoverLogFile(uint64_t log_number, bool last_log, bool* save_manifest,
                        VersionEdit* edit, SequenceNumber* max_sequence)
      EXCLUSIVE_LOCKS_REQUIRED(mutex_);

  Status WriteLevel0Table(MemTable* mem, VersionEdit* edit, Version* base)
      EXCLUSIVE_LOCKS_REQUIRED(mutex_);

  Status MakeRoomForWrite(bool force /* compact even if there is room? */)
      EXCLUSIVE_LOCKS_REQUIRED(mutex_);
  WriteBatch* BuildBatchGroup(Writer** last_writer)
      EXCLUSIVE_LOCKS_REQUIRED(mutex_);

  void RecordBackgroundError(const Status& s);

  void MaybeScheduleCompaction() EXCLUSIVE_LOCKS_REQUIRED(mutex_);
  static void BGWork(void* db);
  void BackgroundCall();
  void BackgroundCompaction() EXCLUSIVE_LOCKS_REQUIRED(mutex_);
  void CleanupCompaction(CompactionState* compact)
      EXCLUSIVE_LOCKS_REQUIRED(mutex_);
  Status DoCompactionWork(CompactionState* compact)
      EXCLUSIVE_LOCKS_REQUIRED(mutex_);

  Status OpenCompactionOutputFile(CompactionState* compact);
  Status FinishCompactionOutputFile(CompactionState* compact, Iterator* input);
  Status InstallCompactionResults(CompactionState* compact)
      EXCLUSIVE_LOCKS_REQUIRED(mutex_);

  // Constant after construction 下面这个区域内定义的成员一旦初始化都不允许更改
  Env* const env_;
  const InternalKeyComparator internal_comparator_;
  const InternalFilterPolicy internal_filter_policy_;
  const Options options_;  // options_.comparator == &internal_comparator_
  const bool owns_info_log_;
  const bool owns_cache_;
  const std::string dbname_;

  // table_cache_ 提供了自己的同步设施
  TableCache* const table_cache_; 

  // Lock over the persistent DB state.  Non-null iff successfully acquired.
  FileLock* db_lock_; // 针对 DB 状态的访问需要使用该锁进行同步

  // mutex_ 后面的成员变量均由它来守护
  port::Mutex mutex_;
  port::AtomicPointer shutting_down_;
  port::CondVar background_work_finished_signal_ GUARDED_BY(mutex_);
  // 当前在用的 memtable
  MemTable* mem_;
  // immutable memtable, 即待压实的 memtable, 其对应 log 文件已经满了.
  MemTable* imm_ GUARDED_BY(mutex_);
  // 后台线程可以基于该成员检测是否存在 imm_
  port::AtomicPointer has_imm_;
  // 指向在写 log 文件
  WritableFile* logfile_;
  // 当前在写 log 文件的文件号
  uint64_t logfile_number_ GUARDED_BY(mutex_);
  log::Writer* log_;
  uint32_t seed_ GUARDED_BY(mutex_);  // For sampling.

  // Queue of writers.
  std::deque<Writer*> writers_ GUARDED_BY(mutex_);
  WriteBatch* tmp_batch_ GUARDED_BY(mutex_);

  // leveldb 是个 append 类型而非 in-place 修改的数据库,
  // 所以它的快照和 MySQL 这类直接把某个时间点的全量数据作为
  // 拷贝作为快照的做法不同.
  // 就我目前的理解, leveldb 快照是针对某个 key 的, 因为
  // 针对同一个 key, 在效果上 append 会导致后来的数据项覆盖
  // 前面的数据项. 但如果用户想用前面的数据项咋办呢? 我们用
  // 那个特定数据项对应的序列号来做快照就行了, 查找某个 user key
  // 的时候, 带上快照, 组装成 internal key, 就能找到了.
  SnapshotList snapshots_ GUARDED_BY(mutex_);

  // Set of table files to protect from deletion because they are
  // part of ongoing compactions.
  // table 文件集合, 用于避免某文件在压实时被意外删除. 
  std::set<uint64_t> pending_outputs_ GUARDED_BY(mutex_);

  // Has a background compaction been scheduled or is running?
  // 一个标识, 标识当前后台是否已经调度了压实任务(memtable 转 sstable, 
  // 或者客户端手动触发或者某个 level 达到压实条件)(无论是否执行中)
  bool background_compaction_scheduled_ GUARDED_BY(mutex_);

  // 手工压实相关的信息
  struct ManualCompaction {
    int level;
    bool done;
    const InternalKey* begin;   // null 意味着 key 范围的负无穷
    const InternalKey* end;     // null 意味着 key 范围的正无穷
    InternalKey tmp_storage;    // 用于追踪压实进展
  };
  ManualCompaction* manual_compaction_ GUARDED_BY(mutex_);

  VersionSet* const versions_;

  // 偏执模式下执行后台压实任务时是否遇到了错误
  Status bg_error_ GUARDED_BY(mutex_);

  // Per level compaction stats.  stats_[level] stores the stats for
  // compactions that produced data for the specified "level".
  // 每个 level 对应的的压实过程统计. 
  // 可以通过 db 提供的 GetProperty 接口供外部查询.
  struct CompactionStats {
    // 记录所属 level 压实累计耗费的时间, 单位毫秒
    int64_t micros;
    int64_t bytes_read;
    // 记录所属 level 压实累计写入的字节数
    int64_t bytes_written;

    CompactionStats() : micros(0), bytes_read(0), bytes_written(0) { }

    void Add(const CompactionStats& c) {
      this->micros += c.micros;
      this->bytes_read += c.bytes_read;
      this->bytes_written += c.bytes_written;
    }
  };
  // 每个 level 对应一个压实状态
  CompactionStats stats_[config::kNumLevels] GUARDED_BY(mutex_);

  // No copying allowed
  DBImpl(const DBImpl&);
  void operator=(const DBImpl&);

  const Comparator* user_comparator() const {
    return internal_comparator_.user_comparator();
  }
};

// Sanitize db options.  The caller should delete result.info_log if
// it is not equal to src.info_log.
Options SanitizeOptions(const std::string& db,
                        const InternalKeyComparator* icmp,
                        const InternalFilterPolicy* ipolicy,
                        const Options& src);

}  // namespace leveldb

#endif  // STORAGE_LEVELDB_DB_DB_IMPL_H_

// Copyright (c) 2011 The LevelDB Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file. See the AUTHORS file for names of contributors.

#include "db/db_impl.h"

#include <stdint.h>
#include <stdio.h>

#include <algorithm>
#include <set>
#include <string>
#include <vector>

#include "db/builder.h"
#include "db/db_iter.h"
#include "db/dbformat.h"
#include "db/filename.h"
#include "db/log_reader.h"
#include "db/log_writer.h"
#include "db/memtable.h"
#include "db/table_cache.h"
#include "db/version_set.h"
#include "db/write_batch_internal.h"
#include "leveldb/db.h"
#include "leveldb/env.h"
#include "leveldb/status.h"
#include "leveldb/table.h"
#include "leveldb/table_builder.h"
#include "port/port.h"
#include "table/block.h"
#include "table/merger.h"
#include "table/two_level_iterator.h"
#include "util/coding.h"
#include "util/logging.h"
#include "util/mutexlock.h"

namespace leveldb {

const int kNumNonTableCacheFiles = 10;

// Information kept for every waiting writer
// 针对调用 db 进行的写操作, 都会生成一个对应的 writer, 其封装了写入数据和写入进度.
struct DBImpl::Writer {
  Status status;
  WriteBatch* batch;
  bool sync;
  bool done;
  port::CondVar cv;

  explicit Writer(port::Mutex* mu) : cv(mu) { }
};

struct DBImpl::CompactionState {
  Compaction* const compaction;

  // 小于 smallest_snapshot 的序列号都不用管了, 因为我们
  // 不会服务一个小于 smallest_snapshot 的快照.
  // 因此, 如果我们看到了一个不大于 smallest_snapshot 的序列号 S,
  // 与 S 同一个 key 的其它数据项, 如果序列号小于 S 则可以直接丢弃.
  //
  // 关于快照的作用和目的可以见 DBImpl::snapshots_
  SequenceNumber smallest_snapshot;

  // 压实产生的文件, 对应一个 sstable 文件
  struct Output {
    uint64_t number;
    uint64_t file_size;
    InternalKey smallest, largest;
  };
  std::vector<Output> outputs;

  // State kept for output being generated
  // builder 会向 outfile 写入压实后数据
  WritableFile* outfile;
  TableBuilder* builder;

  uint64_t total_bytes;

  // 返回当前压实在写的文件
  Output* current_output() { return &outputs[outputs.size()-1]; }

  explicit CompactionState(Compaction* c)
      : compaction(c),
        outfile(nullptr),
        builder(nullptr),
        total_bytes(0) {
  }
};

// Fix user-supplied options to be reasonable
template <class T, class V>
static void ClipToRange(T* ptr, V minvalue, V maxvalue) {
  if (static_cast<V>(*ptr) > maxvalue) *ptr = maxvalue;
  if (static_cast<V>(*ptr) < minvalue) *ptr = minvalue;
}
Options SanitizeOptions(const std::string& dbname,
                        const InternalKeyComparator* icmp,
                        const InternalFilterPolicy* ipolicy,
                        const Options& src) {
  Options result = src;
  result.comparator = icmp;
  result.filter_policy = (src.filter_policy != nullptr) ? ipolicy : nullptr;
  ClipToRange(&result.max_open_files,    64 + kNumNonTableCacheFiles, 50000);
  ClipToRange(&result.write_buffer_size, 64<<10,                      1<<30);
  ClipToRange(&result.max_file_size,     1<<20,                       1<<30);
  ClipToRange(&result.block_size,        1<<10,                       4<<20);
  if (result.info_log == nullptr) {
    // Open a log file in the same directory as the db
    src.env->CreateDir(dbname);  // In case it does not exist
    src.env->RenameFile(InfoLogFileName(dbname), OldInfoLogFileName(dbname));
    Status s = src.env->NewLogger(InfoLogFileName(dbname), &result.info_log);
    if (!s.ok()) {
      // No place suitable for logging
      result.info_log = nullptr;
    }
  }
  if (result.block_cache == nullptr) {
    result.block_cache = NewLRUCache(8 << 20);
  }
  return result;
}

// 计算 table_cache_ 容量.
// 从计算过程可以看出, 这个 cache 还是很大的, 
// 几乎把磁盘上的 sorted string tables 文件都在内存中保存了一份.
static int TableCacheSize(const Options& sanitized_options) {
  // Reserve ten files or so for other uses and give the rest to TableCache.
  return sanitized_options.max_open_files - kNumNonTableCacheFiles;
}

DBImpl::DBImpl(const Options& raw_options, const std::string& dbname)
    : env_(raw_options.env),
      internal_comparator_(raw_options.comparator),
      internal_filter_policy_(raw_options.filter_policy),
      options_(SanitizeOptions(dbname, &internal_comparator_,
                               &internal_filter_policy_, raw_options)),
      owns_info_log_(options_.info_log != raw_options.info_log),
      owns_cache_(options_.block_cache != raw_options.block_cache),
      dbname_(dbname),
      table_cache_(new TableCache(dbname_, options_, TableCacheSize(options_))),
      db_lock_(nullptr),
      shutting_down_(nullptr),
      background_work_finished_signal_(&mutex_),
      mem_(nullptr),
      imm_(nullptr),
      logfile_(nullptr),
      logfile_number_(0),
      log_(nullptr),
      seed_(0),
      tmp_batch_(new WriteBatch),
      background_compaction_scheduled_(false),
      manual_compaction_(nullptr),
      versions_(new VersionSet(dbname_, &options_, table_cache_,
                               &internal_comparator_)) {
  has_imm_.Release_Store(nullptr);
}

DBImpl::~DBImpl() {
  // Wait for background work to finish
  mutex_.Lock();
  shutting_down_.Release_Store(this);  // Any non-null value is ok 随便存个非空值即可, 标记正在关闭
  while (background_compaction_scheduled_) { // 循环是为了防止虚假唤醒误判
    background_work_finished_signal_.Wait(); // 等待后台工作结束
  }
  mutex_.Unlock();

  if (db_lock_ != nullptr) {
    env_->UnlockFile(db_lock_);
  }

  delete versions_;
  if (mem_ != nullptr) mem_->Unref();
  if (imm_ != nullptr) imm_->Unref();
  delete tmp_batch_;
  delete log_;
  delete logfile_;
  delete table_cache_;

  if (owns_info_log_) {
    delete options_.info_log;
  }
  if (owns_cache_) {
    delete options_.block_cache;
  }
}

// 初始化一个 version_edit 对象, 创建 MANIFEST 文件, 并将前述 version_edit 序列化为一条日志写入到该文件;
// 然后创建 CURRENT 文件, 将 MANIFEST 文件名写入到 CURRENT 文件中.
Status DBImpl::NewDB() {
  VersionEdit new_db;
  new_db.SetComparatorName(user_comparator()->Name());
  new_db.SetLogNumber(0);
  // 下面马上新建一个占用了 1, 所以下一个是 2
  new_db.SetNextFile(2);
  new_db.SetLastSequence(0);

  // 创建 MANIFEST 文件
  const std::string manifest = DescriptorFileName(dbname_, 1);
  WritableFile* file;
  Status s = env_->NewWritableFile(manifest, &file);
  if (!s.ok()) {
    return s;
  }
  {
    // 将上面新创建的 version_edit 内容序列化为一条日志记录到 MANIFEST 文件
    log::Writer log(file);
    std::string record;
    new_db.EncodeTo(&record);
    s = log.AddRecord(record);
    if (s.ok()) {
      s = file->Close();
    }
  }
  delete file;
  // 将 MANIFEST 文件名写到 CURRENT 文件
  if (s.ok()) {
    // Make "CURRENT" file that points to the new manifest file.
    s = SetCurrentFile(env_, dbname_, 1);
  } else {
    env_->DeleteFile(manifest);
  }
  return s;
}

void DBImpl::MaybeIgnoreError(Status* s) const {
  if (s->ok() || options_.paranoid_checks) {
    // No change needed
  } else {
    Log(options_.info_log, "Ignoring error %s", s->ToString().c_str());
    *s = Status::OK();
  }
}

// 删除过期文件
void DBImpl::DeleteObsoleteFiles() {
  mutex_.AssertHeld();

  if (!bg_error_.ok()) {
		// 如果后台任务出错, 我们不清楚新的 version 是否成功提交,
		// 所以我们不能放心地删除文件.
    return;
  }

  // 将当前全部 sstable 文件添加到 live 集合中
  std::set<uint64_t> live = pending_outputs_;
  // 将全部存活 version 中维护的文件添加到 live 集合中
  versions_->AddLiveFiles(&live);

  std::vector<std::string> filenames;
	// 故意忽略返回值可能反馈的错误, 反正是 GC, 能回收多少是多少
  env_->GetChildren(dbname_, &filenames);
  uint64_t number;
  FileType type;
  for (size_t i = 0; i < filenames.size(); i++) {
    if (ParseFileName(filenames[i], &number, &type)) {
      bool keep = true;
      switch (type) {
        case kLogFile:
          keep = ((number >= versions_->LogNumber()) ||
                  (number == versions_->PrevLogNumber()));
          break;
        case kDescriptorFile:
          // Keep my manifest file, and any newer incarnations'
          // (in case there is a race that allows other incarnations)
          keep = (number >= versions_->ManifestFileNumber());
          break;
        case kTableFile:
          keep = (live.find(number) != live.end());
          break;
        case kTempFile:
          // Any temp files that are currently being written to must
          // be recorded in pending_outputs_, which is inserted into "live"
          keep = (live.find(number) != live.end());
          break;
        case kCurrentFile:
        case kDBLockFile:
        case kInfoLogFile:
          keep = true;
          break;
      }

      if (!keep) {
        if (type == kTableFile) {
          table_cache_->Evict(number);
        }
        Log(options_.info_log, "Delete type=%d #%lld\n",
            static_cast<int>(type),
            static_cast<unsigned long long>(number));
        env_->DeleteFile(dbname_ + "/" + filenames[i]);
      }
    }
  }
}

// 该方法用于刚打开数据库时从磁盘读取数据在内存建立 level 架构.
// save_manifest 用于指示是否续用老的 MANIFEST 文件.
// - 读取 CURRENT 文件(不存在则新建)找到最新的 MANIFEST 文件(不存在则新建)的名称
// - 读取该 MANIFEST 文件内容
// - 清理过期的文件
// - 这一步我们可以打开全部 sstables, 但最好等会再打开
// - 将 log 文件块转换为一个新的 level-0 sstable
// - 将接下来的要写的数据写入一个新的 log 文件
Status DBImpl::Recover(VersionEdit* edit, bool *save_manifest) {
  mutex_.AssertHeld();

  // Ignore error from CreateDir since the creation of the DB is
  // committed only when the descriptor is created, and this directory
  // may already exist from a previous failed creation attempt.
  // 创建数据库目录(一个目录代表一个数据库)
  env_->CreateDir(dbname_);
  assert(db_lock_ == nullptr);
  // 锁定该目录
  Status s = env_->LockFile(LockFileName(dbname_), &db_lock_);
  if (!s.ok()) {
    return s;
  }

  // 如果 CURRENT 文件(记录当前 MENIFEST 文件名称)不存在则创建之
  if (!env_->FileExists(CurrentFileName(dbname_))) {
    if (options_.create_if_missing) {
      // 创建之
      s = NewDB();
      if (!s.ok()) {
        return s;
      }
    } else {
      // 报错
      return Status::InvalidArgument(
          dbname_, "does not exist (create_if_missing is false)");
    }
  } else {
    if (options_.error_if_exists) {
      return Status::InvalidArgument(
          dbname_, "exists (error_if_exists is true)");
    }
  }

  // 该方法负责从最后一个 MANIFEST 文件解析内容出来与当前 Version
  // 保存的 level 架构合并保存到一个
  // 新建的 Version 中, 然后将这个新的 version 作为当前的 version.
  // 参数是输出型的, 负责保存一个指示当前 MANIFEST 文件是否可以续用.
  s = versions_->Recover(save_manifest);
  if (!s.ok()) {
    return s;
  }
  SequenceNumber max_sequence(0);

  // Recover from all newer log files than the ones named in the
  // descriptor (new log files may have been added by the previous
  // incarnation without registering them in the descriptor).
  //
  // Note that PrevLogNumber() is no longer used, but we pay
  // attention to it in case we are recovering a database
  // produced by an older version of leveldb.
  const uint64_t min_log = versions_->LogNumber();
  const uint64_t prev_log = versions_->PrevLogNumber();
  std::vector<std::string> filenames;
  // 获取数据库目录下全部文件列表(后面会把 log 文件筛出来)存到 filenames 中
  s = env_->GetChildren(dbname_, &filenames);
  if (!s.ok()) {
    return s;
  }
  // 从旧到新遍历 version, 在每个 version 中从低到高遍历 level,
  // 将 level 中的文件都插入到集合 expected 中.
  std::set<uint64_t> expected;
  versions_->AddLiveFiles(&expected);
  uint64_t number;
  FileType type;
  std::vector<uint64_t> logs;
  // 遍历数据库目录下全部文件.
  // 筛选出 sorted string table 文件, 验证 version 包含的 level 架构图有效性;
  // 同时将全部 log 文件筛选换出来待下面解析成 memtable.
  for (size_t i = 0; i < filenames.size(); i++) {
    // 解析文件类型
    if (ParseFileName(filenames[i], &number, &type)) {
      expected.erase(number);
      // 若文件类型为 log, 且文件名(是一个数字)不小于当前在写入文件名或者等于当前正在转换为 memtable 的文件名,
      // 则将文该文件加入到恢复列表中.
      if (type == kLogFile && ((number >= min_log) || (number == prev_log)))
        logs.push_back(number);
    }
  }
  // 若 versionset 中记录的文件多于从当前数据库目录中读取到的文件,
  // 则说明数据库目录有文件丢失, 数据库损坏.
  if (!expected.empty()) {
    char buf[50];
    snprintf(buf, sizeof(buf), "%d missing files; e.g.",
             static_cast<int>(expected.size()));
    return Status::Corruption(buf, TableFileName(dbname_, *(expected.begin())));
  }

  // Recover in the order in which the logs were generated
  // 将 log 文件列表按照文件名从旧到新排序, 逐个恢复.
  std::sort(logs.begin(), logs.end());
  for (size_t i = 0; i < logs.size(); i++) {
    // 从旧到新逐个 log 文件恢复, 如果有 log 文件转换为 sorted string table 文件(如大小到达阈值)落盘则
    // 将 save_manifest 标记为 true, 表示需要写日志到 manifest 文件.
    s = RecoverLogFile(logs[i], (i == logs.size() - 1), save_manifest, edit,
                       &max_sequence);
    if (!s.ok()) {
      return s;
    }

    // The previous incarnation may not have written any MANIFEST
    // records after allocating this log number.  So we manually
    // update the file number allocation counter in VersionSet.
    versions_->MarkFileNumberUsed(logs[i]);
  }

  if (versions_->LastSequence() < max_sequence) {
    versions_->SetLastSequence(max_sequence);
  }

  return Status::OK();
}

// 读取 log 文件并将其转为 memtable.
// 如果该 log 文件继续使用则将其对应 memtable 赋值到 mem_ 继续使用;
// 否则将 log 文件对应 memtable 转换为 sstable 文件写入磁盘, 同时标记 save_manifest 为 true,
// 表示 level 架构变动需要记录文件变更到 manifest 文件.
Status DBImpl::RecoverLogFile(uint64_t log_number, bool last_log,
                              bool* save_manifest, VersionEdit* edit,
                              SequenceNumber* max_sequence) {
  struct LogReporter : public log::Reader::Reporter {
    Env* env;
    Logger* info_log;
    const char* fname;
    Status* status;  // null if options_.paranoid_checks==false
    virtual void Corruption(size_t bytes, const Status& s) {
      Log(info_log, "%s%s: dropping %d bytes; %s",
          (this->status == nullptr ? "(ignoring error) " : ""),
          fname, static_cast<int>(bytes), s.ToString().c_str());
      if (this->status != nullptr && this->status->ok()) *this->status = s;
    }
  };

  mutex_.AssertHeld();

  // Open the log file
  std::string fname = LogFileName(dbname_, log_number);
  SequentialFile* file;
  Status status = env_->NewSequentialFile(fname, &file);
  if (!status.ok()) {
    MaybeIgnoreError(&status);
    return status;
  }

  // Create the log reader.
  LogReporter reporter;
  reporter.env = env_;
  reporter.info_log = options_.info_log;
  reporter.fname = fname.c_str();
  reporter.status = (options_.paranoid_checks ? &status : nullptr);
  // We intentionally make log::Reader do checksumming even if
  // paranoid_checks==false so that corruptions cause entire commits
  // to be skipped instead of propagating bad information (like overly
  // large sequence numbers).
  // 创建 log reader, 强制做校验和处理, 避免错误数据传递到其它操作
  log::Reader reader(file, &reporter, true/*checksum*/,
                     0/*initial_offset*/);
  Log(options_.info_log, "Recovering log #%llu",
      (unsigned long long) log_number);

  // Read all the records and add to a memtable
  std::string scratch;
  Slice record;
  WriteBatch batch;
  int compactions = 0;
  MemTable* mem = nullptr;
  // 读取 log 文件并将其转为 memtable
  while (reader.ReadRecord(&record, &scratch) &&
         status.ok()) {
    if (record.size() < 12) {
      reporter.Corruption(
          record.size(), Status::Corruption("log record too small"));
      continue;
    }
    WriteBatchInternal::SetContents(&batch, record);

    if (mem == nullptr) {
      mem = new MemTable(internal_comparator_);
      mem->Ref();
    }
    // 将数据填充到 memtable
    status = WriteBatchInternal::InsertInto(&batch, mem);
    MaybeIgnoreError(&status);
    if (!status.ok()) {
      break;
    }
    const SequenceNumber last_seq =
        WriteBatchInternal::Sequence(&batch) +
        WriteBatchInternal::Count(&batch) - 1;
    if (last_seq > *max_sequence) {
      *max_sequence = last_seq;
    }

    // 如果 memtable 太大了, 将其转为 sorted string table 文件写入磁盘,
    // 同时将其对应的 table 对象放到 table_cache_ 缓存
    if (mem->ApproximateMemoryUsage() > options_.write_buffer_size) {
      compactions++;
      *save_manifest = true;
      status = WriteLevel0Table(mem, edit, nullptr);
      mem->Unref();
      // 如果写入磁盘了, 那么对应的 memtable 就和磁盘数据重复了, 置空, 下次循环会新建
      mem = nullptr;
      if (!status.ok()) {
        // Reflect errors immediately so that conditions like full
        // file-systems cause the DB::Open() to fail.
        break;
      }
    }
  }

  // 读取完了, 释放指向 log 文件的指针
  delete file;

  // See if we should keep reusing the last log file.
  if (status.ok() && options_.reuse_logs && last_log && compactions == 0) {
    assert(logfile_ == nullptr);
    assert(log_ == nullptr);
    assert(mem_ == nullptr);
    uint64_t lfile_size;
    if (env_->GetFileSize(fname, &lfile_size).ok() &&
        env_->NewAppendableFile(fname, &logfile_).ok()) {
      Log(options_.info_log, "Reusing old log %s \n", fname.c_str());
      // 继续使用之前创建的最后一个 log 文件记录日志
      log_ = new log::Writer(logfile_, lfile_size);
      logfile_number_ = log_number;
      // 为当前 log 文件重用(之前未写磁盘)或创建一个新的 memtable;
      // 重用 mem 后其必定被置为空.
      if (mem != nullptr) {
        mem_ = mem;
        mem = nullptr;
      } else {
        // mem can be nullptr if lognum exists but was empty.
        mem_ = new MemTable(internal_comparator_);
        mem_->Ref();
      }
    }
  }

  // log 文件没有被重用, 将其对应 memtable 转为 sorted string table 文件写入磁盘
  if (mem != nullptr) {
    // mem did not get reused; compact it.
    if (status.ok()) {
      *save_manifest = true;
      status = WriteLevel0Table(mem, edit, nullptr);
    }
    mem->Unref();
  }

  return status;
}

// 将 mem 对应的 memtable 以 table 文件形式保存到磁盘,
// 并将本次变更对应的元信息(level、filemeta 等)保存到 edit 中
Status DBImpl::WriteLevel0Table(MemTable* mem, VersionEdit* edit,
                                Version* base) {
  mutex_.AssertHeld();

  // 1. 先把 imm 转换为 sstable 并进行落盘

  const uint64_t start_micros = env_->NowMicros();
  // 对应即将构造的 sstable 文件
  FileMetaData meta;
  // 为 memtable 对应的 table 文件生成一个文件 number
  meta.number = versions_->NewFileNumber();
  // 保护 number 对应的 table 文件, 避免在压实过程中被删除
  pending_outputs_.insert(meta.number);
  // 获取 memtable 对应的迭代器
  Iterator* iter = mem->NewIterator();
  Log(options_.info_log, "Level-0 table #%llu: started",
      (unsigned long long) meta.number);

  Status s;
  {
    // 构造 Table 文件的时候与 mutex_ 要守护的成员变量无关, 可以解除锁定
    mutex_.Unlock();
    // 将 memtable 序列化为一个 sorted string table 文件并写入磁盘,
    // 文件大小会被保存到 meta 中. 同时将 sstable 对应的 Table 实例放入
    // table_cache_ 中.
    s = BuildTable(dbname_, env_, options_, table_cache_, iter, &meta);
    // 构造完毕, 重新获取锁, 诸如 pending_outputs_ 需要 mutex_ 来守护
    mutex_.Lock();
  }

  Log(options_.info_log, "Level-0 table #%llu: %lld bytes %s",
      (unsigned long long) meta.number,
      (unsigned long long) meta.file_size,
      s.ToString().c_str());
  delete iter;

  // sstable 落盘完成, 下面要为其选择对应的 level.

  // meta->number 对应的文件已经写入磁盘
  pending_outputs_.erase(meta.number);

  // 如果 file_size 等于 0, 则对应文件已经被删除了而且不应该加入到 manifest 中.
  int level = 0;
  if (s.ok() && meta.file_size > 0) {
    // 2. sstable 落盘完成, 下面要为其选择对应的 level.

    // meta 相关成员信息在 BuildTable 时填充过了
    const Slice min_user_key = meta.smallest.user_key();
    const Slice max_user_key = meta.largest.user_key();
    if (base != nullptr) {
      // 为 [min_user_key, max_user_key] 对应的 sstable 文件找一个落脚的 level.
      // 注意, leveldb 文件存储和 level 架构信息存储是分开的,
      // 文件落盘就是直接写, 相关架构信息如具体属于哪个 level, 包含的键区间,
			// 另外记录到其它地方.
      level = base->PickLevelForMemTableOutput(min_user_key, max_user_key);
    }

    // 3. 将 sstable 相关元信息记录到 edit, 方便后面更新 level 架构.

    // 将 [min_user_key, max_user_key] 对应的 Table 文件
		// 元信息及其 level 记录到 edit 中
    edit->AddFile(level, meta.number, meta.file_size,
                  meta.smallest, meta.largest);
  }

  // 将本次压实过程对应的数据存储到 level 层对应的压实状态中
  CompactionStats stats;
  stats.micros = env_->NowMicros() - start_micros;
  stats.bytes_written = meta.file_size;
  stats_[level].Add(stats);
  return s;
}

// 将内存中的 memtable 转换为 sstable 文件并写入到磁盘中.
// 当且仅当该方法执行成功后, 切换到一组新的 log-file/memtable 组合并且写一个新的描述符.
// 如果执行失败, 则将错误记录到 bg_error_.
// 调用该方法之前必须获取相应的锁.
void DBImpl::CompactMemTable() {
  mutex_.AssertHeld();
  assert(imm_ != nullptr);

  // 将内存中的 memtable 内容保存为 sstable 文件.
  // 每次落盘就是对当前 level 架构版本的一次编辑.
  VersionEdit edit;
  // 获取当前 dbimpl 对应的最新 version
  Version* base = versions_->current();
  // 将该 version 活跃引用计数加一
  base->Ref();
  // 将 imm_ 对应的 memtable 以 table 文件形式保存到
	// 磁盘并将其对应的元信息(level、filemeta 等)保存到 edit 中
	// (edit 维护着 level 架构每一层文件信息, 新文件落盘要记录下来)
  Status s = WriteLevel0Table(imm_, &edit, base);
  // 将该 version 活跃引用计数减一
  base->Unref();

  // 如果压实 memtable 过程中发生了删除 DB 的情况则报错
  if (s.ok() && shutting_down_.Acquire_Load()) {
    s = Status::IOError("Deleting DB during memtable compaction");
  }

  // 用生成的 Table 替换不可变的 memtable
  if (s.ok()) {
    edit.SetPrevLogNumber(0);
    // memtable 已经转换为 Table 写入磁盘了, 之前的 logs 都不需要了.
    edit.SetLogNumber(logfile_number_);
    s = versions_->LogAndApply(&edit, &mutex_);
  }

  if (s.ok()) {
    // 压实完成, 释放引用
    imm_->Unref();
    imm_ = nullptr;
    has_imm_.Release_Store(nullptr);
    // 删除过期文件
    DeleteObsoleteFiles();
  } else {
    // 压实失败, 记录错误
    RecordBackgroundError(s);
  }
}

/**
 * 将键范围 [*begin,*end] 对应的底层存储压实, 注意范围是左闭右闭. 
 *
 * 压实过程中, 已经被删除或者被覆盖过的数据会被丢弃,
 * 同时会将数据重新安放以减少后续数据访问操作的成本.
 * 这个操作是为那些理解底层实现的用户准备的. 
 *
 * 如果 begin==nullptr, 则从第一个键开始;
 * 如果 end==nullptr 则到最后一个键为止.
 * 所以, 如果像下面这样做则意味着压紧整个数据库:
 * db->CompactRange(nullptr, nullptr);
 * @param begin 起始键
 * @param end 截止键
 */
void DBImpl::CompactRange(const Slice* begin, const Slice* end) {
  int max_level_with_files = 1;
  // 下面这个局部作用域为了控制 l 的生命周期进而简化加锁和释放锁操作
	{
    MutexLock l(&mutex_);
    Version* base = versions_->current();
		// 检查每个 level, 确认其包含的键区间是否与要压实的目标键区间有交集.
		for (int level = 1; level < config::kNumLevels; level++) {
      if (base->OverlapInLevel(level, begin, end)) {
        // 与目标键区间有交集的最高 level
        max_level_with_files = level;
      }
    }
  }
  // 因为当前在写的 memtable 可能与目标键区间有交集, 所以
  // 强制触发一次 memtable 压实
	// (即将当前 memtable 文件转为 sstable 文件并写入磁盘)
  // 并生成新 log 文件和对应的 memtable.
  TEST_CompactMemTable();  // TODO(sanjay): Skip if memtable does not overlap
	// 针对与目标键区间有交集的各个 level 触发一次手动压实
  for (int level = 0; level < max_level_with_files; level++) {
    TEST_CompactRange(level, begin, end);
  }
}

void DBImpl::TEST_CompactRange(int level, const Slice* begin,
                               const Slice* end) {
  assert(level >= 0);
  assert(level + 1 < config::kNumLevels);

  InternalKey begin_storage, end_storage;

  ManualCompaction manual;
  manual.level = level;
  manual.done = false;
  if (begin == nullptr) {
    manual.begin = nullptr;
  } else {
    begin_storage = InternalKey(*begin, kMaxSequenceNumber, kValueTypeForSeek);
    manual.begin = &begin_storage;
  }
  if (end == nullptr) {
    manual.end = nullptr;
  } else {
    end_storage = InternalKey(*end, 0, static_cast<ValueType>(0));
    manual.end = &end_storage;
  }

  MutexLock l(&mutex_);
  while (!manual.done && !shutting_down_.Acquire_Load() && bg_error_.ok()) {
    if (manual_compaction_ == nullptr) {  // Idle
      manual_compaction_ = &manual;
      MaybeScheduleCompaction();
    } else {  // Running either my compaction or another compaction.
      background_work_finished_signal_.Wait();
    }
  }
  if (manual_compaction_ == &manual) {
    // Cancel my manual compaction since we aborted early for some reason.
    manual_compaction_ = nullptr;
  }
}

// 强制触发一次 memtable 压实(即将当前 memtable 文件转为 sstable 文件并写入磁盘)
// 并生成新 log 文件和对应的 memtable
Status DBImpl::TEST_CompactMemTable() {
  // 第二个参数为 nullptr, 目的是等待之前发生的用户写操作结束
  Status s = Write(WriteOptions(), nullptr);
  if (s.ok()) {
    // Wait until the compaction completes
    MutexLock l(&mutex_);
    while (imm_ != nullptr && bg_error_.ok()) {
      background_work_finished_signal_.Wait();
    }
    if (imm_ != nullptr) {
      s = bg_error_;
    }
  }
  return s;
}

// 这里要做的其实就是在后台任务出错(如刷盘失败)时记录错误到 bg_error_,
// 同时唤醒那些等待后台任务完成的线程从而将这个错误传播出去.
void DBImpl::RecordBackgroundError(const Status& s) {
  mutex_.AssertHeld();
  if (bg_error_.ok()) {
    bg_error_ = s;
    background_work_finished_signal_.SignalAll();
  }
}

// 根据具体情况决定是否进行 memtable 到 sstable 文件的转换
void DBImpl::MaybeScheduleCompaction() {
  // 多个线程压实一个 memtable 显然是有问题的
  mutex_.AssertHeld();
  // 如果已经触发过压实任务直接返回
  if (background_compaction_scheduled_) {
    // Already scheduled
  } else if (shutting_down_.Acquire_Load()) {
    // 如果数据库已经关闭了, 也没必要压实了, 因为关闭时做过了
  } else if (!bg_error_.ok()) {
    // 如果后台压实出了错误, 这意味着将不会有新的
		// 数据写操作会成功所以就不会产生新的变更.
  } else if (imm_ == nullptr &&
             manual_compaction_ == nullptr &&
             !versions_->NeedsCompaction()) {
    // 如果无 memtable 需压实并且没有手工触发的
		// 压实任务并且没有任何 level 需要压实,
    // 则啥也不做
  } else {
    // 否则, 需要触发压实任务
    background_compaction_scheduled_ = true;
    env_->Schedule(&DBImpl::BGWork, this);
  }
}

// 该方法仅在 DBImpl::MaybeScheduleCompaction 调用
void DBImpl::BGWork(void* db) {
  reinterpret_cast<DBImpl*>(db)->BackgroundCall();
}

// 该方法仅在 DBImpl::BGWork 调用
void DBImpl::BackgroundCall() {
  MutexLock l(&mutex_);
  // 该标识已经在 DBImpl::MaybeScheduleCompaction 进行设置
  assert(background_compaction_scheduled_);
  if (shutting_down_.Acquire_Load()) {
    // No more background work when shutting down.
  } else if (!bg_error_.ok()) {
    // No more background work after a background error.
  } else {
    // 执行具体的压实任务
    BackgroundCompaction();
  }

  background_compaction_scheduled_ = false;

  // 前一次压实可能在某个 level 产生了过多文件, 所以再调度
  // 一次压实, 如果判断真得需要的话.
  MaybeScheduleCompaction();
  background_work_finished_signal_.SignalAll();
}

// 该方法仅在 DBImpl::BackgroundCall 调用
void DBImpl::BackgroundCompaction() {
  // 压实过程需要全程持有锁, 这也暗示压实不能耗费太多时间.
  mutex_.AssertHeld();

  // 先压实已满的 memtable
  if (imm_ != nullptr) {
    CompactMemTable();
    return;
  }

  // 下面要分手动触发和自动触发来构造 Compaction
  Compaction* c;
  // leveldb 提供了 `DBImpl::CompactRange()` 接口供应用层手工触发压实.
  bool is_manual = (manual_compaction_ != nullptr);
  InternalKey manual_end;
  // 如果手动触发了一个压实
  if (is_manual) {
    ManualCompaction* m = manual_compaction_;
    // 确定压实范围, 即 level 层待压实文件列表, level+1 与之重叠文件列表.
    c = versions_->CompactRange(m->level, m->begin, m->end);
    m->done = (c == nullptr);
    if (c != nullptr) {
      // 获取本次压实范围的最大 key
      manual_end = c->input(0, c->num_input_files(0) - 1)->largest;
    }
    Log(options_.info_log,
        "Manual compaction at level-%d from %s .. %s; will stop at %s\n",
        m->level,
        (m->begin ? m->begin->DebugString().c_str() : "(begin)"),
        (m->end ? m->end->DebugString().c_str() : "(end)"),
        (m->done ? "(end)" : manual_end.DebugString().c_str()));
  } else {
    // 否则根据统计信息确定待压实 level
    c = versions_->PickCompaction();
  }

  Status status;
  if (c == nullptr) {
    // 无需压实
  } else if (!is_manual && c->IsTrivialMove()) {
    // 不做压实, 直接把文件从 level 移动到 level+1
    assert(c->num_input_files(0) == 1);
    FileMetaData* f = c->input(0, 0);
    // 将该文件从 level 层删除
    c->edit()->DeleteFile(c->level(), f->number);
    // 将该文件增加到 level+1
    c->edit()->AddFile(c->level() + 1, f->number, f->file_size,
                       f->smallest, f->largest);
    // 应用本次移动操作
    status = versions_->LogAndApply(c->edit(), &mutex_);
    if (!status.ok()) {
      RecordBackgroundError(status);
    }
    VersionSet::LevelSummaryStorage tmp;
    Log(options_.info_log, "Moved #%lld to level-%d %lld bytes %s: %s\n",
        static_cast<unsigned long long>(f->number),
        c->level() + 1,
        static_cast<unsigned long long>(f->file_size),
        status.ToString().c_str(),
        versions_->LevelSummary(&tmp));
  } else {
    CompactionState* compact = new CompactionState(c);
    // 做压实
    status = DoCompactionWork(compact);
    if (!status.ok()) {
      RecordBackgroundError(status);
    }
    // 清理压实现场
    CleanupCompaction(compact);
    // 释放压实用到的输入文件
    c->ReleaseInputs();
    // 删除过期文件
    DeleteObsoleteFiles();
  }
  delete c;

  if (status.ok()) {
    // Done
  } else if (shutting_down_.Acquire_Load()) {
    // Ignore compaction errors found during shutting down
  } else {
    Log(options_.info_log,
        "Compaction error: %s", status.ToString().c_str());
  }

  if (is_manual) {
    ManualCompaction* m = manual_compaction_;
    if (!status.ok()) {
      m->done = true;
    }
    if (!m->done) {
      // We only compacted part of the requested range.  Update *m
      // to the range that is left to be compacted.
      m->tmp_storage = manual_end;
      m->begin = &m->tmp_storage;
    }
    manual_compaction_ = nullptr;
  }
}

void DBImpl::CleanupCompaction(CompactionState* compact) {
  mutex_.AssertHeld();
  if (compact->builder != nullptr) {
    // May happen if we get a shutdown call in the middle of compaction
    // 如果压实过程中收到 shutdown 调用就会出现这个情况.
    // 正在构造的压实结果数据直接丢掉.
    compact->builder->Abandon();
    delete compact->builder;
  } else {
    assert(compact->outfile == nullptr);
  }
  delete compact->outfile;
  for (size_t i = 0; i < compact->outputs.size(); i++) {
    const CompactionState::Output& out = compact->outputs[i];
    // 已落盘, 解除了被误删除风险, 可以解开保护
    pending_outputs_.erase(out.number);
  }
  delete compact;
}

// 创建一个 sstable 空文件用于写入压实数据, 同时创建一个 TableBuilder,
// 后者将会使用前者.
Status DBImpl::OpenCompactionOutputFile(CompactionState* compact) {
  assert(compact != nullptr);
  assert(compact->builder == nullptr);
  uint64_t file_number;
  {
    mutex_.Lock();
    file_number = versions_->NewFileNumber();
    pending_outputs_.insert(file_number);
    CompactionState::Output out;
    out.number = file_number;
    out.smallest.Clear();
    out.largest.Clear();
    compact->outputs.push_back(out);
    mutex_.Unlock();
  }

  // Make the output file
  std::string fname = TableFileName(dbname_, file_number);
  Status s = env_->NewWritableFile(fname, &compact->outfile);
  if (s.ok()) {
    compact->builder = new TableBuilder(options_, compact->outfile);
  }
  return s;
}

// 用压实内容构造 sstable 文件并落盘, 同时加载一遍确保构造正确.
Status DBImpl::FinishCompactionOutputFile(CompactionState* compact,
                                          Iterator* input) {
  assert(compact != nullptr);
  assert(compact->outfile != nullptr);
  assert(compact->builder != nullptr);

  const uint64_t output_number = compact->current_output()->number;
  assert(output_number != 0);

  // Check for iterator errors
  Status s = input->status();
  const uint64_t current_entries = compact->builder->NumEntries();
  if (s.ok()) {
    // 完成 sstable 构造和落盘
    s = compact->builder->Finish();
  } else {
    compact->builder->Abandon();
  }
  const uint64_t current_bytes = compact->builder->FileSize();
  compact->current_output()->file_size = current_bytes;
  compact->total_bytes += current_bytes;
  delete compact->builder;
  compact->builder = nullptr;

  // Finish and check for file errors
  if (s.ok()) {
    s = compact->outfile->Sync();
  }
  if (s.ok()) {
    s = compact->outfile->Close();
  }
  delete compact->outfile;
  compact->outfile = nullptr;

  if (s.ok() && current_entries > 0) {
    // 确保生成的 sstable 文件可用(加载解析一遍, 成功即可)
    Iterator* iter = table_cache_->NewIterator(ReadOptions(),
                                               output_number,
                                               current_bytes);
    s = iter->status();
    delete iter;
    if (s.ok()) {
      Log(options_.info_log,
          "Generated table #%llu@%d: %lld keys, %lld bytes",
          (unsigned long long) output_number,
          compact->compaction->level(),
          (unsigned long long) current_entries,
          (unsigned long long) current_bytes);
    }
  }
  return s;
}

// 该方法负责将合并生成的文件添加到 level-(L+1) 层.
Status DBImpl::InstallCompactionResults(CompactionState* compact) {
  mutex_.AssertHeld();
  Log(options_.info_log,  "Compacted %d@%d + %d@%d files => %lld bytes",
      compact->compaction->num_input_files(0),
      compact->compaction->level(),
      compact->compaction->num_input_files(1),
      compact->compaction->level() + 1,
      static_cast<long long>(compact->total_bytes));

  // Add compaction outputs
  compact->compaction->AddInputDeletions(compact->compaction->edit());
  const int level = compact->compaction->level();
  for (size_t i = 0; i < compact->outputs.size(); i++) {
    const CompactionState::Output& out = compact->outputs[i];
    compact->compaction->edit()->AddFile(
        level + 1,
        out.number, out.file_size, out.smallest, out.largest);
  }
  return versions_->LogAndApply(compact->compaction->edit(), &mutex_);
}

// 具体压实就做一件事情:
// 遍历待压实文件, 如果某个 key (位于 level-L 或者 level-(L+1))的类型属性取值为"删除",
// 则确认其在 level-(L+2) 或之上是否存在, 若不存在则丢弃之, 否则写入合并后的文件.
Status DBImpl::DoCompactionWork(CompactionState* compact) {
  const uint64_t start_micros = env_->NowMicros();
  // 用于 imm_ 压实耗时统计
  int64_t imm_micros = 0;

  Log(options_.info_log,  "Compacting %d@%d + %d@%d files",
      compact->compaction->num_input_files(0),
      compact->compaction->level(),
      compact->compaction->num_input_files(1),
      compact->compaction->level() + 1);

  assert(versions_->NumLevelFiles(compact->compaction->level()) > 0);
  assert(compact->builder == nullptr);
  assert(compact->outfile == nullptr);
  // 如果快照列表为空, 则将最新的操作序列号作为最小的快照
  if (snapshots_.empty()) {
    compact->smallest_snapshot = versions_->LastSequence();
  } else {
    // 否则从快照列表获取最老的快照对应的序列号作为最小快照.
    // 虽然最老, 但是没有 release 就是要保障可见性的.
    compact->smallest_snapshot = snapshots_.oldest()->sequence_number();
  }

  // 真正做压实工作的之前要释放锁
  mutex_.Unlock();

  // 针对待压实的全部文件创建一个大迭代器
  Iterator* input = versions_->MakeInputIterator(compact->compaction);
  // 迭代器指针拨到开头
  input->SeekToFirst();
  Status status;
  ParsedInternalKey ikey;
  // 下面三个临时变量用来处理多个文件(如果压实涉及了 level-0)
  // 或多个 level 存在同名 user key 的问题, 典型地有如下两种:
  // 1. level-0 文件可能存在重叠, 同名 user key 后出现的更新,
  // 序列号也更大.
  // 2. 低 level  和高 level 之间可能重叠(这个可能其实是肯定,
  // 因为不重叠就不用压实了), 同名 user key 先出现的更新, 序列号也更大.
  std::string current_user_key;
  bool has_current_user_key = false;
  // 如果 user key 出现多次, 下面这个用于记录上次出现时对应的
  // internal key 的序列号.
  SequenceNumber last_sequence_for_key = kMaxSequenceNumber;
  for (; input->Valid() && !shutting_down_.Acquire_Load(); ) {
    // 优先处理已经写满待压实的 memtable
    if (has_imm_.NoBarrier_Load() != nullptr) {
      const uint64_t imm_start = env_->NowMicros();
      mutex_.Lock();
      if (imm_ != nullptr) {
        // immutable memtable 落盘
        CompactMemTable();
        // 如有必要唤醒 MakeRoomForWrite()
        background_work_finished_signal_.SignalAll();
      }
      mutex_.Unlock();
      imm_micros += (env_->NowMicros() - imm_start);
    }

    // 即将被处理的 key
    Slice key = input->key();
    // 当发现截止到 key, level 和 level+2 重叠数据量已经达到上限, 则
    // 开始进行压实; key 也是压实的最右区间.
    //　一进来循环看到这个判断代码可能比较懵, 肯定看不太懂, 其实下面这个判断一般
    // 要经过若干循环才能成立, 先看后面代码再回来看这个判断.
    if (compact->compaction->ShouldStopBefore(key) &&
        compact->builder != nullptr) {
      // 将压实生成的文件落盘
      status = FinishCompactionOutputFile(compact, input);
      if (!status.ok()) {
        break;
      }
    }

    // Handle key/value, add to state, etc.
    bool drop = false;
    // 反序列化 internal key
    if (!ParseInternalKey(key, &ikey)) {
      // Do not hide error keys
      current_user_key.clear();
      has_current_user_key = false;
      last_sequence_for_key = kMaxSequenceNumber;
    } else {
      // 如果这个 user key 之前迭代未出现过, 记下来
      if (!has_current_user_key ||
          user_comparator()->Compare(ikey.user_key,
                                     Slice(current_user_key)) != 0) {
        current_user_key.assign(ikey.user_key.data(), ikey.user_key.size());
        has_current_user_key = true;
        // 标记这个 user key 截止目前轮次迭代对应的序列号;
        // 因为是首次出现所以这里直接置为序列号最大可能取值.
        last_sequence_for_key = kMaxSequenceNumber;
      }

      // 序列号过小, 丢弃这个 key 本次迭代对应的数据; 后面还有这个 key
      // 对应的更新的数据.
      if (last_sequence_for_key <= compact->smallest_snapshot) {
        // Hidden by an newer entry for same user key
        drop = true;    // 规则 (A)
      } else if (ikey.type == kTypeDeletion &&
                 ikey.sequence <= compact->smallest_snapshot &&
                 compact->compaction->IsBaseLevelForKey(ikey.user_key)) {
        // 对于这个 user key:
        // (1) 更高的 levels(指的是祖父 level 及之上)没有对应数据了
        // (2) 更低的 levels 对应的数据的序列号会更大(这个是显然地)
        // (3) 目前正在被压实的各个 levels(即 level 和 level+1) 中序列号
        // 更小的数据在循环的未来几次迭代中会被丢弃(根据上面的规则(A)).
        //
        // 综上, 这个删除标记已经过期了并且可以被丢弃.
        drop = true;
      }

      last_sequence_for_key = ikey.sequence;
    }
#if 0
    Log(options_.info_log,
        "  Compact: %s, seq %d, type: %d %d, drop: %d, is_base: %d, "
        "%d smallest_snapshot: %d",
        ikey.user_key.ToString().c_str(),
        (int)ikey.sequence, ikey.type, kTypeValue, drop,
        compact->compaction->IsBaseLevelForKey(ikey.user_key),
        (int)last_sequence_for_key, (int)compact->smallest_snapshot);
#endif

    // 如果当前数据项不丢弃, 则进行压实落盘
    if (!drop) {
      // 如有必要则创建新的 output file
      if (compact->builder == nullptr) {
        status = OpenCompactionOutputFile(compact);
        if (!status.ok()) {
          break;
        }
      }
      if (compact->builder->NumEntries() == 0) {
        // 如果一个都没写过, input 迭代器又是从小到大遍历,
        // 所以当前 user key 肯定是最小的
        compact->current_output()->smallest.DecodeFrom(key);
      }
      // 否则当前 user key 目前就是最大的
      compact->current_output()->largest.DecodeFrom(key);
      // 将该 user key 对应的数据项写入 sstable.
      // TODO 这里有个地方没看明白:
      // 如果当前 user key 首次出现, 则
      // 上面 last_sequence_for_key 被置为 kMaxSequenceNumber,
      // 且类型不是 kTypeDeletion, 那当前数据项就不会被 drop, 即使
      // 这个数据项实际 sequence number 小于 smallest_snapshot,
      // 有点矛盾了.
      compact->builder->Add(key, input->value());

      // 如果 sstable 文件足够大, 则落盘并关闭
      if (compact->builder->FileSize() >=
          compact->compaction->MaxOutputFileSize()) {
        status = FinishCompactionOutputFile(compact, input);
        if (!status.ok()) {
          break;
        }
      }
    }

    // 处理下个 key
    input->Next();
  }

  if (status.ok() && shutting_down_.Acquire_Load()) {
    status = Status::IOError("Deleting DB during compaction");
  }
  if (status.ok() && compact->builder != nullptr) {
    status = FinishCompactionOutputFile(compact, input);
  }
  if (status.ok()) {
    status = input->status();
  }
  delete input;
  input = nullptr;

  CompactionStats stats;
  stats.micros = env_->NowMicros() - start_micros - imm_micros;
  for (int which = 0; which < 2; which++) {
    for (int i = 0; i < compact->compaction->num_input_files(which); i++) {
      stats.bytes_read += compact->compaction->input(which, i)->file_size;
    }
  }
  for (size_t i = 0; i < compact->outputs.size(); i++) {
    stats.bytes_written += compact->outputs[i].file_size;
  }

  mutex_.Lock();
  stats_[compact->compaction->level() + 1].Add(stats);

  if (status.ok()) {
    status = InstallCompactionResults(compact);
  }
  if (!status.ok()) {
    RecordBackgroundError(status);
  }
  VersionSet::LevelSummaryStorage tmp;
  Log(options_.info_log,
      "compacted to: %s", versions_->LevelSummary(&tmp));
  return status;
}

namespace {

// 对迭代器状态暂存, 用于迭代器析构时取出来进行资源释放相关操作.
struct IterState {
  port::Mutex* const mu;
  Version* const version GUARDED_BY(mu);
  MemTable* const mem GUARDED_BY(mu);
  MemTable* const imm GUARDED_BY(mu);

  IterState(port::Mutex* mutex, MemTable* mem, MemTable* imm, Version* version)
      : mu(mutex), version(version), mem(mem), imm(imm) { }
};

static void CleanupIteratorState(void* arg1, void* arg2) {
  IterState* state = reinterpret_cast<IterState*>(arg1);
  state->mu->Lock();
  state->mem->Unref();
  if (state->imm != nullptr) state->imm->Unref();
  state->version->Unref();
  state->mu->Unlock();
  delete state;
}

}  // anonymous namespace

// 该方法负责按序将当前 memtable 以及全部 sorted string table 文件对应的迭代器构造出来, 
// 然后将其组装成一个逻辑迭代器 MergingIterator. 然后就可以用该迭代器遍历整个数据库了.
Iterator* DBImpl::NewInternalIterator(const ReadOptions& options,
                                      SequenceNumber* latest_snapshot,
                                      uint32_t* seed) {
  mutex_.Lock();
  *latest_snapshot = versions_->LastSequence();

  // Collect together all needed child iterators
  std::vector<Iterator*> list;
  // 把当前 memtable 迭代器加入其中
  list.push_back(mem_->NewIterator());
  mem_->Ref();
  if (imm_ != nullptr) {
    // 把待写盘 memtable 迭代器追加到列表中
    list.push_back(imm_->NewIterator());
    imm_->Ref();
  }
  // 将当前 version 维护的 level 架构中每个 sorted string table 文件对应的迭代器追加到列表中
  versions_->current()->AddIterators(options, &list);
  // 将全部迭代器上面加一层抽象构成一个逻辑迭代器 MergingIterator
  Iterator* internal_iter =
      NewMergingIterator(&internal_comparator_, &list[0], list.size());
  versions_->current()->Ref();

  // 由于当前 memtable/in-merging-memtable/各个 level 的 sorted string tables 的迭代器都被用了,
  // 所以它们各自引用计数要累加. 等到用完还要递减以释放不需要的内存.
  IterState* cleanup = new IterState(&mutex_, mem_, imm_, versions_->current());
  // 注册该逻辑迭代器对应的清理函数, 该迭代器析构时就会执行清理函数释释放资源.
  internal_iter->RegisterCleanup(CleanupIteratorState, cleanup, nullptr);

  *seed = ++seed_;
  mutex_.Unlock();
  return internal_iter;
}

Iterator* DBImpl::TEST_NewInternalIterator() {
  SequenceNumber ignored;
  uint32_t ignored_seed;
  return NewInternalIterator(ReadOptions(), &ignored, &ignored_seed);
}

int64_t DBImpl::TEST_MaxNextLevelOverlappingBytes() {
  MutexLock l(&mutex_);
  return versions_->MaxNextLevelOverlappingBytes();
}

// 先查询当前在用的 memtable, 如果没有则查询正在转换为 sorted string table 的 memtable 中寻找, 
// 如果没有则我们在磁盘上采用从底向上 level-by-level 的寻找目标 key. 
// 由于 level 越低数据越新, 因此, 当我们在一个较低的 level 找到数据的时候, 不用在更高的 levels 找了.
// 由于 level-0 文件之间可能存在重叠, 而且针对同一个 key, 后产生的文件数据更新所以先将包含 key 的文件找出来
// 按照文件号从大到小(对应文件从新到老)排序查找 key; 针对 level-1 及其以上 level, 由于每个 level 内
// 文件之间不存在重叠, 于是在每个 level 中直接采用二分查找定位 key.
Status DBImpl::Get(const ReadOptions& options,
                   const Slice& key,
                   std::string* value) {
  Status s;
  MutexLock l(&mutex_);
  SequenceNumber snapshot;
  if (options.snapshot != nullptr) {
    // 如果查询某个快照版本对应的 value(比如针对同样的 key,
    // 比如 hello, 多次 Put 写入, 每次 Put 时对应序列号是不同的)
    snapshot =
        static_cast<const SnapshotImpl*>(options.snapshot)->sequence_number();
  } else {
    // 否则就用目前数据库最大序列号作为查询时组装 internal_key
    // 用的序列号, 保证查到的是最新的那次更新.
    // (比较时候也会用到序列号, 序列号越大越新)
    snapshot = versions_->LastSequence();
  }

  MemTable* mem = mem_;
  MemTable* imm = imm_;
  // VersionSet 的当前 Version 保存了目前最新
  // 的 level 架构信息(每个 level 各自包含了哪些文件覆盖了哪些键区间)
  Version* current = versions_->current();
  mem->Ref();
  if (imm != nullptr) imm->Ref();
  current->Ref();

  bool have_stat_update = false;
  Version::GetStats stats;

  // 当读取文件和 memtables 的时候, 释放锁
  {
    mutex_.Unlock();
    // 根据 user_key 和快照对应的序列号构造一个 internal_key
    LookupKey lkey(key, snapshot);
    // 先查询内存中与当前 log 文件对应的 memtable
    if (mem->Get(lkey, value, &s)) {
      // Done
      // 查不到再去待压实的 memtable 去查询
    } else if (imm != nullptr && imm->Get(lkey, value, &s)) {
      // Done
    } else {
      // 查不到再逐 level 去 sstable 文件查找
      s = current->Get(options, lkey, value, &stats);
      have_stat_update = true;
    }
    mutex_.Lock();
  }

  // 每次查询完都要检查下是否有文件查询次数已经达到最大需要进行压实了.
  if (have_stat_update && current->UpdateStats(stats)) {
    MaybeScheduleCompaction();
  }
  mem->Unref();
  if (imm != nullptr) imm->Unref();
  current->Unref();
  return s;
}

// 将内存 memtable 和磁盘 sorted string table 文件全部数据结构
// 串起来构造一个大一统迭代器, 可以遍历整个数据库.
// 具体由 leveldb::DBImpl::NewInternalIterator 负责完成.
Iterator* DBImpl::NewIterator(const ReadOptions& options) {
  SequenceNumber latest_snapshot;
  uint32_t seed;
  // 将内存和磁盘全部数据结构串起来构造一个大一统迭代器, 可以遍历整个数据库
  Iterator* iter = NewInternalIterator(options, &latest_snapshot, &seed);
  return NewDBIterator(
      this, user_comparator(), iter,
      (options.snapshot != nullptr
       ? static_cast<const SnapshotImpl*>(options.snapshot)->sequence_number()
       : latest_snapshot),
      seed);
}

void DBImpl::RecordReadSample(Slice key) {
  MutexLock l(&mutex_);
  if (versions_->current()->RecordReadSample(key)) {
    MaybeScheduleCompaction();
  }
}

// 用数据库当前最新的更新操作对应的序列号创建一个快照.
// 快照最核心的就是那个操作序列号, 因为查询时会把 user_key 
// 和操作序列号一起构成一个 internal_key, 针对 user_key 相等的情况
// 比如针对 hello 这个 user_key Put 多次, 则每次序列号就不一样,
// 于是根据特定序列号可以查询到特定的那次 Put 写入的 value 值.
const Snapshot* DBImpl::GetSnapshot() {
  MutexLock l(&mutex_);
  return snapshots_.New(versions_->LastSequence());
}

void DBImpl::ReleaseSnapshot(const Snapshot* snapshot) {
  MutexLock l(&mutex_);
  snapshots_.Delete(static_cast<const SnapshotImpl*>(snapshot));
}

// Convenience methods
// 直接调用了基类的 Put 方法
Status DBImpl::Put(const WriteOptions& o, const Slice& key, const Slice& val) {
  return DB::Put(o, key, val);
}

Status DBImpl::Delete(const WriteOptions& options, const Slice& key) {
  return DB::Delete(options, key);
}

Status DBImpl::Write(const WriteOptions& options, WriteBatch* my_batch) {
  // 每次批量写会被封装为一个 Writer
  Writer w(&mutex_); // 注意这里并不执行上锁操作.
  w.batch = my_batch;
  w.sync = options.sync;
  w.done = false;

  // 上锁保护下面的 writers_ 队列操作, 注意这里用的锁
	// 和上面创建 Writer 用的是一把锁.
  MutexLock l(&mutex_);
  // 新构造的 writer 追加到 writers_ 队尾.
  // 注意, 指针是内置类型, 当作为右值类型时支持 move 语义,
  // 所以这里调用的就是 deque 的右值引用版本的 push_back
  writers_.push_back(&w);
  // 当前 writer 工作没完成并且不是队首元素, 换句话说, 其它线程先于当前线程
	// 调用了 Write() 方法在执行批量更新, 则挂起当前线程, 这里的设计有点像通过
	// 自旋实现同步.
  while (!w.done && &w != writers_.front()) {
    // 当前 writer 所在线程进入等待状态,
    // 这会导致上面 MutexLock 上的锁被释放掉,
    // 于是其它写线程得以有机会调用当前 Write() 方法.
    w.cv.Wait();
  }
  // 还没写怎么就 done 了呢? 原因是 Write() 方法支持 writer 合并,
	// 如果当前 writer 如果被排在前面的 writer 给合并写入了, 那么它的 done 就会被标记.
	// 这里的检查和上面 while 中的检查构成了一个 double check, 因为 while 和 if
	// 之间有个时间窗口.
  if (w.done) {
    return w.status;
  }

  // 确认是否为本次写操作分配新的 log 文件, 如果需要则分配.
  // 下面方法可能会临时释放锁并进入等待状态, 这会导致更多 writers
  // 入队, 方便后面写的时候做写入合并写入一大批数据.
	// 如果用户调用 Write() 传入了一个空 batch, 表达的意思是强制分配
	// log 文件.
  Status status = MakeRoomForWrite(my_batch == nullptr);
  uint64_t last_sequence = versions_->LastSequence();
  Writer* last_writer = &w;
	// nullptr batch 用于触发压实
  if (status.ok() && my_batch != nullptr) {
    // 队首 writer 负责将队列前面若干 writers 的 batch 合并为一个 group.
		//
    // 注意, writers 被合并但是不出队, 写 log 期间队首 writer 始终不变,
    // 这个能确保后续写 log 写 memtable 期间, 其它进入 Write() 方法
    // 的线程对应的 Writer 因为不可能是队首元素最深只能进入到 Write() 方法入口
    // 的 while() 循环并陷入等待状态, 而不会出现多个 writer 并发写的情况,
    // 从而确保了 log/memtable 相关操作的线程安全.
		//
    // 执行这些操作需要持有锁, 确保不会同时发生多个针对相同数据的  合并操作.
    WriteBatch* updates = BuildBatchGroup(&last_writer);
    // 设置本批次第一个写操作的序列号, 然后根据操作个数更新全局写操作的序列号,
    // 执行这些操作需要持有锁, 确保 sequence 被互斥访问.
    WriteBatchInternal::SetSequence(updates, last_sequence + 1);
    last_sequence += WriteBatchInternal::Count(updates);

		// 将 updates 追加到 log 文件同时写入 memtable, 期间可以释放锁, 因为 &w 负责
		// 当前日志写入, 可以避免并发 loggers 和并发写操作到 mem_.
		// 只要 &w 不出队, 后面的 writers 就没机会出循环(这个循环相当于一个通过自旋做同步的设施),
		// 也就到不了这里和它竞争写入 log 文件或 memtable, 所以没有线程安全问题.
    {
      // 这里临时释放可以让其它 writer 趁机在 Write 方法入口处进入写入队列.
      mutex_.Unlock();
      // 将合并后的 batch 作为 record 追加到 log 文件中
      status = log_->AddRecord(WriteBatchInternal::Contents(updates));
      bool sync_error = false;
      if (status.ok() && options.sync) {
				// 如果调用方要求同步写入, 这里要进行一次刷盘
        status = logfile_->Sync();
        if (!status.ok()) {
          sync_error = true;
        }
      }
      if (status.ok()) {
        // 如果追加 log 文件成功,则将被追加的数据插入到内存中的 memtable 中
        status = WriteBatchInternal::InsertInto(updates, mem_);
      }
			// 写完了, 后面要操作 writers_ 队列了, 而这个队列在上面拦着一堆后来的 writer,
			// 要修改状态了, 所以要获取锁.
      mutex_.Lock();
      if (sync_error) {
				// 同步出错, log 文件的状态是不确定的: 我们刚刚追加过 log record,
				// 在后续重新打开 DB 时候, 那些 record 可能会出现也可能不会出现.
				// 所以我们强制 DB 进入一个状态, 在这个状态下后续写操作将会全部失败,
				// 这个模式会通过后续 writer 执行上面
				// Status status = MakeRoomForWrite(my_batch == nullptr);
				// 时通过返回的状态感知到.
        RecordBackgroundError(status);
      }
    }
    if (updates == tmp_batch_) tmp_batch_->Clear();

    versions_->SetLastSequence(last_sequence);
  }

  // 参与上面 batch group 写入 log 文件的 writer 都取出来并设置为写入完成
  while (true) {
    // [&w, last_writer] 的 batch 被合并写入 log 了, 所以将其出队.
    Writer* ready = writers_.front();
    writers_.pop_front();
    // &w 并没有 wait, 它是本次负责合并写入的 writer,
		// 所以它 &w 的 status 和 done 可以不用改, 反正也用不到.
    if (ready != &w) {
			// 传递合并写执行结果给 group 中各个 writer
      ready->status = status;
      ready->done = true;
      // 唤醒当前方法入口的 w.cv.Wait(), 通过此处被唤醒的
			// writers 都是被合并到队首 writer 统一写入 log 文件的.
      // 它们被唤醒后, 只需检查下 done 状态就可以返回了.
      ready->cv.Signal();
    }
    // last_writer 指向被合并处理的最后一个 writer
    if (ready == last_writer) break;
  }

  // 如果当前 writers_ 队列不为空, 唤醒当前的队首节点.
  if (!writers_.empty()) {
    // 叫醒新的待写入 writer
    writers_.front()->cv.Signal();
  }

  return status;
}

// 当外部调用 db 写数据时, 该方法将队列前若干 writer 的 batch 合并到一起.
// 返回合并后的结果 batch, 参数 last_writer 也作为输出参数, 包含了被合并的最后一个 writer 的指针.
// 要求: Writer 队列不为空且对手元素的 batch 不为 nullptr.
WriteBatch* DBImpl::BuildBatchGroup(Writer** last_writer) {
  mutex_.AssertHeld();
  assert(!writers_.empty());
  // 取出队首 writer
  Writer* first = writers_.front();
  // 取出队首 writer 的待写数据集
  WriteBatch* result = first->batch;
  assert(result != nullptr);

  // 计算队首 writer 数据集大小
  size_t size = WriteBatchInternal::ByteSize(first->batch);

	// 虽然支持合并, 但是有两个限制条件:
	// 1. 不合并同步写入操作(设置了 writer.sync), 发现同步写操作立马停止后续合并操作并返回已合并内容.
	// 2. 为了避免小数据量写入操作被延迟太久, 针对合并上限做了限制, 最大 1MB.
  size_t max_size = 1 << 20;
	// 如果队首 writer 要写内容大小不超过 128KB
  if (size <= (128<<10)) {
		// 则 max_size 改为不超过 256KB
    max_size = size + (128<<10);
  }

  *last_writer = first;
  std::deque<Writer*>::iterator iter = writers_.begin();
  ++iter;  // Advance past "first"
  // iter 从 first 之后 writer 开始遍历
  for (; iter != writers_.end(); ++iter) {
    Writer* w = *iter;
		// 同步写操作不做合并
    if (w->sync && !first->sync) {
      // Do not include a sync write into a batch handled by a non-sync write.
      break;
    }

    if (w->batch != nullptr) {
      size += WriteBatchInternal::ByteSize(w->batch);
      if (size > max_size) {
        // 避免 batch group 过大
        break;
      }

      // Append to *result
      if (result == first->batch) {
        // 不篡改 first writer 的 batch, 而是把若干 batch 合并到临时的 tmp_batch_ 中
        result = tmp_batch_;
        assert(WriteBatchInternal::Count(result) == 0);
        WriteBatchInternal::Append(result, first->batch);
      }
      WriteBatchInternal::Append(result, w->batch);
    }
    // last_writer 指向被合并的最后一个 writer
    *last_writer = w;
  }
  return result;
}

// 当外部调用 db 写数据时, 该方法被调用负责根据实际情况创建新的 log 文件
// 以及对应的 memtable, 同时将当前 memtable 赋值给 imm_ 等待被写盘.
// 
// 调用该方法前提: 
// - mutex_ 被当前线程持有
// - 当前线程当前在 writer_ 队列的队首
Status DBImpl::MakeRoomForWrite(bool force) {
  mutex_.AssertHeld(); // 断言当前线程持有 mutex_
  assert(!writers_.empty()); // 断言 writer 队列不为空
  bool allow_delay = !force;
  Status s;
  // 循环目的:
	// 1. 为了避免下面的 background_work_finished_signal_.Wait() 过程中发生 Spurious wakeup,
  // 具体见 https://en.wikipedia.org/wiki/Spurious_wakeup
	// 2. 如果开始创建新 log 条件不成熟, 等待一会再检查可能就成熟了.
	// 满足创建条件后, 再次进入循环第三个条件可以确保循环退出.
  while (true) {
    if (!bg_error_.ok()) {
      // 后台压实任务出错, 先不能分配新的, 防止丢数据(此时是偏执模式, 对数据安全性要求高).
      s = bg_error_;
      break;
    } else if (allow_delay &&
      versions_->NumLevelFiles(0) >= config::kL0_SlowdownWritesTrigger) {
			// 如果用户允许延迟写入且 L0 文件数接近达到硬上限,
			// 为了避免在达到上限后将单个写入延迟数秒, 我们
			// 在这将单个写操作延迟 1ms 以减小延迟的方差.
			// 而且, 这里的延迟操作可以在压实和写操作共用一个 core 的
			// 时候将 CPU 时间拱手让给压实线程.
      mutex_.Unlock();
      env_->SleepForMicroseconds(1000);
			// 我们不重复推迟同一个写操作
      allow_delay = false;
      mutex_.Lock();
    } else if (!force &&
               (mem_->ApproximateMemoryUsage() <= options_.write_buffer_size)) {
      // 如果非强制分配且当前 memtable 还没写满, 则不分配新的 log 文件
      break;
    } else if (imm_ != nullptr) {
			// 当前 memtable 已经写满, 但是前一个 memtable 还在压实过程中, 等待压实完成.
      Log(options_.info_log, "Current memtable full; waiting...\n");
      background_work_finished_signal_.Wait();
    } else if (versions_->NumLevelFiles(0) >= config::kL0_StopWritesTrigger) {
      // level-0 文件个数太多了, 等待压实完成.
      Log(options_.info_log, "Too many L0 files; waiting...\n");
      background_work_finished_signal_.Wait();
    } else {
			// 尝试切换到新的 memtable, 同时触发一个针对老 memtable 的压实.
      assert(versions_->PrevLogNumber() == 0);
      uint64_t new_log_number = versions_->NewFileNumber();
      WritableFile* lfile = nullptr;
      // 分配新文件号, 创建新的 log 文件
      s = env_->NewWritableFile(LogFileName(dbname_, new_log_number), &lfile);
      if (!s.ok()) {
        // Avoid chewing through file number space in a tight loop.
        versions_->ReuseFileNumber(new_log_number);
        break;
      }
      delete log_;
      delete logfile_;
      logfile_ = lfile;
      // 更新当前在写文件的文件号
      logfile_number_ = new_log_number;
      // 生成一个新的 log writer 负责写文件
      log_ = new log::Writer(lfile);
      // 将满了的 mem_ 赋值给 imm_ 等待被落盘
      imm_ = mem_;
      // 将 imm_ 存储到 has_imm_ 中
      has_imm_.Release_Store(imm_);
      // 创建一个与新 log 文件对应的 memtable
      mem_ = new MemTable(internal_comparator_);
      mem_->Ref();
			// 创建新文件后将强制状态取消
      force = false; // Do not force another compaction if have room
      // 如果需要触发压实操作, 则进行压实. 由于上面设置了 imm_, 只要之前 mem_ 不为空则必触发压实.
      MaybeScheduleCompaction();
    }
  }
  return s;
}

/**
 * DB 实现可以通过该方法导出自身状态相关的信息. 如果提供的属性可以被 DB 实现理解, 那么第二个参数将会
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
 * @param property 要查询的状态名称
 * @param value 保存属性名称对应的属性值
 * @return
 */
bool DBImpl::GetProperty(const Slice& property, std::string* value) {
  value->clear();

  MutexLock l(&mutex_);
  Slice in = property;
  Slice prefix("leveldb.");
  if (!in.starts_with(prefix)) return false;
  in.remove_prefix(prefix.size());

  if (in.starts_with("num-files-at-level")) {
    in.remove_prefix(strlen("num-files-at-level"));
    uint64_t level;
    bool ok = ConsumeDecimalNumber(&in, &level) && in.empty();
    if (!ok || level >= config::kNumLevels) {
      return false;
    } else {
      char buf[100];
      snprintf(buf, sizeof(buf), "%d",
               versions_->NumLevelFiles(static_cast<int>(level)));
      *value = buf;
      return true;
    }
  } else if (in == "stats") {
    char buf[200];
    snprintf(buf, sizeof(buf),
             "                               Compactions\n"
             "Level  Files Size(MB) Time(sec) Read(MB) Write(MB)\n"
             "--------------------------------------------------\n"
             );
    value->append(buf);
    for (int level = 0; level < config::kNumLevels; level++) {
      int files = versions_->NumLevelFiles(level);
      if (stats_[level].micros > 0 || files > 0) {
        snprintf(
            buf, sizeof(buf),
            "%3d %8d %8.0f %9.0f %8.0f %9.0f\n",
            level,
            files,
            versions_->NumLevelBytes(level) / 1048576.0,
            stats_[level].micros / 1e6,
            stats_[level].bytes_read / 1048576.0,
            stats_[level].bytes_written / 1048576.0);
        value->append(buf);
      }
    }
    return true;
  } else if (in == "sstables") {
    *value = versions_->current()->DebugString();
    return true;
  } else if (in == "approximate-memory-usage") {
    size_t total_usage = options_.block_cache->TotalCharge();
    if (mem_) {
      total_usage += mem_->ApproximateMemoryUsage();
    }
    if (imm_) {
      total_usage += imm_->ApproximateMemoryUsage();
    }
    char buf[50];
    snprintf(buf, sizeof(buf), "%llu",
             static_cast<unsigned long long>(total_usage));
    value->append(buf);
    return true;
  }

  return false;
}

// 返回 range 包含的键区间在磁盘上占用的空间大小.
// range 和 sizes 元素个数均为 n.
void DBImpl::GetApproximateSizes(
    const Range* range, int n,
    uint64_t* sizes) {
  // TODO(opt): better implementation
  Version* v;
  {
    MutexLock l(&mutex_);
    // 获取当前 level 架构信息对应的 Version
    versions_->current()->Ref();
    v = versions_->current();
  }

  // 遍历 range
  for (int i = 0; i < n; i++) {
    // Convert user_key into a corresponding internal key.
    InternalKey k1(range[i].start, kMaxSequenceNumber, kValueTypeForSeek);
    InternalKey k2(range[i].limit, kMaxSequenceNumber, kValueTypeForSeek);
    // 确定 k1 在当前数据库中的大致字节偏移量
    uint64_t start = versions_->ApproximateOffsetOf(v, k1);
    // 确定 k2 在当前数据库中的大致字节偏移量
    uint64_t limit = versions_->ApproximateOffsetOf(v, k2);
    // k2 - k1 即为该键区间占用的空间大致大小
    sizes[i] = (limit >= start ? limit - start : 0);
  }

  {
    MutexLock l(&mutex_);
    v->Unref();
  }
}

// Default implementations of convenience methods that subclasses of DB
// can call if they wish
//
// DB 的默认 Put 实现, 派生类可以直接使用. 
// 但是 DB 未实现 Write 方法, 所以派生类需要自己实现, 然后调用 Put 时调用自己实现的 Write 方法.
Status DB::Put(const WriteOptions& opt, const Slice& key, const Slice& value) {
  WriteBatch batch;
  batch.Put(key, value);
  return Write(opt, &batch);
}

// DB 的默认 Delete 实现, 派生类可以直接使用. 
// 但是 DB 未实现 Write 方法, 所以派生类需要自己实现, 然后调用 Put 时调用自己实现的 Write 方法.
Status DB::Delete(const WriteOptions& opt, const Slice& key) {
  WriteBatch batch;
  batch.Delete(key);
  return Write(opt, &batch);
}

DB::~DB() { }

Status DB::Open(const Options& options, const std::string& dbname,
                DB** dbptr) {
  *dbptr = nullptr;

  DBImpl* impl = new DBImpl(options, dbname);
  impl->mutex_.Lock();
  // 新建一个 edit
  VersionEdit edit;
  // 下面的 Recover 自动处理这两种错误: create_if_missing, error_if_exists.
  // save_manifest 用于标识是否需要在 Recover 后写 manifest 文件.
  bool save_manifest = false;
  // 读取 current 文件, manifest 文件, sorted string table 文件和 log 文件恢复数据库.
  // 如果要打开的数据库不存在, Recover 负责进行创建.
  Status s = impl->Recover(&edit, &save_manifest);
  // 如果打开成功且当前 memtable 为空则创建之及其对应的 log 文件
  if (s.ok() && impl->mem_ == nullptr) {
    // 创建 log 文件及其对应的 memtable.
    // log 文件名就是一个数字, 由 VersionSet 负责维护.
    uint64_t new_log_number = impl->versions_->NewFileNumber();
    WritableFile* lfile;
    s = options.env->NewWritableFile(LogFileName(dbname, new_log_number),
                                     &lfile);
    // log 文件创建成功, 则将其 log 名字记录到 edit, 并创建对应的 memtable
    if (s.ok()) {
      edit.SetLogNumber(new_log_number);
      impl->logfile_ = lfile;
      impl->logfile_number_ = new_log_number;
      impl->log_ = new log::Writer(lfile);
      impl->mem_ = new MemTable(impl->internal_comparator_);
      impl->mem_->Ref();
    }
  }
  // 如果数据库打开成功, 且需要记录变更到 manifest 文件(上面 Recover 导致),
	// 则将 edit 序列化到 manifest
  if (s.ok() && save_manifest) {
		// 做完 recover 后, 之前的 log 文件都没用了.
    edit.SetPrevLogNumber(0);
    edit.SetLogNumber(impl->logfile_number_);
    s = impl->versions_->LogAndApply(&edit, &impl->mutex_);
  }
  // 数据库打开成功, 启动过期文件删除和周期性压实任务
  if (s.ok()) {
    impl->DeleteObsoleteFiles();
    impl->MaybeScheduleCompaction();
  }
  impl->mutex_.Unlock();
  if (s.ok()) {
    assert(impl->mem_ != nullptr);
    *dbptr = impl;
  } else {
    delete impl;
  }
  return s;
}

Snapshot::~Snapshot() {
}

Status DestroyDB(const std::string& dbname, const Options& options) {
  Env* env = options.env;
  std::vector<std::string> filenames;
  Status result = env->GetChildren(dbname, &filenames);
  if (!result.ok()) {
    // Ignore error in case directory does not exist
    return Status::OK();
  }

  FileLock* lock;
  const std::string lockname = LockFileName(dbname);
  result = env->LockFile(lockname, &lock);
  if (result.ok()) {
    uint64_t number;
    FileType type;
    for (size_t i = 0; i < filenames.size(); i++) {
      if (ParseFileName(filenames[i], &number, &type) &&
          type != kDBLockFile) {  // Lock file will be deleted at end
        Status del = env->DeleteFile(dbname + "/" + filenames[i]);
        if (result.ok() && !del.ok()) {
          result = del;
        }
      }
    }
    env->UnlockFile(lock);  // Ignore error since state is already gone
    env->DeleteFile(lockname);
    env->DeleteDir(dbname);  // Ignore error in case dir contains other files
  }
  return result;
}

}  // namespace leveldb

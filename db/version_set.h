// Copyright (c) 2011 The LevelDB Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file. See the AUTHORS file for names of contributors.
//
// The representation of a DBImpl consists of a set of Versions.  The
// newest version is called "current".  Older versions may be kept
// around to provide a consistent view to live iterators.
//
// Each Version keeps track of a set of Table files per level.  The
// entire set of versions is maintained in a VersionSet.
//
// Version,VersionSet are thread-compatible, but require external
// synchronization on all accesses.
//
// 一个 DBImpl 由一组 Versions 构成, 最新的 version 叫做 "current".
// 较老的 versions 可能也会被保留以为存活的迭代器提供一致性视图.
// 每个 Version 会追踪每个 level 的 sstable 文件.
// VersionSet 保存全部 Versions.
// Version, VersionSet 都是线程兼容的(非线程安全但是可以做到),
// 但是在并发访问的时候需要外部的同步设施.
#ifndef STORAGE_LEVELDB_DB_VERSION_SET_H_
#define STORAGE_LEVELDB_DB_VERSION_SET_H_

#include <map>
#include <set>
#include <vector>
#include "db/dbformat.h"
#include "db/version_edit.h"
#include "port/port.h"
#include "port/thread_annotations.h"

namespace leveldb {

namespace log { class Writer; }

class Compaction;
class Iterator;
class MemTable;
class TableBuilder;
class TableCache;
class Version;
class VersionSet;
class WritableFile;

// 返回满足条件 files[i]->largest >= key 的最小索引 i; 如果不存在则返回 files 列表长度. 
// 通过二分法在 files 里查找. 
// 要求: files 必须包含一组有序的且无重叠的文件(除了 level-0, 其它 levels 的文件列表均满足该条件). 
int FindFile(const InternalKeyComparator& icmp,
             const std::vector<FileMetaData*>& files,
             const Slice& key);

// Returns true iff some file in "files" overlaps the user key range
// [*smallest,*largest].
// smallest==nullptr represents a key smaller than all keys in the DB.
// largest==nullptr represents a key largest than all keys in the DB.
// REQUIRES: If disjoint_sorted_files, files[] contains disjoint ranges
//           in sorted order.
// 检查 files 中是否存在文件与区间 [*smallest,*largest] 重叠. 
// 当且仅当 files 中某个文件与参数 [*smallest,*largest] 指定的 user key 区间重叠返回 true. 
// smallest==nullptr 表示小于 DB 中全部 keys 的 key. 
// largest==nullptr 表示大于 DB 中全部 keys 的 key. 
// 要求: 如果 disjoint_sorted_files 为 true, files[] 包含的区间不相交且有序. 
bool SomeFileOverlapsRange(const InternalKeyComparator& icmp,
                           bool disjoint_sorted_files,
                           const std::vector<FileMetaData*>& files,
                           const Slice* smallest_user_key,
                           const Slice* largest_user_key);

// 一个 DBImpl 由一组 Versions 构成.
// 每个 Version 都保存着 db 每个 level 的文件元数据链表, 
// 核心数据成员为 std::vector<FileMetaData*> files_[config::kNumLevels]. 
// (一个 Version 对应一个 DBImpl 快照. )
class Version {
 public:
  // 将当前 version 维护的 level 架构中每一个 sstable 文件对应的迭代器
  // 追加到 iters 向量里, 这些迭代器加上 memtable 的迭代器, 就能
  // 遍历整个数据库的内容了.
  // 前提: 当前 Version 对象事先已经通过 VersionSet::SaveTo 方法被保存过了.
  void AddIterators(const ReadOptions&, std::vector<Iterator*>* iters);

  // 该结构体与下面的 Get() 方法配套使用.
  // 用于在调用 Get 时保存待压实的文件及其 level 信息.
  // 具体见 Get() 方法.
  // 前提: 未持有锁(具体见 DBImpl::Get()）
  struct GetStats {
    FileMetaData* seek_file;
    int seek_file_level;
  };

  // DBImpl::Get() 自己只在两个内存结构 memtable 中查询, 如果没找到就要扫盘, 扫盘就是 Version::Get() 负责的.
  // 在 DBImpl::Get() 中被调用, 用于逐层扫描各个 level 文件定位目标 key.
  // 先查询当前在用的 memtable, 如果没有则查询正在转换为 sstable 的 memtable 中寻找,
  // 如果没有则我们在磁盘上采用从底向上 level-by-level 的寻找目标 key.
  //
  // 由于 level 越低数据越新, 因此, 当我们在一个较低的 level 找到数据的时候, 不用在更高的 levels 找了.
  // 由于 level-0 文件之间可能存在重叠, 而且针对同一个 key, 后产生的文件数据更新所以先将包含 key 的文件找出来
  // 按照文件号从大到小(对应文件从新到老)排序查找 key; 针对 level-1 及其以上 level, 由于每个 level 内
  // 文件之间不存在重叠, 于是在每个 level 中直接采用二分查找定位 key.
  // 
  // 当查询一个 key 时, 如果找到了对应的 value, 则将 value 保存到 *val 指向的地址并且返回 OK; 
  // 否则, 返回一个  non-OK.
  //
  // 前提:  调用该方法之前必须未持有锁(调用前 db 会释放持有的锁, 具体见 DBImpl::Get). 
  Status Get(const ReadOptions&, const LookupKey& key, std::string* val,
             GetStats* stats);

  // 如果上次调用 Get 查询感知到疑似需要进行压实, 则此处进一步检查确定是否触发压实.
  // 检查条件是 stats 的 allowed_seeks 是否降为 0.
  // 如果需要触发一个压实, 则返回 true; 否则返回 false. 
  // 前提: 调用该方法前必须已经持有锁. 
  bool UpdateStats(const GetStats& stats);

  // Record a sample of bytes read at the specified internal key.
  // Samples are taken approximately once every config::kReadBytesPeriod
  // bytes.  Returns true if a new compaction may need to be triggered.
  // REQUIRES: lock is held
  //
  // 看了该方法实现发现上面的英文注释有点莫名其妙. 
  // 该方法就是从 level-0 开始检查有多少个文件的范围与 key 重叠, 
  // 如果有多于两个就会设置触发压实的标记, 并且在检查过程中会记录第一个与 key 重叠的文件及其 level. 
  // 如果需要触发一次新的压实, 则返回 true, 否则返回 false. 
  // 前提: 调用该方法前必须已经持有锁. 
  bool RecordReadSample(Slice key);

  // Version 的引用计数管理(可以做到迭代器在则 Versions 也在)
  void Ref();
  void Unref();

  void GetOverlappingInputs(
      int level,
      const InternalKey* begin,         // nullptr means before all keys 为空表示在全部 keys 之前
      const InternalKey* end,           // nullptr means after all keys 为空表示在全部 keys 之后
      std::vector<FileMetaData*>* inputs);

  // Returns true iff some file in the specified level overlaps
  // some part of [*smallest_user_key,*largest_user_key].
  // smallest_user_key==nullptr represents a key smaller than all the DB's keys.
  // largest_user_key==nullptr represents a key largest than all the DB's keys.
  // 检查指定的 level 是否与 [*smallest, *largest] 区间相交. 
  // 当且仅当指定 level 的某个文件与 [*smallest, *largest] 指定的 user key 区间重叠返回 true; 
  // 意思是 [*smallest, *largest] 与指定的 level 有重叠. 
  // smallest==nullptr 表示小于 DB 中全部 keys 的 key. 
  // largest==nullptr 表示大于 DB 中全部 keys 的 key. 
  bool OverlapInLevel(int level,
                      const Slice* smallest_user_key,
                      const Slice* largest_user_key);

  // Return the level at which we should place a new memtable compaction
  // result that covers the range [smallest_user_key,largest_user_key].
  //
  // 该方法负责为一个 memtable 在当前 level 架构(保存在当前 version 中)找一个落脚的 level. 
  // 如果该 memtable 与 level-0 文件有重叠, 则放到 level-0; 否则, 它的判断条件就从从 level-1 开始寻找, 
  // 主要是借用了压实磁盘 level 某个文件
  // 时生成新文件的判断条件之二. "在合并 level-L 和 level-(L+1) 文件时生成新文件条件之一是达到了 2MB, 条件二是
  // 即如果 level-L 和 level-(L+2) 重叠文件超过 10 个".
  // 
  // 一个 memtable 对应一个 [smallest_user_key,largest_user_key] 区间, 
  // 我们将该 memtable 构造成一个 Table 文件后, 需要为该文件寻找一个落脚的 level. 
  // 该方法所作的即是依据与区间 [smallest_user_key,largest_user_key] 的重叠情况获取可以存储对应 Table 文件的 level. 
  // 具体选取过程与压实策略有关.
  int PickLevelForMemTableOutput(const Slice& smallest_user_key,
                                 const Slice& largest_user_key);

  // 返回指定 level 对应的文件个数
  int NumFiles(int level) const { return files_[level].size(); }

  // Return a human readable string that describes this version's contents.
  // 返回一个对人类友好的描述该 version 内容的字符串, 具体内容为每个 level 的全部文件的
  // 相关信息, 这些信息包含每个文件的号码、文件大小、起止 key. 
  std::string DebugString() const;

 private:
  friend class Compaction;
  friend class VersionSet;

  class LevelFileNumIterator;
  Iterator* NewConcatenatingIterator(const ReadOptions&, int level) const;

  // Call func(arg, level, f) for every file that overlaps user_key in
  // order from newest to oldest.  If an invocation of func returns
  // false, makes no more calls.
  //
  // REQUIRES: user portion of internal_key == user_key.
  //
  // 从 level-0 开始, level-by-level 的针对与 user_key 重叠的每个文件
  // 按从最新到最老的顺序依次调用 func(arg, level, f). 
  // 针对 func 的调用失败一次即停止后续调用. 
  // 前提: internal_key 的 user key 部分 == user_key
  void ForEachOverlapping(Slice user_key, Slice internal_key,
                          void* arg,
                          bool (*func)(void*, int, FileMetaData*));
  // 该 version 所属的 VersionSet
  VersionSet* vset_;           
  // 接下来两个指针使得 Version 可以构成双向循环链表
  // 指向链表中下个 version 的指针
  Version* next_;              
  // 指向链表中前个 version 的指针
  Version* prev_;              
  // 该 version 的活跃引用计数
  int refs_;                    

  // 核心成员, 该成员保存了当前最新的 level 架构信息,
  // 即 db 每个 level 的文件元数据链表
  std::vector<FileMetaData*> files_[config::kNumLevels];

  // 基于查询统计而得出的下个待压实的文件及其所在的 level
  FileMetaData* file_to_compact_;
  int file_to_compact_level_;

  // 基于存储比值计算的压实分数,
  // 小于 1 意味着未到上限, 压实不是很需要.
  // 由 Finalize() 计算.
  double compaction_score_;
  // 基于存储比值而得出的下个待压实的 level.
  // 由 Finalize() 计算.
  int compaction_level_;

  explicit Version(VersionSet* vset)
      : vset_(vset), next_(this), prev_(this), refs_(0),
        file_to_compact_(nullptr),
        file_to_compact_level_(-1),
        compaction_score_(-1),
        compaction_level_(-1) {
  }

  ~Version();

  // No copying allowed
  Version(const Version&);
  void operator=(const Version&);
};

// 一个 DBImpl 由一组 Versions 构成. 
// 最新的 version 叫做 "current", 
// 较老的 versions 可能也会被保留以为活跃的迭代器提供一致性视图. 
// Version 跟踪全部 level 及其文件. 
// 全部 versions 集合由 VersionSet 维护. 
// Version, VersionSet 都是线程兼容的,
// 但是在访问的时候需要外部的同步设施. 
class VersionSet {
 public:
  VersionSet(const std::string& dbname,
             const Options* options,
             TableCache* table_cache,
             const InternalKeyComparator*);
  ~VersionSet();

  // 1, 将 *edit 内容(可看作当前针对 level 架构的增量更新)和当前 version 内容合并构成一个新的 version;
  // 2, 然后将这个新 version 内容序列化为一条日志持久化到新建的 manifest 文件;
  // 3, 同时将该 manifest 文件名写入 current 文件;
  // 4, 最后把新的 version 替换当前 version.
  //
  // 前提: *mu 在进入方法之前就被持有了. 
  // 前提: 没有其它线程并发调用该方法. 
  Status LogAndApply(VersionEdit* edit, port::Mutex* mu)
      EXCLUSIVE_LOCKS_REQUIRED(mu);

  // Recover the last saved descriptor from persistent storage.
  // 该方法负责从最后一个 MANIFEST 文件解析内容出来与当前 Version 保存的 level 架构合并保存到一个
  // 新建的 Version 中, 然后将这个新的 version 作为当前的 version.
  // 参数是输出型的, 负责保存一个指示当前 MANIFEST 文件是否可以续用.
  Status Recover(bool *save_manifest);

  // Return the current manifest file number
  //
  // 返回当前使用的 MANIFEST 文件 number
  uint64_t ManifestFileNumber() const { return manifest_file_number_; }

  // Allocate and return a new file number
  //
  // 分配和返回一个新的文件 number
  uint64_t NewFileNumber() { return next_file_number_++; }

  // Arrange to reuse "file_number" unless a newer file number has
  // already been allocated.
  // REQUIRES: "file_number" was returned by a call to NewFileNumber().
  //
  // 除非已经分配了一个新的文件 number, 否则重用参数中的 file_number
  void ReuseFileNumber(uint64_t file_number) {
    if (next_file_number_ == file_number + 1) {
      next_file_number_ = file_number;
    }
  }

  // Return the number of Table files at the specified level.
  //
  // 返回指定 level 的 Table 文件个数
  int NumLevelFiles(int level) const;

  // Return the combined file size of all files at the specified level.
  //
  // 返回指定 level 全部文件的总大小
  int64_t NumLevelBytes(int level) const;

  // 返回当前 version, 代表了当前 leveldb 磁盘文件架构的一个视图.
  Version* current() const { return current_; }

  // 返回上一个序列号
  uint64_t LastSequence() const { return last_sequence_; }

  // Set the last sequence number to s.
  //
  // 将上个序列号设置为 s, 注意 s 必须大于等于上个序列号
  void SetLastSequence(uint64_t s) {
    assert(s >= last_sequence_);
    last_sequence_ = s;
  }

  // Mark the specified file number as used.
  //
  // 将指定的文件 number 标记为已用
  void MarkFileNumberUsed(uint64_t number);

  // Return the current log file number.
  //
  // 返回当前 log 文件的 number
  uint64_t LogNumber() const { return log_number_; }

  // Return the log file number for the log file that is currently
  // being compacted, or zero if there is no such log file.
  //
  // 返回当前正在进行压实的 log 文件的 number, 如果没有文件在压实则返回 0.
  uint64_t PrevLogNumber() const { return prev_log_number_; }

  // 为一个新的压实过程挑选 level 和输入文件列表.
  // 挑选待压实 level 主要基于两条策略(优先级先 1 后 2)：
  // 1. 基于存储统计(Finalize()）发现某个 level 超上限严重, 拿来做压实.
  // 2. 基于查询统计(Version::Get(), Version::UpdateStats()) 发现有文件超过查询次数上限
  // 明确待压实 level 后, 挑选其待压实文件, 然后从将其父 level(即 level+1)将与其重叠文件加入到待压实文件集中.
  //
  // 如果不需要进行压实则返回 nullptr;
  // 否则返回一个指向在堆上分配的 compaction 对象的指针, 该对象表示压实相关信息.
  // 调用者负责释放返回的结果. 
  Compaction* PickCompaction();

  // 返回一个 Compaction 对象， 用于压实指定 level 的 [begin,end] 范围.
  // 如果指定 level 没有文件与该范围重叠则返回 nullptr.
  // 调用者负责释放返回的结果.
  Compaction* CompactRange(
      int level,
      const InternalKey* begin,
      const InternalKey* end);

  // Return the maximum overlapping data (in bytes) at next level for any
  // file at a level >= 1.
  //
  // 针对 level-1 及其以上的任意一个文件, 返回下一层与之重叠的最大数据大小(单位, 字节)
  int64_t MaxNextLevelOverlappingBytes();

  // 创建一个 iterator, 它负责迭代待压实的输入文件内容.
  // 当 iterator 不再使用的时候, 调用者负责删除它.
  // 注意: 因为待压实文件跨 level 存在重叠, 所以迭代器指向的内容并不是严格有序的.
  Iterator* MakeInputIterator(Compaction* c);

  // Returns true iff some level needs a compaction.
  //
  // 当且仅当某个 level 需要一次压实时返回 true
  bool NeedsCompaction() const {
    Version* v = current_;
    return (v->compaction_score_ >= 1) || (v->file_to_compact_ != nullptr);
  }

  // Add all files listed in any live version to *live.
  // May also mutate some internal state.
  //
  // 从旧到新遍历 version(因为新 version 是其前一个 version 合并 versionedit 构造的), 
  // 在每个 version 中从低到高遍历 level(针对同样的 key, level 越低其对应数据越新),
  // 将 level 中的文件都插入到集合 live 中.
  void AddLiveFiles(std::set<uint64_t>* live);

  // 返回目标 key 在 v 对应的 level 架构中的估计字节偏移量
  uint64_t ApproximateOffsetOf(Version* v, const InternalKey& key);

  // Return a human-readable short (single-line) summary of the number
  // of files per level.  Uses *scratch as backing store.
  struct LevelSummaryStorage {
    char buffer[100];
  };
  // 返回当前 version 保存的每个 level 文件数目.
  // 使用 *scratch 作为底层存储. 
  const char* LevelSummary(LevelSummaryStorage* scratch) const;

 private:
  class Builder;

  friend class Compaction;
  friend class Version;

  bool ReuseManifest(const std::string& dscname, const std::string& dscbase);

  void Finalize(Version* v);

  void GetRange(const std::vector<FileMetaData*>& inputs,
                InternalKey* smallest,
                InternalKey* largest);

  void GetRange2(const std::vector<FileMetaData*>& inputs1,
                 const std::vector<FileMetaData*>& inputs2,
                 InternalKey* smallest,
                 InternalKey* largest);

  void SetupOtherInputs(Compaction* c);

  // Save current contents to *log
  Status WriteSnapshot(log::Writer* log);

  // 令 v 成为 current_.
  // 每个 VersionSet 包含一组 Version,
  // 这里是将新创建的 Version 追加到该 VersionSet 的双向链表中.
  // 具体操作为将 v 插入到双向循环链表, 且位于 dummy_versions_ 前面. 
  void AppendVersion(Version* v);

  Env* const env_;
  const std::string dbname_;
  const Options* const options_;
  // 每次用户进行查询操作的时候(DBImpl::Get())可能需要去查询
  // 磁盘上的文件, 这就要求有个缓存功能来加速.
  // 下面这个成员会缓存 sstable 文件对应的 Table 实例, 
  // 用于加速用户的查询, 否则每次读文件解析
  // 就很慢了. 目前在用的缓存策略是 LRU.
  // 该变量实际值来自 DBImpl 实例, 具体见 VersionSet 构造方法.
  TableCache* const table_cache_;
  const InternalKeyComparator icmp_;
  uint64_t next_file_number_;
  uint64_t manifest_file_number_;
  // 记录最近一次更新操作对应的序列号(逐一递增, WriteBatch 包含一批更新操作, 每个更新操作都会有一个序列号).
  // 具体修改建 DbImpl::Write 方法
  uint64_t last_sequence_;
  uint64_t log_number_;
  uint64_t prev_log_number_;  // 0 or backing store for memtable being compacted

  // Opened lazily
  // 当前 MANIFEST 文件
  WritableFile* descriptor_file_;
  // MANIFEST 文件格式同 log 文件, 所以写入方法就复用了.
  // 其每条日志就是一个序列化后的 VersionEdit.
  log::Writer* descriptor_log_; 
  // 属于该 VersionSet 的 Version 都会被维护到一个双向循环链表中,
  // 而且新加入的 Version 都会插入到 dummy_versions_ 前面. 
  // dummy_versions_.next_ 默认指向自己(具体见 Version 构造函数)后续指向最老的 version.
  Version dummy_versions_; 
  // 指向当前 Version == dummy_versions_.prev_
  Version* current_;       

  // Per-level key at which the next compaction at that level should start.
  // Either an empty string, or a valid InternalKey.
  // 记录了每个 level 各自对应的下次压实的起始 key
  std::string compact_pointer_[config::kNumLevels];

  // No copying allowed
  VersionSet(const VersionSet&);
  void operator=(const VersionSet&);
};

// 该类封装了与一次压实过程相关的信息
class Compaction {
 public:
  ~Compaction();

  // 返回正在进行压实的 level. 
  // 来自 level 和 level+1 的输入将会被合并, 然后产生一组 level+1 文件. 
  int level() const { return level_; }

  // 返回一个 VersionEdit 对象, 该对象持有本次压实导致的更改, 这些修改会被保存到
  // descriptor 文件(即 MANIFEST 文件).
  VersionEdit* edit() { return &edit_; }

  // 返回 which level 对应的文件个数.
  // 参数 which 非 0 即 1, 因为 input_ 长度为 2. 
  int num_input_files(int which) const { return inputs_[which].size(); }

  // 返回 level[which] 中的第 i 个输入文件, which 非 0 即 1.
  FileMetaData* input(int which, int i) const { return inputs_[which][i]; }

  // 压实过程中可以构建的最大文件大小
  uint64_t MaxOutputFileSize() const { return max_output_file_size_; }

  // 如果一次压实可以通过将单个输入文件移动到下一层实现 (中间不涉及合并或者拆分),
  // 那这就是一个平凡的压实. 
  bool IsTrivialMove() const;

  // 将本次压实的全部输入作为删除操作添加到 *edit 中
  void AddInputDeletions(VersionEdit* edit);

  // 如果我们可用的信息能够保证压实正在 "level+1" 产生数据,
  // 而且对于 "level+1" 来说, 它包含的数据没有出现在更高层, 
  // 那么返回 true. 
  bool IsBaseLevelForKey(const Slice& user_key);

  // 如果参数 internal_key 在 level+2 太靠后意味着 level 与
  // level+2 重叠太多了, 这个键就是压实的右区间, 此时返回 true; 否则返回 false.
  bool ShouldStopBefore(const Slice& internal_key);

  // 圧实成功后, 释放输入 Version.
  void ReleaseInputs();

 private:
  friend class Version;
  friend class VersionSet;

  Compaction(const Options* options, int level);

  int level_;
  uint64_t max_output_file_size_;
  Version* input_version_;
  VersionEdit edit_;

  // 每次压实从 level_ 和 level_+1 读取内容
  std::vector<FileMetaData*> inputs_[2];

  // 用于保存与祖父重合的文件列表(parent == level_ + 1, grandparent == level_ + 2)
  // 这个主要是一个限制, 限制 level 和 level+1 合并后每个文件
  // 的大小以避免后续压实负担太重.
  std::vector<FileMetaData*> grandparents_;
  // 用于 grandparents_ 的索引变量
  size_t grandparent_index_;  // Index in grandparent_starts_
  // 已经被看到的输出 key
  bool seen_key_;             // Some output key has been seen
  // 当前输出与祖父文件重叠的字节数
  int64_t overlapped_bytes_;  // Bytes of overlap between current output
                              // and grandparent files

  // level_ptrs_ 用于实现 IsBaseLevelForKey

  // level_ptrs_ 持有 input_version_->levels_ 里的索引: our state
  // is that we are positioned at one of the file ranges for each
  // higher level than the ones involved in this compaction (i.e. for
  // all L >= level_ + 2).
  //
  // 用于实现 IsBaseLevelForKey 方法. 
  // level_ptrs_ 持有 input_version_->levels_ 的索引: 
  // 我们被定位到其中一个文件范围内, 对于没一个比当前中那个在进行压实的 level. 
  size_t level_ptrs_[config::kNumLevels];
};

}  // namespace leveldb

#endif  // STORAGE_LEVELDB_DB_VERSION_SET_H_

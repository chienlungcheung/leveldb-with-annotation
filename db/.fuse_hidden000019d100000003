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

// Return the smallest index i such that files[i]->largest >= key.
// Return files.size() if there is no such file.
// REQUIRES: "files" contains a sorted list of non-overlapping files.
//
// 返回满足条件 files[i]->largest >= key 的最小索引 i；如果不存在则返回 files 列表长度。
// 通过二分法在 files 里查找。
// 要求：files 必须包含一组有序的且无重叠的文件（除了 level-0，其它 levels 的文件列表均满足该条件）。
int FindFile(const InternalKeyComparator& icmp,
             const std::vector<FileMetaData*>& files,
             const Slice& key);

// Returns true iff some file in "files" overlaps the user key range
// [*smallest,*largest].
// smallest==nullptr represents a key smaller than all keys in the DB.
// largest==nullptr represents a key largest than all keys in the DB.
// REQUIRES: If disjoint_sorted_files, files[] contains disjoint ranges
//           in sorted order.
// 当且仅当 files 中某个文件与参数 [*smallest,*largest] 指定的 user key 区间重叠返回 true。
// smallest==nullptr 表示小于 DB 中全部 keys 的 key。
// largest==nullptr 表示大于 DB 中全部 keys 的 key。
// 要求：如果 disjoint_sorted_files 为 true，files[] 包含的区间不相交且有序。
bool SomeFileOverlapsRange(const InternalKeyComparator& icmp,
                           bool disjoint_sorted_files,
                           const std::vector<FileMetaData*>& files,
                           const Slice* smallest_user_key,
                           const Slice* largest_user_key);

class Version {
 public:
  // Append to *iters a sequence of iterators that will
  // yield the contents of this Version when merged together.
  // REQUIRES: This version has been saved (see VersionSet::SaveTo)
  //
  // 将一系列迭代器追加到 iters 向量里，这些迭代器在被合并后会生成该版本的内容。
  // 前提：该版本事先已经通过 VersionSet::SaveTo 方法被保存过了。
  void AddIterators(const ReadOptions&, std::vector<Iterator*>* iters);

  // Lookup the value for key.  If found, store it in *val and
  // return OK.  Else return a non-OK status.  Fills *stats.
  // REQUIRES: lock is not held
  struct GetStats {
    FileMetaData* seek_file;
    int seek_file_level;
  };
  // 结构体 GetStats 服务于下面紧接着的 Get 方法。
  // 从 level-0 开始一层一层查找 key 对应的 value。
  // 当查询一个 key 时，如果找到了对应的 value，则将 value 保存到 *val 指向的地址并且返回 OK；
  // 否则，返回一个  non-OK。
  // 前提： 调用该方法之前必须未持有锁。todo 这个前提不知道是不是写错了。
  Status Get(const ReadOptions&, const LookupKey& key, std::string* val,
             GetStats* stats);

  // Adds "stats" into the current state.  Returns true if a new
  // compaction may need to be triggered, false otherwise.
  // REQUIRES: lock is held
  // 将 stats 增加到当前状态中。如果需要触发一个压缩，则返回 true；否则返回 false。
  // 前提：调用该方法前必须已经持有锁。
  bool UpdateStats(const GetStats& stats);

  // Record a sample of bytes read at the specified internal key.
  // Samples are taken approximately once every config::kReadBytesPeriod
  // bytes.  Returns true if a new compaction may need to be triggered.
  // REQUIRES: lock is held
  //
  // 看了该方法实现发现上面的英文注释有点莫名其妙。
  // 该方法就是从 level-0 开始检查有多少个文件的范围与 key 重叠，
  // 如果有多于两个就会设置触发压实的标记，并且在检查过程中会记录第一个与 key 重叠的文件及其 level。
  // 如果需要触发一次新的压实，则返回 true，否则返回 false。
  // 前提：调用该方法前必须已经持有锁。
  bool RecordReadSample(Slice key);

  // Reference count management (so Versions do not disappear out from
  // under live iterators)
  // 引用计数管理
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
  //
  // 当且仅当指定 level 的某个文件与 [*smallest, *largest] 指定的 user key 区间重叠返回 true。
  // smallest==nullptr 表示小于 DB 中全部 keys 的 key。
  // largest==nullptr 表示大于 DB 中全部 keys 的 key。
  bool OverlapInLevel(int level,
                      const Slice* smallest_user_key,
                      const Slice* largest_user_key);

  // Return the level at which we should place a new memtable compaction
  // result that covers the range [smallest_user_key,largest_user_key].
  //
  // 返回需要进行压实的 level。todo 需要确认该方法怎么使用的，以及参数具体是什么
  int PickLevelForMemTableOutput(const Slice& smallest_user_key,
                                 const Slice& largest_user_key);

  // 返回 leven 指定层的文件个数
  int NumFiles(int level) const { return files_[level].size(); }

  // Return a human readable string that describes this version's contents.
  // 返回一个对人类友好的描述该 version 内容的字符串
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
  // 从 level-0 开始，level-by-level 的针对与 user_key 重叠的每个文件
  // 按从最新到最老的顺序依次调用 func(arg, level, f)。
  // 针对 func 的调用失败一次即停止后续调用。
  // 前提：internal_key 的 user key 部分 == user_key
  void ForEachOverlapping(Slice user_key, Slice internal_key,
                          void* arg,
                          bool (*func)(void*, int, FileMetaData*));
  // 该 version 所属的 VersionSet
  VersionSet* vset_;            // VersionSet to which this Version belongs
  // 链表中下个 version 指针
  Version* next_;               // Next version in linked list
  // 链表中前个 version 指针
  Version* prev_;               // Previous version in linked list
  // 该 version 的活跃引用计数
  int refs_;                    // Number of live refs to this version

  // List of files per level
  // 保存每个 level 的文件元数据链表
  std::vector<FileMetaData*> files_[config::kNumLevels];

  // Next file to compact based on seek stats.
  // 下个待压实的文件
  FileMetaData* file_to_compact_;
  int file_to_compact_level_;

  // Level that should be compacted next and its compaction score.
  // Score < 1 means compaction is not strictly needed.  These fields
  // are initialized by Finalize().
  // 压实分数，小于 1 意味着压实不是很需要。由 Finalize() 初始化。
  double compaction_score_;
  // 即将被压缩的 level 。由 Finalize() 初始化。
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

class VersionSet {
 public:
  VersionSet(const std::string& dbname,
             const Options* options,
             TableCache* table_cache,
             const InternalKeyComparator*);
  ~VersionSet();

  // Apply *edit to the current version to form a new descriptor that
  // is both saved to persistent state and installed as the new
  // current version.  Will release *mu while actually writing to the file.
  // REQUIRES: *mu is held on entry.
  // REQUIRES: no other thread concurrently calls LogAndApply()
  //
  // 将 *edit 内容施用到当前 version 来构成一个新的描述符，这个描述符会被持久化保存同时会被作为新的当前 version 进行安装。
  // 前提：*mu 在进入方法之前就被持有了。
  // 前提：没有其它线程并发调用该方法。
  Status LogAndApply(VersionEdit* edit, port::Mutex* mu)
      EXCLUSIVE_LOCKS_REQUIRED(mu);

  // Recover the last saved descriptor from persistent storage.
  //
  // 从持久化存储恢复上一个保存的描述符
  Status Recover(bool *save_manifest);

  // Return the current version.
  //
  // 返回当前 version
  Version* current() const { return current_; }

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
  // 除非已经分配了一个新的文件 number，否则重用参数中的 file_number
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

  // Return the last sequence number.
  //
  // 返回上一个序列号
  uint64_t LastSequence() const { return last_sequence_; }

  // Set the last sequence number to s.
  //
  // 将上个序列号设置为 s，注意 s 必须大于等于上个序列号
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
  // 返回当前正在进行压实的 log 文件的 number，如果没有文件在压实则返回 0.
  uint64_t PrevLogNumber() const { return prev_log_number_; }

  // Pick level and inputs for a new compaction.
  // Returns nullptr if there is no compaction to be done.
  // Otherwise returns a pointer to a heap-allocated object that
  // describes the compaction.  Caller should delete the result.
  //
  // 为一个新的压实过程挑选 level 和输入文件。
  // 如果不需要进行压实则返回 nullptr；否则返回一个指向在堆上分配的 compaction 对象的指针，该对象表示压实相关信息。
  // 调用者负责释放返回的结果。
  Compaction* PickCompaction();

  // Return a compaction object for compacting the range [begin,end] in
  // the specified level.  Returns nullptr if there is nothing in that
  // level that overlaps the specified range.  Caller should delete
  // the result.
  //
  // 为了压实指定 level 中 [begin,end] 范围返回一个 compaction 对象。
  // 如果该 level 没有文件与该范围重叠则返回 nullptr。调用者负责释放返回的结果。
  Compaction* CompactRange(
      int level,
      const InternalKey* begin,
      const InternalKey* end);

  // Return the maximum overlapping data (in bytes) at next level for any
  // file at a level >= 1.
  //
  // 针对 level-1 及其以上的任意一个文件，返回下一层与之重叠的最大数据大小（单位，字节）
  int64_t MaxNextLevelOverlappingBytes();

  // Create an iterator that reads over the compaction inputs for "*c".
  // The caller should delete the iterator when no longer needed.
  //
  // 创建一个 iterator，它负责读取压实后的输入 *c。
  // 当 iterator 不再使用的时候，调用者负责删除它。
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
  // 将任意活跃的 version 中的全部文件添加到 *live 中。
  // 可能会修改一些内部状态。
  void AddLiveFiles(std::set<uint64_t>* live);

  // Return the approximate offset in the database of the data for
  // "key" as of version "v".
  //
  // 返回估计偏移量 key 在数据库中的todo
  uint64_t ApproximateOffsetOf(Version* v, const InternalKey& key);

  // Return a human-readable short (single-line) summary of the number
  // of files per level.  Uses *scratch as backing store.
  struct LevelSummaryStorage {
    char buffer[100];
  };
  // 针对每一个 level 返回一个人类可读的短（一行）摘要。
  // 使用 *scratch 作为底层存储。
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

  void AppendVersion(Version* v);

  Env* const env_;
  const std::string dbname_;
  const Options* const options_;
  TableCache* const table_cache_;
  const InternalKeyComparator icmp_;
  uint64_t next_file_number_;
  uint64_t manifest_file_number_;
  uint64_t last_sequence_;
  uint64_t log_number_;
  uint64_t prev_log_number_;  // 0 or backing store for memtable being compacted

  // Opened lazily
  WritableFile* descriptor_file_;
  log::Writer* descriptor_log_;
  Version dummy_versions_;  // Head of circular doubly-linked list of versions.
  Version* current_;        // == dummy_versions_.prev_

  // Per-level key at which the next compaction at that level should start.
  // Either an empty string, or a valid InternalKey.
  std::string compact_pointer_[config::kNumLevels];

  // No copying allowed
  VersionSet(const VersionSet&);
  void operator=(const VersionSet&);
};

// A Compaction encapsulates information about a compaction.
// 该类封装了与一次压实过程相关的信息
class Compaction {
 public:
  ~Compaction();

  // Return the level that is being compacted.  Inputs from "level"
  // and "level+1" will be merged to produce a set of "level+1" files.
  //
  // 返回正在进行压实的 level。
  // 来自 level 和 level+1 的输入将会被合并，然后产生一组 level+1 文件。
  int level() const { return level_; }

  // Return the object that holds the edits to the descriptor done
  // by this compaction.
  //
  // 返回一个 VersionEdit 对象，该对象持有本次压实针对的描述符的编辑内容。
  VersionEdit* edit() { return &edit_; }

  // "which" must be either 0 or 1
  //
  // 参数 which 非 0 即 1，因为 input_ 长度为 2。
  int num_input_files(int which) const { return inputs_[which].size(); }

  // Return the ith input file at "level()+which" ("which" must be 0 or 1).
  //
  // 返回 level() + which 中的第 i 个输入文件，which 非 0 即 1.
  FileMetaData* input(int which, int i) const { return inputs_[which][i]; }

  // Maximum size of files to build during this compaction.
  //
  // 压实过程中可以构建的最大文件大小
  uint64_t MaxOutputFileSize() const { return max_output_file_size_; }

  // Is this a trivial compaction that can be implemented by just
  // moving a single input file to the next level (no merging or splitting)
  //
  // 如果一次压实可以通过将单个输入文件移动到下一层实现，中间不涉及合并或者拆分。
  // 那这就是一个平凡的压实。
  bool IsTrivialMove() const;

  // Add all inputs to this compaction as delete operations to *edit.
  //
  // 将本次压实的全部输入作为删除操作添加到 *edit 中
  void AddInputDeletions(VersionEdit* edit);

  // Returns true if the information we have available guarantees that
  // the compaction is producing data in "level+1" for which no data exists
  // in levels greater than "level+1".
  //
  // 如果我们可用的信息能够保证圧实正在 "level+1" 产生数据，
  // 而且对于 "level+1" 来说，它包含的数据没有出现在更高层，
  // 那么返回 true。
  bool IsBaseLevelForKey(const Slice& user_key);

  // Returns true iff we should stop building the current output
  // before processing "internal_key".
  //
  // 当且仅当我们在处理参数 “internal_key” 之前应该停止构建当前输出时返回 true
  bool ShouldStopBefore(const Slice& internal_key);

  // Release the input version for the compaction, once the compaction
  // is successful.
  //
  // 圧实成功后，释放输入。
  void ReleaseInputs();

 private:
  friend class Version;
  friend class VersionSet;

  Compaction(const Options* options, int level);

  int level_;
  uint64_t max_output_file_size_;
  Version* input_version_;
  VersionEdit edit_;

  // Each compaction reads inputs from "level_" and "level_+1"
  //
  // 每次压实从 level_ 和 level_+1 读取的内容
  std::vector<FileMetaData*> inputs_[2];      // The two sets of inputs

  // State used to check for number of of overlapping grandparent files
  // (parent == level_ + 1, grandparent == level_ + 2)
  //
  // 用于保存与祖父重合的文件列表(parent == level_ + 1, grandparent == level_ + 2)
  std::vector<FileMetaData*> grandparents_;
  // 用于 grandparents_ 的索引变量
  size_t grandparent_index_;  // Index in grandparent_starts_
  // 已经被看到的输出 key
  bool seen_key_;             // Some output key has been seen
  // 当前输出与祖父文件重叠的字节数
  int64_t overlapped_bytes_;  // Bytes of overlap between current output
                              // and grandparent files

  // State for implementing IsBaseLevelForKey

  // level_ptrs_ holds indices into input_version_->levels_: our state
  // is that we are positioned at one of the file ranges for each
  // higher level than the ones involved in this compaction (i.e. for
  // all L >= level_ + 2).
  size_t level_ptrs_[config::kNumLevels];
};

}  // namespace leveldb

#endif  // STORAGE_LEVELDB_DB_VERSION_SET_H_

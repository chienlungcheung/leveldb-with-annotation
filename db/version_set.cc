// Copyright (c) 2011 The LevelDB Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file. See the AUTHORS file for names of contributors.

#include "db/version_set.h"

#include <algorithm>
#include <stdio.h>
#include "db/filename.h"
#include "db/log_reader.h"
#include "db/log_writer.h"
#include "db/memtable.h"
#include "db/table_cache.h"
#include "leveldb/env.h"
#include "leveldb/table_builder.h"
#include "table/merger.h"
#include "table/two_level_iterator.h"
#include "util/coding.h"
#include "util/logging.h"

namespace leveldb {

// 目标文件大小（leveldb 最多写入 max_file_size 个字节到一个文件中，超过这个值就会关闭当前文件然后写下一个新的文件）
static size_t TargetFileSize(const Options* options) {
  return options->max_file_size;
}

// Maximum bytes of overlaps in grandparent (i.e., level+2) before we
// stop building a single file in a level->level+1 compaction.
// 压实过程：
//    当 level-L 大小超过了上限，我们就在后台线程中将其压实。
//    压实过程会从 level-L 挑一个文件，然后将 level-(L+1) 中与该文件键区间重叠的文件都找出来。
// 一次压实会合并多个被挑选文件的内容从而生成一系列新的 level-(L+1) 文件，生成一个新文件的条件有两个：
//    - 当前文件大小达到了 2MB
//    - 当前文件的键区间与超过 10 个 level-(L+2) 文件发生了重叠。
// 第二个条件的目的在于避免后续对 level-(L+1) 文件进行压实时需要从 level-(L+2) 读取过多的数据。
//
// 下面这个方法是上述第二个条件限制的实现，它返回的是 10 个文件的大小，达到这个大小表示达到了 10 个文件。
static int64_t MaxGrandParentOverlapBytes(const Options* options) {
  return 10 * TargetFileSize(options);
}

// Maximum number of bytes in all compacted files.  We avoid expanding
// the lower level file set of a compaction if it would make the
// total compaction cover more than this many bytes.
//
// 已经被压实的全部文件的最大字节数。
// 如果一个压实后的文件集合字节数超过该值，那么我们就避免展开它。
static int64_t ExpandedCompactionByteSizeLimit(const Options* options) {
  return 25 * TargetFileSize(options);
}

// 除了 level-0 以外，每个 level 允许的最大字节数。
static double MaxBytesForLevel(const Options* options, int level) {
  // Note: the result for level zero is not really used since we set
  // the level-0 compaction threshold based on number of files.
  // 注意：这里的 result 对 level-0 没有用处，
  // 因为针对 level-0 的压实阈值基于文件个数，默认是超过 4 个就触发。

  // Result for both level-0 and level-1
  // 10MB
  double result = 10. * 1048576.0;
  // level-L 大小不超过 10^L MB
  while (level > 1) {
    result *= 10;
    level--;
  }
  return result;
}

// 返回某个 level 最大文件大小，当前每一层文件大小都是默认 2MB
static uint64_t MaxFileSizeForLevel(const Options* options, int level) {
  // We could vary per level to reduce number of files?
  return TargetFileSize(options);
}

// 计算 files 中全部文件的大小
static int64_t TotalFileSize(const std::vector<FileMetaData*>& files) {
  int64_t sum = 0;
  for (size_t i = 0; i < files.size(); i++) {
    sum += files[i]->file_size;
  }
  return sum;
}

Version::~Version() {
  assert(refs_ == 0);

  // Remove from linked list
  // 将该 version 从链表移除
  prev_->next_ = next_;
  next_->prev_ = prev_;

  // Drop references to files
  // 将该 version 引用的而全部文件元数据引用数减一
  for (int level = 0; level < config::kNumLevels; level++) {
    for (size_t i = 0; i < files_[level].size(); i++) {
      FileMetaData* f = files_[level][i];
      assert(f->refs > 0);
      f->refs--; // 每个文件元数据被某个 version 引用的时候它对应的计数会加一，因为该 version 要销毁了，所以这里将其减一
      if (f->refs <= 0) {
        delete f; // 如果没有任何 version 引用该文件元数据，则将其所占空间释放
      }
    }
  }
}

int FindFile(const InternalKeyComparator& icmp, // internal_key comparator
             const std::vector<FileMetaData*>& files,
             const Slice& key) {
  uint32_t left = 0;
  uint32_t right = files.size();
  while (left < right) { // 左闭右开区间
    uint32_t mid = (left + right) / 2;
    const FileMetaData* f = files[mid];
    if (icmp.InternalKeyComparator::Compare(f->largest.Encode(), key) < 0) {
      // Key at "mid.largest" is < "target".  Therefore all
      // files at or before "mid" are uninteresting.
      // 如果 mid 小于 key，那 [0, mid] 都不满足条件
      left = mid + 1;
    } else {
      // Key at "mid.largest" is >= "target".  Therefore all files
      // after "mid" are uninteresting.
      right = mid; // 如果 mid 大于等于 key，那么 (mid, files.size()) 都不满足条件
    }
  }
  return right; // 返回后要校验 right 是否小于 files.size()
}

// 判断 user_key 是否大于 f 文件中最大的 key
static bool AfterFile(const Comparator* ucmp, // user_key comparator
                      const Slice* user_key, const FileMetaData* f) {
  // null user_key occurs before all keys and is therefore never after *f
  return (user_key != nullptr &&
          ucmp->Compare(*user_key, f->largest.user_key()) > 0);
}

// 判断 user_key 是否小于 f 文件中最小的 key
static bool BeforeFile(const Comparator* ucmp, // user_key comparator
                       const Slice* user_key, const FileMetaData* f) {
  // null user_key occurs after all keys and is therefore never before *f
  return (user_key != nullptr &&
          ucmp->Compare(*user_key, f->smallest.user_key()) < 0);
}

bool SomeFileOverlapsRange(
    const InternalKeyComparator& icmp,
    bool disjoint_sorted_files,
    const std::vector<FileMetaData*>& files,
    const Slice* smallest_user_key,
    const Slice* largest_user_key) {
  const Comparator* ucmp = icmp.user_comparator();
  // 如果 files 中的文件之间相交但相互之间有序，
  // 则需要从小到大挨个校验每个 file 是否与区间 [smallest_user_key, largest_user_key] 有交集
  if (!disjoint_sorted_files) {
    // Need to check against all files
    for (size_t i = 0; i < files.size(); i++) {
      const FileMetaData* f = files[i];
      if (AfterFile(ucmp, smallest_user_key, f) ||
          BeforeFile(ucmp, largest_user_key, f)) {
        // No overlap
      } else {
        return true;  // Overlap
      }
    }
    return false;
  }

  // Binary search over file list
  // 如果各个文件之间无交集，则可以使用二分法来查找（文件内部保证有序）
  uint32_t index = 0;
  if (smallest_user_key != nullptr) {
    // Find the earliest possible internal key for smallest_user_key
    // 构建 smallest_user_key 对应的 internal_key，即使用 smallest_user_key 和最大的序列号、最大的操作类型进行拼接
    InternalKey small(*smallest_user_key, kMaxSequenceNumber, kValueTypeForSeek);
    // 在 files 中寻找最大 key 大于等于 smallest_user_key 的第一个文件.
    // 注意这里查的是第一个，因为后面还要比较 largest_user_key 与该文件最小 key 的关系，
    // 如果该文件最小 key 都大于 largest_user_key，
    // 那么无疑后面的文件最小 key 肯定也都大于 largest_user_key。
    index = FindFile(icmp, files, small.Encode());
  }

  if (index >= files.size()) { // 不存在这样的文件
    // beginning of range is after all files, so no overlap.
    return false;
  }

  // 到这说明 files[index].largest >= smallest_user_key，
  // 需要确认 files[index].smallest <= largest_user_key 是否成立，如果成立则相交。
  // 为啥不是检查 largest_user_key <= files[index].largest？
  // 是为了避免 files[index].largest > smallest_user_key 且 files[index].largest > largest_user_key，
  // 这样看不出来是否相交。

  // 如果存在这样的文件，检查 largest_user_key 是否不小于该文件最小的 key。
  // 前面确认了 files[index].largest >= smallest_user_key，这是相交成立的第一个条件，
  // 如果 largest_user_key >= files[index].smallest，
  // 而且已知 largest_user_key >= smallest_user_key，
  // 则说明 [smallest, largest] 与 [files[index].smallest, files[index].largest] 相交。
  return !BeforeFile(ucmp, largest_user_key, files[index]);
}

// An internal iterator.  For a given version/level pair, yields
// information about the files in the level.  For a given entry, key()
// is the largest key that occurs in the file, and value() is an
// 16-byte value containing the file number and file size, both
// encoded using EncodeFixed64.
//
// 一个迭代器，迭代的是 flist 文件列表
class Version::LevelFileNumIterator : public Iterator {
 public:
  LevelFileNumIterator(const InternalKeyComparator& icmp,
                       const std::vector<FileMetaData*>* flist)
      : icmp_(icmp),
        flist_(flist),
        index_(flist->size()) {        // Marks as invalid 初始置为非法值
  }
  // index_ 取值有效则该迭代器有效
  virtual bool Valid() const {
    return index_ < flist_->size();
  }
  // 将 index_ 移动到 flist 中第一个最大 key 大于等于 target 的索引位置
  virtual void Seek(const Slice& target) {
    index_ = FindFile(icmp_, *flist_, target);
  }
  virtual void SeekToFirst() { index_ = 0; }
  virtual void SeekToLast() {
    index_ = flist_->empty() ? 0 : flist_->size() - 1; // 如果 flist 为空，赋值为 0
  }
  virtual void Next() {
    assert(Valid());
    index_++;
  }
  virtual void Prev() {
    assert(Valid());
    if (index_ == 0) {
      index_ = flist_->size();  // Marks as invalid
    } else {
      index_--;
    }
  }
  // key 为 index_ 指向文件的最大 key
  Slice key() const {
    assert(Valid());
    return (*flist_)[index_]->largest.Encode();
  }
  // value 为 index_ 指向文件的 number 和 file_size 组合。
  Slice value() const {
    assert(Valid());
    EncodeFixed64(value_buf_, (*flist_)[index_]->number);
    EncodeFixed64(value_buf_+8, (*flist_)[index_]->file_size);
    return Slice(value_buf_, sizeof(value_buf_));
  }
  virtual Status status() const { return Status::OK(); }
 private:
  const InternalKeyComparator icmp_;
  const std::vector<FileMetaData*>* const flist_;
  uint32_t index_; // flist 当前索引即为该迭代器的底层表示，移动迭代器即移动它

  // Backing store for value().  Holds the file number and size.
  // value() 方法的底层存储，保存着 file number 和 file size，这两者都是 varint64 格式。
  // 改成员为 mutable 形式，表示可以在 const 方法中对其进行修改。
  mutable char value_buf_[16];
};

// 从 tablecache 中获取 file_value 对应的 table 文件的双层迭代器
static Iterator* GetFileIterator(void* arg,
                                 const ReadOptions& options,
                                 const Slice& file_value) {
  TableCache* cache = reinterpret_cast<TableCache*>(arg);
  if (file_value.size() != 16) {
    return NewErrorIterator(
        Status::Corruption("FileReader invoked with unexpected value"));
  } else {
    return cache->NewIterator(options,
                              DecodeFixed64(file_value.data()),
                              DecodeFixed64(file_value.data() + 8));
  }
}

// 构造第 level 层文件列表的双层迭代器：
// - 第一层迭代器（LevelFileNumIterator）指向文件；
// - 第二层迭代器指向某个 table 文件具体内容，其实它也是一个双层迭代器。
Iterator* Version::NewConcatenatingIterator(const ReadOptions& options,
                                            int level) const {
  return NewTwoLevelIterator(
      new LevelFileNumIterator(vset_->icmp_, &files_[level]),
      &GetFileIterator, vset_->table_cache_, options);
}

void Version::AddIterators(const ReadOptions& options,
                           std::vector<Iterator*>* iters) {
  // Merge all level zero files together since they may overlap
  // 将 level-0 文件合并到一起，因为它们互相之间可能有重叠。
  // 合并过程就是为各个 table 文件生成相应的两层迭代器，然后将各个迭代器放入 *iters
  for (size_t i = 0; i < files_[0].size(); i++) {
    iters->push_back(
        // 针对给定的 file_number（对应的文件长度也必须恰好是 file_size 字节数），返回一个与其对应 table 的 iterator。
        // 如果 tableptr 参数非空，设置 *tableptr 指向返回的 iterator 底下的 Table 对象。
        // 返回的 *tableptr 对象由 cache 所拥有，所以用户不要删除它；而且只要 iterator 还活着，该对象就有效。
        vset_->table_cache_->NewIterator(
            options, files_[0][i]->number, files_[0][i]->file_size));
  }

  // For levels > 0, we can use a concatenating iterator that sequentially
  // walks through the non-overlapping files in the level, opening them
  // lazily.
  // level-1 及其以上，为每一层生成一个级联迭代器
  // （本质也是一个两层迭代器具体见 Version::NewConcatenatingIterator，level-1 及其之上，每一层内部，文件不会发生重叠）
  // 放入 *iters
  for (int level = 1; level < config::kNumLevels; level++) {
    if (!files_[level].empty()) {
      iters->push_back(NewConcatenatingIterator(options, level));
    }
  }
}

// Callback from TableCache::Get()
// 以下结构和方法使用见 TableCache::Get() 方法
namespace {
enum SaverState {
  kNotFound,
  kFound,
  kDeleted,
  kCorrupt,
};
struct Saver {
  SaverState state;
  const Comparator* ucmp; // user_key comparator
  Slice user_key;
  std::string* value;
};
}

// 如果 arg （其实是个 Saver）中保存的 key 与 ikey 相等，且 ikey 对应的 tag 不表示删除，则将
// 与 ikey 对应的 value 保存到 arg 对应成员中。
static void SaveValue(void* arg, const Slice& ikey, const Slice& v) {
  Saver* s = reinterpret_cast<Saver*>(arg);
  ParsedInternalKey parsed_key;
  if (!ParseInternalKey(ikey, &parsed_key)) {
    s->state = kCorrupt;
  } else {
    if (s->ucmp->Compare(parsed_key.user_key, s->user_key) == 0) {
      // 因为 leveldb 的删除也是一种写操作，所以要检查 key 的 type
      s->state = (parsed_key.type == kTypeValue) ? kFound : kDeleted;
      if (s->state == kFound) {
        // 将对应的 value 赋值到 saver 对应成员中
        s->value->assign(v.data(), v.size());
      }
    }
  }
}

// 比较 a 和 b 谁的 file_number 更大，大的那个文件比较新
static bool NewestFirst(FileMetaData* a, FileMetaData* b) {
  return a->number > b->number;
}

void Version::ForEachOverlapping(Slice user_key, Slice internal_key,
                                 void* arg,
                                 bool (*func)(void*, int, FileMetaData*)) {
  // TODO(sanjay): Change Version::Get() to use this function.
  // 取出用于比较 user_key 的 comparator
  const Comparator* ucmp = vset_->icmp_.user_comparator();

  /**
   * 先处理 level-0，这一层比较特殊，因为文件之间可能存在重叠。
   */
  // Search level-0 in order from newest to oldest.
  std::vector<FileMetaData*> tmp;
  // 数目确定，为 tmp 一次性分配空间，避免后续的重分配导致耗时
  tmp.reserve(files_[0].size());
  // 顺序搜索 level-0 文件寻找与 user_key 重叠的文件
  for (uint32_t i = 0; i < files_[0].size(); i++) {
    FileMetaData* f = files_[0][i];
    if (ucmp->Compare(user_key, f->smallest.user_key()) >= 0 &&
        ucmp->Compare(user_key, f->largest.user_key()) <= 0) {
      // 如果 user_key 落在了 f 文件范围内，则将 f 加入 tmp
      tmp.push_back(f);
    }
  }
  if (!tmp.empty()) {
    // 将与 user_key 重叠的文件进行排序，从最新到最旧
    std::sort(tmp.begin(), tmp.end(), NewestFirst);
    for (uint32_t i = 0; i < tmp.size(); i++) {
      // 按照文件最新到最旧调用 func，直至 func 返回 false。
      if (!(*func)(arg, 0, tmp[i])) {
        return;
      }
    }
  }

  /**
   * 处理其它 levels，因为除了 level-0 其它 level 内部文件都不存在重叠（而且还是有序的）
   */
  // Search other levels.
  for (int level = 1; level < config::kNumLevels; level++) {
    size_t num_files = files_[level].size();
    // 有的 level 可能为空
    if (num_files == 0) continue;

    // Binary search to find earliest index whose largest key >= internal_key.
    // 通过二分查找找到第一个大于等于 internal_key 的文件，注意，user_key 与 internal_key 用户部分是相同的。
    uint32_t index = FindFile(vset_->icmp_, files_[level], internal_key);
    // 找到了一个可能与 user_key 重叠的文件
    if (index < num_files) {
      FileMetaData* f = files_[level][index];
      // 这个判断确保 user_key 与 f 文件重叠
      if (ucmp->Compare(user_key, f->smallest.user_key()) < 0) {
        // All of "f" is past any data for user_key
        // 不重叠
      } else {
        // 存在重叠，则在该文件上调用 func
        if (!(*func)(arg, level, f)) {
          return;
        }
      }
    }
  }
}

Status Version::Get(const ReadOptions& options,
                    const LookupKey& k,
                    std::string* value,
                    GetStats* stats) {
  Slice ikey = k.internal_key();
  Slice user_key = k.user_key();
  const Comparator* ucmp = vset_->icmp_.user_comparator();
  Status s;

  stats->seek_file = nullptr;
  stats->seek_file_level = -1;
  FileMetaData* last_file_read = nullptr;
  int last_file_read_level = -1;

  // We can search level-by-level since entries never hop across
  // levels.  Therefore we are guaranteed that if we find data
  // in an smaller level, later levels are irrelevant.
  // 我们采用 level-by-level 的寻找，因为数据项不会跨越多层。因此，可以保证当我们
  // 在一个较小的 level 找到数据的时候，不用在更大的 levels 找了。
  std::vector<FileMetaData*> tmp;
  FileMetaData* tmp2;
  // 逐层处理
  for (int level = 0; level < config::kNumLevels; level++) {
    size_t num_files = files_[level].size(); // 第 level 层文件总数
    if (num_files == 0) continue; // 空 level，跳过

    // Get the list of files to search in this level
    // 指针可以改，但指针指向内容也不能改，避免下面误操作。
    // 而且还要注意，这里利用 vector 底层存储连续的特点直接采用指针遍历 vector，而不是采用 vector 内置迭代器方式进行。
    FileMetaData* const* files = &files_[level][0];
    if (level == 0) {
      // Level-0 files may overlap each other.  Find all files that
      // overlap user_key and process them in order from newest to oldest.
      // level-0 比较特殊，因为它内部的文件之间可能互相重叠，所以需要单独处理。
      // 找到全部与 user_key 有重叠的文件，然后从最新到最旧顺序进行处理。
      tmp.reserve(num_files); // 已知存储上限，预分配，避免后续重分配消耗性能
      for (uint32_t i = 0; i < num_files; i++) {
        // 遍历 vector，检查每个文件是否与 user_key 重叠
        FileMetaData* f = files[i];
        if (ucmp->Compare(user_key, f->smallest.user_key()) >= 0 &&
            ucmp->Compare(user_key, f->largest.user_key()) <= 0) {
          // 将有重叠的文件加入到临时存储
          tmp.push_back(f);
        }
      }
      // level-0 没有文件与 user_key 由重叠，返回继续处理下一层
      if (tmp.empty()) continue;

      // 按照 file number 对文件进行从最新到最旧排序
      std::sort(tmp.begin(), tmp.end(), NewestFirst);
      // 修改指向
      files = &tmp[0];
      num_files = tmp.size(); // 重叠的文件总个数
    } else {
      // Binary search to find earliest index whose largest key >= ikey.
      // 在该层采用二分查找定位那个满足最大 key >= ikey 的第一个文件的索引
      uint32_t index = FindFile(vset_->icmp_, files_[level], ikey);
      if (index >= num_files) { // 没找到
        files = nullptr;
        num_files = 0;
      } else {
        // 找到了，再比较 user_key，确保 user_key 与该文件范围重叠。用 user_key 是现成的，internal_key 还需要构造。
        tmp2 = files[index];
        // 未重叠
        if (ucmp->Compare(user_key, tmp2->smallest.user_key()) < 0) {
          // All of "tmp2" is past any data for user_key
          files = nullptr;
          num_files = 0;
        } else {
          // 重叠
          files = &tmp2;
          num_files = 1;
        }
      }
    }

    // 遍历存在重叠的每一个文件（如果是在第 0 层找到的重叠文件，那可能存在多个文件）
    for (uint32_t i = 0; i < num_files; ++i) {
      if (last_file_read != nullptr && stats->seek_file == nullptr) {
        // We have had more than one seek for this read.  Charge the 1st file.
        // 如果不能查一次文件就找到，需要记录下查询的第一个文件及其所处的 level（todo 不知道这是要干啥）
        stats->seek_file = last_file_read;
        stats->seek_file_level = last_file_read_level;
      }

      FileMetaData* f = files[i];
      last_file_read = f;
      last_file_read_level = level;

      Saver saver;
      saver.state = kNotFound;
      saver.ucmp = ucmp;
      saver.user_key = user_key;
      saver.value = value;
      // f 对应的 table 文件可能已经在 cache 中了（不在的话读取后也会加入 cache），
      // 从该文件中查找有无 internal_key 为 ikey 的数据项，如果找到，则调用 SaveValue 将
      // 对应的 value 保存到 saver 数据结构中。
      s = vset_->table_cache_->Get(options, f->number, f->file_size,
                                   ikey, &saver, SaveValue);
      if (!s.ok()) {
        return s;
      }
      switch (saver.state) {
        case kNotFound:
          // 未在该文件中找到，继续查找下个文件
          break;      // Keep searching in other files
        case kFound:
          // 找到了，返回
          return s;
        case kDeleted:
          // 被删除了，返回
          // 这也是为啥上面说从最新文件向最旧文件查找，因为 leveldb 的增和删都是一种插入操作，
          // 如果一个文件先增后删，删除操作对应的插入肯定晚于增加操作。
          s = Status::NotFound(Slice());  // Use empty error message for speed
          return s;
        case kCorrupt:
          // 文件损坏了，返回
          s = Status::Corruption("corrupted key for ", user_key);
          return s;
      }
    }
  }

  return Status::NotFound(Slice());  // Use an empty error message for speed
}

bool Version::UpdateStats(const GetStats& stats) {
  FileMetaData* f = stats.seek_file;
  if (f != nullptr) {
    f->allowed_seeks--;
    if (f->allowed_seeks <= 0 && file_to_compact_ == nullptr) {
      file_to_compact_ = f;
      file_to_compact_level_ = stats.seek_file_level;
      return true;
    }
  }
  return false;
}

bool Version::RecordReadSample(Slice internal_key) {
  ParsedInternalKey ikey;
  if (!ParseInternalKey(internal_key, &ikey)) {
    return false;
  }

  struct State {
    GetStats stats;  // Holds first matching file
    int matches;

    static bool Match(void* arg, int level, FileMetaData* f) {
      State* state = reinterpret_cast<State*>(arg);
      state->matches++;
      if (state->matches == 1) {
        // Remember first match.
        state->stats.seek_file = f;
        state->stats.seek_file_level = level;
      }
      // We can stop iterating once we have a second match.
      return state->matches < 2;
    }
  };

  State state;
  state.matches = 0;
  // 在每个与 user_key 重叠的文件上运行上面定义的 Match 方法，
  // 并将第一个重叠的文件记录到 state 中，其它重叠文件只是在 state 中计数。
  ForEachOverlapping(ikey.user_key, internal_key, &state, &State::Match);

  // Must have at least two matches since we want to merge across
  // files. But what if we have a single file that contains many
  // overwrites and deletions?  Should we have another mechanism for
  // finding such files?
  //
  // 至少有两次匹配（即至少存在两个文件的范围与 user_key 重叠）我们才去合并文件即压实。
  // 但是如果存在单个文件包含很多复写和删除怎么办？我们应该为发现此类文件建立另外的机制吗？
  if (state.matches >= 2) {
    // 1MB cost is about 1 seek (see comment in Builder::Apply).
    // 一次查询大约消耗 1MB（具体见 Builder::Apply 方法注释）
    return UpdateStats(state.stats);
  }
  return false;
}

void Version::Ref() {
  ++refs_;
}

void Version::Unref() {
  assert(this != &vset_->dummy_versions_);
  assert(refs_ >= 1);
  --refs_;
  if (refs_ == 0) {
    // 如果该 version 没有了活跃引用则释放其空间
    delete this;
  }
}

bool Version::OverlapInLevel(int level,
                             const Slice* smallest_user_key,
                             const Slice* largest_user_key) {
  // (level > 0) 表示 level-0 以上的 level 内部文件之间才不会有重叠（有序当然是默认成立的）
  return SomeFileOverlapsRange(vset_->icmp_, (level > 0), files_[level],
                               smallest_user_key, largest_user_key);
}

int Version::PickLevelForMemTableOutput(
    const Slice& smallest_user_key,
    const Slice& largest_user_key) {
  int level = 0;
  // 检查 level-0 是否有文件与 [smallest_user_key, largest_user_key] 有重叠，
  // 如果存在重叠，返回 level-0；否则进一步检查其它 levels。
  if (!OverlapInLevel(0, &smallest_user_key, &largest_user_key)) {
    // Push to next level if there is no overlap in next level,
    // and the #bytes overlapping in the level after that are limited.
    // 分别构建 smallest_user_key 和 largest_user_key 对应的 internal_key。
    // 针对 internal_key，user_key 部分越大越大，序列号越小越大，类型越小越大。
    InternalKey start(smallest_user_key, kMaxSequenceNumber, kValueTypeForSeek);
    InternalKey limit(largest_user_key, 0, static_cast<ValueType>(0));
    std::vector<FileMetaData*> overlaps;
    // 压实过程：
    //    当 level-L 大小超过了上限，我们就在后台线程中将其压实。
    //    压实过程会从 level-L 挑一个文件，然后将 level-(L+1) 中与该文件键区间重叠的文件都找出来。
    // 一次压实会合并多个被挑选文件的内容从而生成一系列新的 level-(L+1) 文件，生成一个新文件的条件有两个：
    //    - 当前文件大小达到了 2MB
    //    - 当前文件的键区间与超过 10 个 level-(L+2) 文件发生了重叠。
    // 第二个条件的目的在于避免后续对 level-(L+1) 文件进行压实时需要从 level-(L+2) 读取过多的数据。
    while (level < config::kMaxMemCompactLevel) {
      // 检查 level-(L+1) 中是否存在与 [smallest_user_key, largest_user_key] 重叠的文件
      if (OverlapInLevel(level + 1, &smallest_user_key, &largest_user_key)) {
        // 如果 level-(L+1) 中存在与 [smallest_user_key, largest_user_key] 重叠的文件则跳出循环，返回该 level
        break;
      }
      // 如果 level-(L+1) 中不存在与 [smallest_user_key, largest_user_key] 重叠的文件，
      // 则检查该  level-(L+2) 是否存在过多（过多即是看重叠文件个数是否超出阈值）与
      // [smallest_user_key, largest_user_key] 重叠的文件。如果重叠的文件数超过阈值，则表示 level-L 需要进行压实了。
      if (level + 2 < config::kNumLevels) {
        // Check that file does not overlap too many grandparent bytes.
        // 获取 level-(L+2) 中与 [smallest_user_key, largest_user_key] 有重叠的全部文件，
        // 并保存到 overlaps。
        GetOverlappingInputs(level + 2, &start, &limit, &overlaps);
        // 计算 level-（L+2） 中与 [smallest_user_key, largest_user_key] 有重叠的全部文件的总大小
        const int64_t sum = TotalFileSize(overlaps);
        // 如果大小超过了阈值，表示 level-(L+2) 存在过多与 [smallest_user_key, largest_user_key] 重叠的文件，
        // 则跳出循环返回 level-L。
        if (sum > MaxGrandParentOverlapBytes(vset_->options_)) {
          break;
        }
      }
      level++;
    }
  }
  return level;
}

// Store in "*inputs" all files in "level" that overlap [begin,end]
//
// 将参数 "level" 指定层中与区间 [begin,end] 有重叠的全部文件保存到 "*inputs"
void Version::GetOverlappingInputs(
    int level,
    const InternalKey* begin,
    const InternalKey* end,
    std::vector<FileMetaData*>* inputs) {
  assert(level >= 0);
  assert(level < config::kNumLevels);
  inputs->clear();
  Slice user_begin, user_end;
  if (begin != nullptr) {
    user_begin = begin->user_key();
  }
  if (end != nullptr) {
    user_end = end->user_key();
  }
  const Comparator* user_cmp = vset_->icmp_.user_comparator();
  // 遍历 level 层每一个文件，检查是否与 [user_begin,user_end] 有重叠
  for (size_t i = 0; i < files_[level].size(); ) {
    FileMetaData* f = files_[level][i++];
    const Slice file_start = f->smallest.user_key();
    const Slice file_limit = f->largest.user_key();
    if (begin != nullptr && user_cmp->Compare(file_limit, user_begin) < 0) {
      // "f" is completely before specified range; skip it
      // f 最大 key 小于 user_begin，则肯定无交集，跳过。
    } else if (end != nullptr && user_cmp->Compare(file_start, user_end) > 0) {
      // "f" is completely after specified range; skip it
      // f 最小 key 大于 user_end，则肯定无交集，跳过。
    } else {
      inputs->push_back(f);
      if (level == 0) {
        // Level-0 files may overlap each other.  So check if the newly
        // added file has expanded the range.  If so, restart search.
        if (begin != nullptr && user_cmp->Compare(file_start, user_begin) < 0) {
          user_begin = file_start;
          inputs->clear();
          i = 0;
        } else if (end != nullptr && user_cmp->Compare(file_limit,
                                                       user_end) > 0) {
          user_end = file_limit;
          inputs->clear();
          i = 0;
        }
      }
    }
  }
}

std::string Version::DebugString() const {
  std::string r;
  // 遍历每一个 level
  for (int level = 0; level < config::kNumLevels; level++) {
    // 每个 level 输出就是下面这个形式，
    // 17 表示的 file number，123 表示的 file size，
    // 中括号第一个值表示该 file 最小的 internal_key，然后省略号，第二个值表示该 file 最大的 internal_key
    // E.g.,
    //   --- level 1 ---
    //   17:123['a' .. 'd']
    //   20:43['e' .. 'g']
    r.append("--- level ");
    AppendNumberTo(&r, level);
    r.append(" ---\n");
    const std::vector<FileMetaData*>& files = files_[level];
    for (size_t i = 0; i < files.size(); i++) { // 遍历每个 level 每个问文件
      r.push_back(' ');
      AppendNumberTo(&r, files[i]->number);
      r.push_back(':');
      AppendNumberTo(&r, files[i]->file_size);
      r.append("[");
      r.append(files[i]->smallest.DebugString());
      r.append(" .. ");
      r.append(files[i]->largest.DebugString());
      r.append("]\n");
    }
  }
  return r;
}

// A helper class so we can efficiently apply a whole sequence
// of edits to a particular state without creating intermediate
// Versions that contain full copies of the intermediate state.
// 一个辅助类，帮助我们在不创建中间 Versions（包含中间状态的全量拷贝）的前提下
// 高效地将一系列编辑（VersionEdit） 应用到某个 VersionSet 的特定 Version 中。
class VersionSet::Builder {
 private:
  // Helper to sort by v->files_[file_number].smallest
  // 辅助类：用于基于 v->files_[file_number].smallest 进行排序
  struct BySmallestKey {
    const InternalKeyComparator* internal_comparator;

    // 先基于文件的最小 key 进行比较，谁的文 key 越小谁越大；
    // 当两个文件的最小 key 一样大的时候，谁的文件编号越小谁越大。
    bool operator()(FileMetaData* f1, FileMetaData* f2) const {
      int r = internal_comparator->Compare(f1->smallest, f2->smallest);
      if (r != 0) {
        return (r < 0);
      } else {
        // Break ties by file number
        // 当两个文件的最小 key 一样大的时候，谁的文件编号越小谁越大
        return (f1->number < f2->number);
      }
    }
  };

  // 第一个参数是 key 类型，第二个参数是比较器类型
  typedef std::set<FileMetaData*, BySmallestKey> FileSet;
  // 描述 level 的状态，包括被删除的文件编号集合和新增的文件集合
  struct LevelState {
    std::set<uint64_t> deleted_files;
    FileSet* added_files;
  };

  VersionSet* vset_;
  Version* base_;
  // 每个 level 一个
  LevelState levels_[config::kNumLevels];

 public:
  // Initialize a builder with the files from *base and other info from *vset
  // 使用 Version base 和 VersionSet vset 中其它信息初始化一个 Builder，通常 base 即为
  // vset 的 Version current_。
  Builder(VersionSet* vset, Version* base)
      : vset_(vset),
        base_(base) {
    base_->Ref();
    BySmallestKey cmp;
    cmp.internal_comparator = &vset_->icmp_;
    for (int level = 0; level < config::kNumLevels; level++) {
      levels_[level].added_files = new FileSet(cmp);
    }
  }

  ~Builder() {
    // 遍历每一层，如果新增文件集合中的文件不再使用则删除之
    for (int level = 0; level < config::kNumLevels; level++) {
      const FileSet* added = levels_[level].added_files;
      std::vector<FileMetaData*> to_unref;
      to_unref.reserve(added->size());
      // todo 不知道为啥要拷贝一份，为啥不把下面循环里的事情一起干了？
      // 可能是避免每次删除都要调整树的平衡吧（红黑数插入、删除、检索时间复杂度都为 O(logN)。
      for (FileSet::const_iterator it = added->begin();
          it != added->end(); ++it) {
        to_unref.push_back(*it);
      }
      delete added;
      for (uint32_t i = 0; i < to_unref.size(); i++) {
        FileMetaData* f = to_unref[i];
        f->refs--;
        if (f->refs <= 0) {
          delete f;
        }
      }
    }
    // 将 base_ 引用的 Version 引用计数减一
    base_->Unref();
  }

  // Apply all of the edits in *edit to the current state.
  // 将 edit 中包含的全部编辑操作导入到该 Builder 的 LevelState[] 中,
  // 后续会调用 Builder::SaveTo 方法将其与 Version base_（通常是 current Version）进行合并然后保存到新的 version 中。
  void Apply(VersionEdit* edit) {
    // Update compaction pointers
    for (size_t i = 0; i < edit->compact_pointers_.size(); i++) {
      const int level = edit->compact_pointers_[i].first;
      vset_->compact_pointer_[level] =
          edit->compact_pointers_[i].second.Encode().ToString();
    }

    // Delete files
    // 将 edit 中保存的待删除文件集合导入到 levels_[].deleted_files 中
    const VersionEdit::DeletedFileSet& del = edit->deleted_files_;
    for (VersionEdit::DeletedFileSet::const_iterator iter = del.begin();
         iter != del.end();
         ++iter) {
      const int level = iter->first;
      const uint64_t number = iter->second;
      levels_[level].deleted_files.insert(number);
    }

    // Add new files
    // 将 edit 中保存的新增文件集合导入到 levels_[].added_files 中
    for (size_t i = 0; i < edit->new_files_.size(); i++) {
      const int level = edit->new_files_[i].first;
      FileMetaData* f = new FileMetaData(edit->new_files_[i].second);
      f->refs = 1;

      // We arrange to automatically compact this file after
      // a certain number of seeks.  Let's assume:
      //   (1) One seek costs 10ms
      //   (2) Writing or reading 1MB costs 10ms (100MB/s)
      //   (3) A compaction of 1MB does 25MB of IO:
      //         1MB read from this level
      //         10-12MB read from next level (boundaries may be misaligned)
      //         10-12MB written to next level
      // This implies that 25 seeks cost the same as the compaction
      // of 1MB of data.  I.e., one seek costs approximately the
      // same as the compaction of 40KB of data.  We are a little
      // conservative and allow approximately one seek for every 16KB
      // of data before triggering a compaction.
      // 经过一定次数查询后再自动进行压实操作。我们假设：
      //    （1）一次查询消耗 10ms
      //    （2）写或者读 1MB 数据消耗 10ms（即 100MB/s）
      //    （3）1MB 数据的压实做了 25MB 数据的 IO 工作：
      //        从 level-L 读取了 1MB
      //        从 level-(L+1) 读取了 10-12MB（边界可能没有对齐）重叠数据
      //        将压实后的 10-12MB 数据写入到 level-(L+1)
      // 基于上述假设，我们可以得出，执行 25 次查询消耗的时间与压实 1MB 数据的时间相同，都是 250ms。
      // 也就是说，一次查询大约相当于压实 40KB （=1MB/25）数据。我们保守一些，就当一次查询相当于压实 16KB 数据。
      //
      // 压实之前该文件允许被查询的次数为[文件字节数/16KiB]
      f->allowed_seeks = (f->file_size / 16384);
      // 如果允许查询次数小于 100，则按 100 次处理。
      if (f->allowed_seeks < 100) f->allowed_seeks = 100;

      // todo 一个文件会同时出现在删除列表和新增列表？
      levels_[level].deleted_files.erase(f->number);
      levels_[level].added_files->insert(f);
    }
  }

  // Save the current state in *v.
  // 将 Version base_ 和 Builder 全部 LevelState 中的数据合并保存到 Version v 中
  void SaveTo(Version* v) {
    BySmallestKey cmp;
    cmp.internal_comparator = &vset_->icmp_;
    // 将每个 level 的 base_ 和 LevelState 合并到 v 的对应 level 中
    for (int level = 0; level < config::kNumLevels; level++) {
      // Merge the set of added files with the set of pre-existing files.
      // Drop any deleted files.  Store the result in *v.
      // Version base_ 中 level-L 对应的文件列表
      const std::vector<FileMetaData*>& base_files = base_->files_[level];
      std::vector<FileMetaData*>::const_iterator base_iter = base_files.begin();
      std::vector<FileMetaData*>::const_iterator base_end = base_files.end();
      // level-L 对应的新增文件集合
      const FileSet* added = levels_[level].added_files;
      // 将 Version v 中 level-L 对应的文件列表大小扩张为 Version base_ 中 level-L 对应的文件列表大小加上 level-L 对应的新增文件集合大小
      v->files_[level].reserve(base_files.size() + added->size());
      // 下面两个循环按照从小到大顺序合并 level-L 对应的 base_ 文件列表和 LevelState 新增文件列表
      for (FileSet::const_iterator added_iter = added->begin();
           added_iter != added->end();
           ++added_iter) {
        // Add all smaller files listed in base_
        // 针对每个新增文件 *added_iter，从 base_ 的 level-L 对应的当前文件列表中寻找第一个大于它的迭代器位置，寻找过程采用 cmp 比较器
        for (std::vector<FileMetaData*>::const_iterator bpos
                 = std::upper_bound(base_iter, base_end, *added_iter, cmp);
             base_iter != bpos;
             ++base_iter) {
          // 将 bpos 之前的 *base_iter 指向的文件追加到 Version v 的对应 level-L 的文件列表中
          MaybeAddFile(v, level, *base_iter);
        }

        // 将 *added_iter 追加到 Version v 的对应 level-L 的文件列表中，即 bpos 位置
        MaybeAddFile(v, level, *added_iter);
      }

      // Add remaining base files
      // 将 Version base_ 中 level-L 对应的文件列表剩余的文件追加到 Version v 的对应 level-L 的文件列表中
      for (; base_iter != base_end; ++base_iter) {
        MaybeAddFile(v, level, *base_iter);
      }

#ifndef NDEBUG
      // Make sure there is no overlap in levels > 0
      // 确保 Version v 中大于 0 的 level-L 内部文件之间不互相重叠
      if (level > 0) {
        for (uint32_t i = 1; i < v->files_[level].size(); i++) {
          // 前一个文件的最大 key
          const InternalKey& prev_end = v->files_[level][i-1]->largest;
          // 当前文件的最小 key
          const InternalKey& this_begin = v->files_[level][i]->smallest;
          // “前一个文件的最大 key”如果大于“当前文件的最小 key” 就说明发生重叠了，终止并报错
          if (vset_->icmp_.Compare(prev_end, this_begin) >= 0) {
            fprintf(stderr, "overlapping ranges in same level %s vs. %s\n",
                    prev_end.DebugString().c_str(),
                    this_begin.DebugString().c_str());
            abort();
          }
        }
      }
#endif
    }
  }

  // 将文件 f 追加到 Version v 的 level 对应的文件列表中
  void MaybeAddFile(Version* v, int level, FileMetaData* f) {
    // 如果 f 在 deleted_files 中，表示它已经被安排删除了，则什么也不做
    if (levels_[level].deleted_files.count(f->number) > 0) {
      // File is deleted: do nothing
    } else {
      std::vector<FileMetaData*>* files = &v->files_[level];
      if (level > 0 && !files->empty()) {
        // Must not overlap
        // 确保新加入的 f 与当前文件列表最后的文件（也是 smallest 和 largest 最大的文件）没有重叠
        assert(vset_->icmp_.Compare((*files)[files->size()-1]->largest,
                                    f->smallest) < 0);
      }
      // 将 f 引用计数加一并将其加入到 Version v 的文件列表中
      f->refs++;
      files->push_back(f);
    }
  }
};

VersionSet::VersionSet(const std::string& dbname,
                       const Options* options,
                       TableCache* table_cache,
                       const InternalKeyComparator* cmp)
    : env_(options->env),
      dbname_(dbname),
      options_(options),
      table_cache_(table_cache),
      icmp_(*cmp),
      next_file_number_(2),
      manifest_file_number_(0),  // Filled by Recover()
      last_sequence_(0),
      log_number_(0),
      prev_log_number_(0),
      descriptor_file_(nullptr),
      descriptor_log_(nullptr),
      dummy_versions_(this),
      current_(nullptr) {
  AppendVersion(new Version(this));
}

VersionSet::~VersionSet() {
  current_->Unref();
  assert(dummy_versions_.next_ == &dummy_versions_);  // List must be empty
  delete descriptor_log_;
  delete descriptor_file_;
}

void VersionSet::AppendVersion(Version* v) {
  // Make "v" current
  assert(v->refs_ == 0);
  assert(v != current_);
  if (current_ != nullptr) {
    current_->Unref();
  }
  current_ = v;
  // current_ 引用了 v，将 v 引用计数加一
  v->Ref();

  // Append to linked list
  // 将 v 加入到双向循环链表中
  v->prev_ = dummy_versions_.prev_;
  v->next_ = &dummy_versions_;
  v->prev_->next_ = v;
  v->next_->prev_ = v;
}

Status VersionSet::LogAndApply(VersionEdit* edit, port::Mutex* mu) {
  if (edit->has_log_number_) {
    assert(edit->log_number_ >= log_number_);
    assert(edit->log_number_ < next_file_number_);
  } else {
    edit->SetLogNumber(log_number_);
  }

  if (!edit->has_prev_log_number_) {
    edit->SetPrevLogNumber(prev_log_number_);
  }

  edit->SetNextFile(next_file_number_);
  edit->SetLastSequence(last_sequence_);

  // 新建一个 Version 用于存储 Builder 输出
  Version* v = new Version(this);
  {
    // 将当前 version 作为输入构建一个新的 Builder
    Builder builder(this, current_);
    // 将 VersionEdit 与当前  Version 内容合并
    builder.Apply(edit);
    // 将 Builder 内容输出到 Version v 中
    builder.SaveTo(v);
  }
  // 计算 Version v 下个最适合做压实的 level。
  Finalize(v);

  // Initialize new descriptor log file if necessary by creating
  // a temporary file that contains a snapshot of the current version.
  // 如有必要通过创建一个临时文件来初始化一个新的文件描述符，这个临时文件包含了当前 Version 的一个快照
  std::string new_manifest_file;
  Status s;
  if (descriptor_log_ == nullptr) {
    // No reason to unlock *mu here since we only hit this path in the
    // first call to LogAndApply (when opening the database).
    assert(descriptor_file_ == nullptr);
    new_manifest_file = DescriptorFileName(dbname_, manifest_file_number_);
    edit->SetNextFile(next_file_number_);
    s = env_->NewWritableFile(new_manifest_file, &descriptor_file_);
    if (s.ok()) {
      descriptor_log_ = new log::Writer(descriptor_file_);
      s = WriteSnapshot(descriptor_log_);
    }
  }

  // Unlock during expensive MANIFEST log write
  {
    mu->Unlock();

    // Write new record to MANIFEST log
    if (s.ok()) {
      std::string record;
      edit->EncodeTo(&record);
      s = descriptor_log_->AddRecord(record);
      if (s.ok()) {
        s = descriptor_file_->Sync();
      }
      if (!s.ok()) {
        Log(options_->info_log, "MANIFEST write: %s\n", s.ToString().c_str());
      }
    }

    // If we just created a new descriptor file, install it by writing a
    // new CURRENT file that points to it.
    if (s.ok() && !new_manifest_file.empty()) {
      s = SetCurrentFile(env_, dbname_, manifest_file_number_);
    }

    mu->Lock();
  }

  // Install the new version
  if (s.ok()) {
    AppendVersion(v);
    log_number_ = edit->log_number_;
    prev_log_number_ = edit->prev_log_number_;
  } else {
    delete v;
    if (!new_manifest_file.empty()) {
      delete descriptor_log_;
      delete descriptor_file_;
      descriptor_log_ = nullptr;
      descriptor_file_ = nullptr;
      env_->DeleteFile(new_manifest_file);
    }
  }

  return s;
}

Status VersionSet::Recover(bool *save_manifest) {
  struct LogReporter : public log::Reader::Reporter {
    Status* status;
    virtual void Corruption(size_t bytes, const Status& s) {
      if (this->status->ok()) *this->status = s;
    }
  };

  // Read "CURRENT" file, which contains a pointer to the current manifest file
  std::string current;
  Status s = ReadFileToString(env_, CurrentFileName(dbname_), &current);
  if (!s.ok()) {
    return s;
  }
  if (current.empty() || current[current.size()-1] != '\n') {
    return Status::Corruption("CURRENT file does not end with newline");
  }
  current.resize(current.size() - 1);

  std::string dscname = dbname_ + "/" + current;
  SequentialFile* file;
  s = env_->NewSequentialFile(dscname, &file);
  if (!s.ok()) {
    if (s.IsNotFound()) {
      return Status::Corruption(
            "CURRENT points to a non-existent file", s.ToString());
    }
    return s;
  }

  bool have_log_number = false;
  bool have_prev_log_number = false;
  bool have_next_file = false;
  bool have_last_sequence = false;
  uint64_t next_file = 0;
  uint64_t last_sequence = 0;
  uint64_t log_number = 0;
  uint64_t prev_log_number = 0;
  Builder builder(this, current_);

  {
    LogReporter reporter;
    reporter.status = &s;
    log::Reader reader(file, &reporter, true/*checksum*/, 0/*initial_offset*/);
    Slice record;
    std::string scratch;
    while (reader.ReadRecord(&record, &scratch) && s.ok()) {
      VersionEdit edit;
      s = edit.DecodeFrom(record);
      if (s.ok()) {
        if (edit.has_comparator_ &&
            edit.comparator_ != icmp_.user_comparator()->Name()) {
          s = Status::InvalidArgument(
              edit.comparator_ + " does not match existing comparator ",
              icmp_.user_comparator()->Name());
        }
      }

      if (s.ok()) {
        builder.Apply(&edit);
      }

      if (edit.has_log_number_) {
        log_number = edit.log_number_;
        have_log_number = true;
      }

      if (edit.has_prev_log_number_) {
        prev_log_number = edit.prev_log_number_;
        have_prev_log_number = true;
      }

      if (edit.has_next_file_number_) {
        next_file = edit.next_file_number_;
        have_next_file = true;
      }

      if (edit.has_last_sequence_) {
        last_sequence = edit.last_sequence_;
        have_last_sequence = true;
      }
    }
  }
  delete file;
  file = nullptr;

  if (s.ok()) {
    if (!have_next_file) {
      s = Status::Corruption("no meta-nextfile entry in descriptor");
    } else if (!have_log_number) {
      s = Status::Corruption("no meta-lognumber entry in descriptor");
    } else if (!have_last_sequence) {
      s = Status::Corruption("no last-sequence-number entry in descriptor");
    }

    if (!have_prev_log_number) {
      prev_log_number = 0;
    }

    MarkFileNumberUsed(prev_log_number);
    MarkFileNumberUsed(log_number);
  }

  if (s.ok()) {
    Version* v = new Version(this);
    builder.SaveTo(v);
    // Install recovered version
    Finalize(v);
    AppendVersion(v);
    manifest_file_number_ = next_file;
    next_file_number_ = next_file + 1;
    last_sequence_ = last_sequence;
    log_number_ = log_number;
    prev_log_number_ = prev_log_number;

    // See if we can reuse the existing MANIFEST file.
    if (ReuseManifest(dscname, current)) {
      // No need to save new manifest
    } else {
      *save_manifest = true;
    }
  }

  return s;
}

bool VersionSet::ReuseManifest(const std::string& dscname,
                               const std::string& dscbase) {
  if (!options_->reuse_logs) {
    return false;
  }
  FileType manifest_type;
  uint64_t manifest_number;
  uint64_t manifest_size;
  if (!ParseFileName(dscbase, &manifest_number, &manifest_type) ||
      manifest_type != kDescriptorFile ||
      !env_->GetFileSize(dscname, &manifest_size).ok() ||
      // Make new compacted MANIFEST if old one is too big
      manifest_size >= TargetFileSize(options_)) {
    return false;
  }

  assert(descriptor_file_ == nullptr);
  assert(descriptor_log_ == nullptr);
  Status r = env_->NewAppendableFile(dscname, &descriptor_file_);
  if (!r.ok()) {
    Log(options_->info_log, "Reuse MANIFEST: %s\n", r.ToString().c_str());
    assert(descriptor_file_ == nullptr);
    return false;
  }

  Log(options_->info_log, "Reusing MANIFEST %s\n", dscname.c_str());
  descriptor_log_ = new log::Writer(descriptor_file_, manifest_size);
  manifest_file_number_ = manifest_number;
  return true;
}

void VersionSet::MarkFileNumberUsed(uint64_t number) {
  if (next_file_number_ <= number) {
    next_file_number_ = number + 1;
  }
}

// 计算 Version v 下个最适合做压实的 level。
// 计算逻辑如下：
// - 针对 level-0，计算当前文件个数相对上限的比值
// - 针对其它 levels，计算每个 level 当前字节数相对于其上限的比值
// 上述比值最大的那个 level 即为下个适合进行压实的 level。
void VersionSet::Finalize(Version* v) {
  // Precomputed best level for next compaction
  // 计算最适合做压实的 level
  int best_level = -1;
  double best_score = -1;

  // 遍历每个 level，计算每个 level 的 score（当前大小相对于上限的比值），并选择最大的 score 及其 level
  for (int level = 0; level < config::kNumLevels-1; level++) {
    double score;
    if (level == 0) {
      // We treat level-0 specially by bounding the number of files
      // instead of number of bytes for two reasons:
      //
      // (1) With larger write-buffer sizes, it is nice not to do too
      // many level-0 compactions.
      //
      // (2) The files in level-0 are merged on every read and
      // therefore we wish to avoid too many files when the individual
      // file size is small (perhaps because of a small write-buffer
      // setting, or very high compression ratios, or lots of
      // overwrites/deletions).
      //
      // 我们会特殊对待 level-0，即限制文件数目而不是字节数目，原因有二：
      // （1）写缓冲空间越大，越不用去做太多的 level-0 压实。
      // （2）每次读取的时候都会对 level-0 文件做合并，因此当单个文件很小（可能因为写缓冲设置的太小或者太高的压缩比或者太多的覆写或者删除）
      // 的时候我们希望避免产生太多的文件。
      score = v->files_[level].size() /
          static_cast<double>(config::kL0_CompactionTrigger);
    } else {
      // Compute the ratio of current size to size limit.
      // 计算 level 当前大小相对于针对该 level 的字节上限（一般是 10^L MB）的比率
      const uint64_t level_bytes = TotalFileSize(v->files_[level]);
      score =
          static_cast<double>(level_bytes) / MaxBytesForLevel(options_, level);
    }

    // best_score 保存最大的 score，best_level 保存具有最大 score 的 level
    if (score > best_score) {
      best_level = level;
      best_score = score;
    }
  }

  // score 最大的那个 level 就是下个要进行压实的 level
  v->compaction_level_ = best_level;
  v->compaction_score_ = best_score;
}

Status VersionSet::WriteSnapshot(log::Writer* log) {
  // TODO: Break up into multiple records to reduce memory usage on recovery?

  // Save metadata
  VersionEdit edit;
  edit.SetComparatorName(icmp_.user_comparator()->Name());

  // Save compaction pointers
  for (int level = 0; level < config::kNumLevels; level++) {
    if (!compact_pointer_[level].empty()) {
      InternalKey key;
      key.DecodeFrom(compact_pointer_[level]);
      edit.SetCompactPointer(level, key);
    }
  }

  // Save files
  for (int level = 0; level < config::kNumLevels; level++) {
    const std::vector<FileMetaData*>& files = current_->files_[level];
    for (size_t i = 0; i < files.size(); i++) {
      const FileMetaData* f = files[i];
      edit.AddFile(level, f->number, f->file_size, f->smallest, f->largest);
    }
  }

  std::string record;
  edit.EncodeTo(&record);
  return log->AddRecord(record);
}

int VersionSet::NumLevelFiles(int level) const {
  assert(level >= 0);
  assert(level < config::kNumLevels);
  return current_->files_[level].size();
}

const char* VersionSet::LevelSummary(LevelSummaryStorage* scratch) const {
  // Update code if kNumLevels changes
  assert(config::kNumLevels == 7);
  snprintf(scratch->buffer, sizeof(scratch->buffer),
           "files[ %d %d %d %d %d %d %d ]",
           int(current_->files_[0].size()),
           int(current_->files_[1].size()),
           int(current_->files_[2].size()),
           int(current_->files_[3].size()),
           int(current_->files_[4].size()),
           int(current_->files_[5].size()),
           int(current_->files_[6].size()));
  return scratch->buffer;
}

uint64_t VersionSet::ApproximateOffsetOf(Version* v, const InternalKey& ikey) {
  uint64_t result = 0;
  for (int level = 0; level < config::kNumLevels; level++) {
    const std::vector<FileMetaData*>& files = v->files_[level];
    for (size_t i = 0; i < files.size(); i++) {
      if (icmp_.Compare(files[i]->largest, ikey) <= 0) {
        // Entire file is before "ikey", so just add the file size
        result += files[i]->file_size;
      } else if (icmp_.Compare(files[i]->smallest, ikey) > 0) {
        // Entire file is after "ikey", so ignore
        if (level > 0) {
          // Files other than level 0 are sorted by meta->smallest, so
          // no further files in this level will contain data for
          // "ikey".
          break;
        }
      } else {
        // "ikey" falls in the range for this table.  Add the
        // approximate offset of "ikey" within the table.
        Table* tableptr;
        Iterator* iter = table_cache_->NewIterator(
            ReadOptions(), files[i]->number, files[i]->file_size, &tableptr);
        if (tableptr != nullptr) {
          result += tableptr->ApproximateOffsetOf(ikey.Encode());
        }
        delete iter;
      }
    }
  }
  return result;
}

void VersionSet::AddLiveFiles(std::set<uint64_t>* live) {
  for (Version* v = dummy_versions_.next_;
       v != &dummy_versions_;
       v = v->next_) {
    for (int level = 0; level < config::kNumLevels; level++) {
      const std::vector<FileMetaData*>& files = v->files_[level];
      for (size_t i = 0; i < files.size(); i++) {
        live->insert(files[i]->number);
      }
    }
  }
}

int64_t VersionSet::NumLevelBytes(int level) const {
  assert(level >= 0);
  assert(level < config::kNumLevels);
  return TotalFileSize(current_->files_[level]);
}

int64_t VersionSet::MaxNextLevelOverlappingBytes() {
  int64_t result = 0;
  std::vector<FileMetaData*> overlaps;
  for (int level = 1; level < config::kNumLevels - 1; level++) {
    for (size_t i = 0; i < current_->files_[level].size(); i++) {
      const FileMetaData* f = current_->files_[level][i];
      current_->GetOverlappingInputs(level+1, &f->smallest, &f->largest,
                                     &overlaps);
      const int64_t sum = TotalFileSize(overlaps);
      if (sum > result) {
        result = sum;
      }
    }
  }
  return result;
}

// Stores the minimal range that covers all entries in inputs in
// *smallest, *largest.
// REQUIRES: inputs is not empty
void VersionSet::GetRange(const std::vector<FileMetaData*>& inputs,
                          InternalKey* smallest,
                          InternalKey* largest) {
  assert(!inputs.empty());
  smallest->Clear();
  largest->Clear();
  for (size_t i = 0; i < inputs.size(); i++) {
    FileMetaData* f = inputs[i];
    if (i == 0) {
      *smallest = f->smallest;
      *largest = f->largest;
    } else {
      if (icmp_.Compare(f->smallest, *smallest) < 0) {
        *smallest = f->smallest;
      }
      if (icmp_.Compare(f->largest, *largest) > 0) {
        *largest = f->largest;
      }
    }
  }
}

// Stores the minimal range that covers all entries in inputs1 and inputs2
// in *smallest, *largest.
// REQUIRES: inputs is not empty
void VersionSet::GetRange2(const std::vector<FileMetaData*>& inputs1,
                           const std::vector<FileMetaData*>& inputs2,
                           InternalKey* smallest,
                           InternalKey* largest) {
  std::vector<FileMetaData*> all = inputs1;
  all.insert(all.end(), inputs2.begin(), inputs2.end());
  GetRange(all, smallest, largest);
}

Iterator* VersionSet::MakeInputIterator(Compaction* c) {
  ReadOptions options;
  options.verify_checksums = options_->paranoid_checks;
  options.fill_cache = false;

  // Level-0 files have to be merged together.  For other levels,
  // we will make a concatenating iterator per level.
  // TODO(opt): use concatenating iterator for level-0 if there is no overlap
  const int space = (c->level() == 0 ? c->inputs_[0].size() + 1 : 2);
  Iterator** list = new Iterator*[space];
  int num = 0;
  for (int which = 0; which < 2; which++) {
    if (!c->inputs_[which].empty()) {
      if (c->level() + which == 0) {
        const std::vector<FileMetaData*>& files = c->inputs_[which];
        for (size_t i = 0; i < files.size(); i++) {
          list[num++] = table_cache_->NewIterator(
              options, files[i]->number, files[i]->file_size);
        }
      } else {
        // Create concatenating iterator for the files from this level
        list[num++] = NewTwoLevelIterator(
            new Version::LevelFileNumIterator(icmp_, &c->inputs_[which]),
            &GetFileIterator, table_cache_, options);
      }
    }
  }
  assert(num <= space);
  Iterator* result = NewMergingIterator(&icmp_, list, num);
  delete[] list;
  return result;
}

Compaction* VersionSet::PickCompaction() {
  Compaction* c;
  int level;

  // We prefer compactions triggered by too much data in a level over
  // the compactions triggered by seeks.
  const bool size_compaction = (current_->compaction_score_ >= 1);
  const bool seek_compaction = (current_->file_to_compact_ != nullptr);
  if (size_compaction) {
    level = current_->compaction_level_;
    assert(level >= 0);
    assert(level+1 < config::kNumLevels);
    c = new Compaction(options_, level);

    // Pick the first file that comes after compact_pointer_[level]
    for (size_t i = 0; i < current_->files_[level].size(); i++) {
      FileMetaData* f = current_->files_[level][i];
      if (compact_pointer_[level].empty() ||
          icmp_.Compare(f->largest.Encode(), compact_pointer_[level]) > 0) {
        c->inputs_[0].push_back(f);
        break;
      }
    }
    if (c->inputs_[0].empty()) {
      // Wrap-around to the beginning of the key space
      c->inputs_[0].push_back(current_->files_[level][0]);
    }
  } else if (seek_compaction) {
    level = current_->file_to_compact_level_;
    c = new Compaction(options_, level);
    c->inputs_[0].push_back(current_->file_to_compact_);
  } else {
    return nullptr;
  }

  c->input_version_ = current_;
  c->input_version_->Ref();

  // Files in level 0 may overlap each other, so pick up all overlapping ones
  if (level == 0) {
    InternalKey smallest, largest;
    GetRange(c->inputs_[0], &smallest, &largest);
    // Note that the next call will discard the file we placed in
    // c->inputs_[0] earlier and replace it with an overlapping set
    // which will include the picked file.
    current_->GetOverlappingInputs(0, &smallest, &largest, &c->inputs_[0]);
    assert(!c->inputs_[0].empty());
  }

  SetupOtherInputs(c);

  return c;
}

void VersionSet::SetupOtherInputs(Compaction* c) {
  const int level = c->level();
  InternalKey smallest, largest;
  GetRange(c->inputs_[0], &smallest, &largest);

  current_->GetOverlappingInputs(level+1, &smallest, &largest, &c->inputs_[1]);

  // Get entire range covered by compaction
  InternalKey all_start, all_limit;
  GetRange2(c->inputs_[0], c->inputs_[1], &all_start, &all_limit);

  // See if we can grow the number of inputs in "level" without
  // changing the number of "level+1" files we pick up.
  if (!c->inputs_[1].empty()) {
    std::vector<FileMetaData*> expanded0;
    current_->GetOverlappingInputs(level, &all_start, &all_limit, &expanded0);
    const int64_t inputs0_size = TotalFileSize(c->inputs_[0]);
    const int64_t inputs1_size = TotalFileSize(c->inputs_[1]);
    const int64_t expanded0_size = TotalFileSize(expanded0);
    if (expanded0.size() > c->inputs_[0].size() &&
        inputs1_size + expanded0_size <
            ExpandedCompactionByteSizeLimit(options_)) {
      InternalKey new_start, new_limit;
      GetRange(expanded0, &new_start, &new_limit);
      std::vector<FileMetaData*> expanded1;
      current_->GetOverlappingInputs(level+1, &new_start, &new_limit,
                                     &expanded1);
      if (expanded1.size() == c->inputs_[1].size()) {
        Log(options_->info_log,
            "Expanding@%d %d+%d (%ld+%ld bytes) to %d+%d (%ld+%ld bytes)\n",
            level,
            int(c->inputs_[0].size()),
            int(c->inputs_[1].size()),
            long(inputs0_size), long(inputs1_size),
            int(expanded0.size()),
            int(expanded1.size()),
            long(expanded0_size), long(inputs1_size));
        smallest = new_start;
        largest = new_limit;
        c->inputs_[0] = expanded0;
        c->inputs_[1] = expanded1;
        GetRange2(c->inputs_[0], c->inputs_[1], &all_start, &all_limit);
      }
    }
  }

  // Compute the set of grandparent files that overlap this compaction
  // (parent == level+1; grandparent == level+2)
  if (level + 2 < config::kNumLevels) {
    current_->GetOverlappingInputs(level + 2, &all_start, &all_limit,
                                   &c->grandparents_);
  }

  // Update the place where we will do the next compaction for this level.
  // We update this immediately instead of waiting for the VersionEdit
  // to be applied so that if the compaction fails, we will try a different
  // key range next time.
  compact_pointer_[level] = largest.Encode().ToString();
  c->edit_.SetCompactPointer(level, largest);
}

Compaction* VersionSet::CompactRange(
    int level,
    const InternalKey* begin,
    const InternalKey* end) {
  std::vector<FileMetaData*> inputs;
  current_->GetOverlappingInputs(level, begin, end, &inputs);
  if (inputs.empty()) {
    return nullptr;
  }

  // Avoid compacting too much in one shot in case the range is large.
  // But we cannot do this for level-0 since level-0 files can overlap
  // and we must not pick one file and drop another older file if the
  // two files overlap.
  if (level > 0) {
    const uint64_t limit = MaxFileSizeForLevel(options_, level);
    uint64_t total = 0;
    for (size_t i = 0; i < inputs.size(); i++) {
      uint64_t s = inputs[i]->file_size;
      total += s;
      if (total >= limit) {
        inputs.resize(i + 1);
        break;
      }
    }
  }

  Compaction* c = new Compaction(options_, level);
  c->input_version_ = current_;
  c->input_version_->Ref();
  c->inputs_[0] = inputs;
  SetupOtherInputs(c);
  return c;
}

Compaction::Compaction(const Options* options, int level)
    : level_(level),
      max_output_file_size_(MaxFileSizeForLevel(options, level)),
      input_version_(nullptr),
      grandparent_index_(0),
      seen_key_(false),
      overlapped_bytes_(0) {
  for (int i = 0; i < config::kNumLevels; i++) {
    level_ptrs_[i] = 0;
  }
}

Compaction::~Compaction() {
  if (input_version_ != nullptr) {
    input_version_->Unref();
  }
}

bool Compaction::IsTrivialMove() const {
  const VersionSet* vset = input_version_->vset_;
  // Avoid a move if there is lots of overlapping grandparent data.
  // Otherwise, the move could create a parent file that will require
  // a very expensive merge later on.
  return (num_input_files(0) == 1 && num_input_files(1) == 0 &&
          TotalFileSize(grandparents_) <=
              MaxGrandParentOverlapBytes(vset->options_));
}

void Compaction::AddInputDeletions(VersionEdit* edit) {
  for (int which = 0; which < 2; which++) {
    for (size_t i = 0; i < inputs_[which].size(); i++) {
      edit->DeleteFile(level_ + which, inputs_[which][i]->number);
    }
  }
}

bool Compaction::IsBaseLevelForKey(const Slice& user_key) {
  // Maybe use binary search to find right entry instead of linear search?
  const Comparator* user_cmp = input_version_->vset_->icmp_.user_comparator();
  for (int lvl = level_ + 2; lvl < config::kNumLevels; lvl++) {
    const std::vector<FileMetaData*>& files = input_version_->files_[lvl];
    for (; level_ptrs_[lvl] < files.size(); ) {
      FileMetaData* f = files[level_ptrs_[lvl]];
      if (user_cmp->Compare(user_key, f->largest.user_key()) <= 0) {
        // We've advanced far enough
        if (user_cmp->Compare(user_key, f->smallest.user_key()) >= 0) {
          // Key falls in this file's range, so definitely not base level
          return false;
        }
        break;
      }
      level_ptrs_[lvl]++;
    }
  }
  return true;
}

bool Compaction::ShouldStopBefore(const Slice& internal_key) {
  const VersionSet* vset = input_version_->vset_;
  // Scan to find earliest grandparent file that contains key.
  const InternalKeyComparator* icmp = &vset->icmp_;
  while (grandparent_index_ < grandparents_.size() &&
      icmp->Compare(internal_key,
                    grandparents_[grandparent_index_]->largest.Encode()) > 0) {
    if (seen_key_) {
      overlapped_bytes_ += grandparents_[grandparent_index_]->file_size;
    }
    grandparent_index_++;
  }
  seen_key_ = true;

  if (overlapped_bytes_ > MaxGrandParentOverlapBytes(vset->options_)) {
    // Too much overlap for current output; start new output
    overlapped_bytes_ = 0;
    return true;
  } else {
    return false;
  }
}

void Compaction::ReleaseInputs() {
  if (input_version_ != nullptr) {
    input_version_->Unref();
    input_version_ = nullptr;
  }
}

}  // namespace leveldb

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

// 目标文件大小(leveldb 最多写入 max_file_size 个字节到一个
// 文件中, 超过这个值就会关闭当前文件然后写下一个新的文件)
static size_t TargetFileSize(const Options* options) {
  return options->max_file_size;
}

// Maximum bytes of overlaps in grandparent (i.e., level+2) before we
// stop building a single file in a level->level+1 compaction.
// 压实过程: 
//    当 level-L 大小超过了上限, 我们就在后台线程中将其压实. 
//    压实过程会从 level-L 挑一个文件, 然后将 level-(L+1) 中与该文件键区间重叠的文件都找出来. 
// 一次压实会合并多个被挑选文件的内容从而生成一系列新的 level-(L+1) 文件, 生成一个新文件的条件有两个: 
//    - 当前文件大小达到了 2MB
//    - 当前文件的键区间与不超过 10 个 level-(L+2) 文件发生了重叠.
// 第二个条件的目的在于避免后续对 level-(L+1) 文件进行压实时需要从 level-(L+2) 读取过多的数据. 
//
// 下面这个方法是上述第二个条件限制的实现, 它返回的是 10 个文件的大小, 达到这个大小表示达到了 10 个文件. 
static int64_t MaxGrandParentOverlapBytes(const Options* options) {
  return 10 * TargetFileSize(options);
}

// 所有被压缩的文件中的最大字节数.
// 如果扩大压实的下层文件集会使总的压实覆盖超过这个字节数, 我们会避免扩大.
static int64_t ExpandedCompactionByteSizeLimit(const Options* options) {
  return 25 * TargetFileSize(options);
}

// 除了 level-0 以外, 每个 level 允许的最大字节数. 
static double MaxBytesForLevel(const Options* options, int level) {
  // Note: the result for level zero is not really used since we set
  // the level-0 compaction threshold based on number of files.
  // 注意: 这里的 result 对 level-0 没有用处, 
  // 因为针对 level-0 的压实阈值基于文件个数, 默认是超过 4 个就触发. 

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

// 返回指定 level 最大文件大小, 当前每一层文件大小都是默认 2MB
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
      f->refs--; // 每个文件元数据被某个 version 引用的时候它对应的计数会加一, 因为该 version 要销毁了, 所以这里将其减一
      if (f->refs <= 0) {
        delete f; // 如果没有任何 version 引用该文件元数据, 则将其所占空间释放
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
      // 如果 mid 小于 key, 那 [0, mid] 都不满足条件
      left = mid + 1;
    } else {
      // Key at "mid.largest" is >= "target".  Therefore all files
      // after "mid" are uninteresting.
      right = mid; // 如果 mid 大于等于 key, 那么 (mid, files.size()) 都不满足条件
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
  // 如果 files 中的文件之间相交但相互之间有序, 
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
  // 如果各个文件之间无交集, 则可以使用二分法来查找(文件内部保证有序)
  uint32_t index = 0;
  if (smallest_user_key != nullptr) {
    // Find the earliest possible internal key for smallest_user_key
    // 构建 smallest_user_key 对应的 internal_key, 即使用 smallest_user_key 和最大的序列号、最大的操作类型进行拼接
    InternalKey small(*smallest_user_key, kMaxSequenceNumber, kValueTypeForSeek);
    // 在 files 中寻找最大 key 大于等于 smallest_user_key 的第一个文件.
    // 注意这里查的是第一个, 因为后面还要比较 largest_user_key 与该文件最小 key 的关系, 
    // 如果该文件最小 key 都大于 largest_user_key, 
    // 那么无疑后面的文件最小 key 肯定也都大于 largest_user_key. 
    index = FindFile(icmp, files, small.Encode());
  }

  if (index >= files.size()) { // 不存在这样的文件
    // beginning of range is after all files, so no overlap.
    return false;
  }

  // 到这说明 files[index].largest >= smallest_user_key, 
  // 需要确认 files[index].smallest <= largest_user_key 是否成立, 如果成立则相交. 
  // 为啥不是检查 largest_user_key <= files[index].largest? 
  // 是为了避免 files[index].largest > smallest_user_key 且 files[index].largest > largest_user_key, 
  // 这样看不出来是否相交. 

  // 如果存在这样的文件, 检查 largest_user_key 是否不小于该文件最小的 key. 
  // 前面确认了 files[index].largest >= smallest_user_key, 这是相交成立的第一个条件, 
  // 如果 largest_user_key >= files[index].smallest, 
  // 而且已知 largest_user_key >= smallest_user_key, 
  // 则说明 [smallest, largest] 与 [files[index].smallest, files[index].largest] 相交. 
  return !BeforeFile(ucmp, largest_user_key, files[index]);
}

// An internal iterator.  For a given version/level pair, yields
// information about the files in the level.  For a given entry, key()
// is the largest key that occurs in the file, and value() is an
// 16-byte value containing the file number and file size, both
// encoded using EncodeFixed64.
//
// 一个迭代器, 迭代的是 flist 文件列表
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
  // 将 index_ 移动到 flist 中第一个最大 key 大于等于 target 的文件号
  virtual void Seek(const Slice& target) {
    index_ = FindFile(icmp_, *flist_, target);
  }
  virtual void SeekToFirst() { index_ = 0; }
  virtual void SeekToLast() {
    index_ = flist_->empty() ? 0 : flist_->size() - 1; // 如果 flist 为空, 赋值为 0
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
  // value 为 index_ 指向文件的 number 和 file_size 组合, 这个组合相当于一个目标数据块. 
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
  uint32_t index_; // flist 当前索引即为该迭代器的底层表示, 移动迭代器即移动它

  // Backing store for value().  Holds the file number and size.
  // value() 方法的底层存储, 保存着 file number 和 file size, 这两者都是 varint64 格式. 
  // 改成员为 mutable 形式, 表示可以在 const 方法中对其进行修改. 
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

// 构造指定 level 层文件列表的双层迭代器: 
// - 第一层迭代器(LevelFileNumIterator)指向文件; 
// - 第二层迭代器指向某个 table 文件具体内容, 其实它也是一个双层迭代器. 
Iterator* Version::NewConcatenatingIterator(const ReadOptions& options,
                                            int level) const {
  return NewTwoLevelIterator(
      // 第一个参数是 level 级别的迭代器, 它相当于索引迭代器, 在这个级别找的是包含目标 key 的文件号;
      // 文件相当于数据块, 再找就是用文件内容的迭代器, 这个是第二级.
      new LevelFileNumIterator(vset_->icmp_, &files_[level]),
      // GetFileIterator 负责根据定位到的文件号及其大小, 
      // 从 table_cache_ 找到对应 table, 然后返回后者的迭代器
      &GetFileIterator, vset_->table_cache_, options);
}

// 将当前 version 维护的 level 架构中每一个 sorted string table 文件对应的迭代器
// 追加到 iters 向量里, 这些迭代器加上两个 memtable 的迭代器, 就能
// 遍历整个数据库的内容了. 
// 前提: 当前 Version 对象事先已经通过 VersionSet::SaveTo 方法被保存过了. 
void Version::AddIterators(const ReadOptions& options,
                           std::vector<Iterator*>* iters) {
  // Merge all level zero files together since they may overlap
  // 将 level-0 文件合并到一起, 因为它们互相之间可能有重叠. 
  // 合并过程就是为各个 table 文件生成相应的两级迭代器, 然后将各个迭代器放入 *iters.
  // 注意这里是按照从小到达顺序进行追加的, 这样虽然部分重叠, 但是整体有序.
  for (size_t i = 0; i < files_[0].size(); i++) {
    iters->push_back(
        // 针对给定的 file_number(对应的文件长度也必须恰好是 file_size 字节数), 
        // 返回一个与其对应 table 的 iterator. 
        // 如果 tableptr 参数非空, 设置 *tableptr 指向返回的 iterator 底下的 Table 对象. 
        // 返回的 *tableptr 对象由 cache 所拥有, 所以用户不要删除它; 而且只要 iterator 还活着, 该对象就有效. 
        vset_->table_cache_->NewIterator(
            options, files_[0][i]->number, files_[0][i]->file_size));
  }

  // For levels > 0, we can use a concatenating iterator that sequentially
  // walks through the non-overlapping files in the level, opening them
  // lazily.
  // level-1 及其以上, 为每一个 level 生成一个级联迭代器
  // (本质也是一个两级迭代器具体见 Version::NewConcatenatingIterator, 
  //    level-1 及其之上, 每一层内部, 文件不会发生重叠)放入 *iters
  // 注意这里是从低 level 到高 level 追加的, 这样可以保证整体有序.
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

// 如果 arg (其实是个 Saver)中保存的 key 与 ikey 相等, 
// 且 ikey 对应的 tag 不表示删除, 则将
// 与 ikey 对应的 value 保存到 arg 对应成员中. 
static void SaveValue(void* arg, const Slice& ikey, const Slice& v) {
  Saver* s = reinterpret_cast<Saver*>(arg);
  ParsedInternalKey parsed_key;
  if (!ParseInternalKey(ikey, &parsed_key)) {
    s->state = kCorrupt;
  } else {
    if (s->ucmp->Compare(parsed_key.user_key, s->user_key) == 0) {
      // 因为 leveldb 的删除也是一种写操作, 所以要检查 key 的 type
      s->state = (parsed_key.type == kTypeValue) ? kFound : kDeleted;
      if (s->state == kFound) {
        // 将对应的 value 赋值到 saver 对应成员中
        s->value->assign(v.data(), v.size());
      }
    }
  }
}

// 比较 a 和 b 谁的 file_number 更大, 大的那个文件比较新
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
   * 先处理 level-0, 这一层比较特殊, 因为文件之间可能存在重叠. 
   */
  // Search level-0 in order from newest to oldest.
  std::vector<FileMetaData*> tmp;
  // 数目确定, 为 tmp 一次性分配空间, 避免后续的重分配导致耗时
  tmp.reserve(files_[0].size());
  // 顺序搜索 level-0 文件寻找与 user_key 重叠的文件
  for (uint32_t i = 0; i < files_[0].size(); i++) {
    FileMetaData* f = files_[0][i];
    if (ucmp->Compare(user_key, f->smallest.user_key()) >= 0 &&
        ucmp->Compare(user_key, f->largest.user_key()) <= 0) {
      // 如果 user_key 落在了 f 文件范围内, 则将 f 加入 tmp
      tmp.push_back(f);
    }
  }
  if (!tmp.empty()) {
    // 将与 user_key 重叠的文件进行排序, 从最新到最旧
    std::sort(tmp.begin(), tmp.end(), NewestFirst);
    for (uint32_t i = 0; i < tmp.size(); i++) {
      // 按照文件最新到最旧调用 func, 直至 func 返回 false. 
      if (!(*func)(arg, 0, tmp[i])) {
        return;
      }
    }
  }

  /**
   * 处理其它 levels, 因为除了 level-0 其它 level 内部文件都不存在重叠(而且还是有序的)
   */
  // Search other levels.
  for (int level = 1; level < config::kNumLevels; level++) {
    size_t num_files = files_[level].size();
    // 有的 level 可能为空
    if (num_files == 0) continue;

    // Binary search to find earliest index whose largest key >= internal_key.
    // 通过二分查找找到第一个大于等于 internal_key 的文件, 注意, user_key 与 internal_key 用户部分是相同的. 
    uint32_t index = FindFile(vset_->icmp_, files_[level], internal_key);
    // 找到了一个可能与 user_key 重叠的文件
    if (index < num_files) {
      FileMetaData* f = files_[level][index];
      // 这个判断确保 user_key 与 f 文件重叠
      if (ucmp->Compare(user_key, f->smallest.user_key()) < 0) {
        // All of "f" is past any data for user_key
        // 不重叠
      } else {
        // 存在重叠, 则在该文件上调用 func
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

  // 我们采用从底向上 level-by-level 的寻找. 
  // 由于 level 越低数据越新, 因此, 当我们在一个较低的 level 
  // 找到数据的时候, 不用在更高的 levels 找了. 
  std::vector<FileMetaData*> tmp;
  FileMetaData* tmp2;
  // 逐 level 查询
  for (int level = 0; level < config::kNumLevels; level++) {
    // 第 level 层文件总数
    size_t num_files = files_[level].size(); 
    // 空 level, 跳过
    if (num_files == 0) continue; 

    // 获取 level 层的文件列表.
    // 注意下面这个指针类型, 指针可以改但指针指向内容不能改, 避免下面误操作. 
    // 而且还要注意, 这里利用 vector 存储连续的特点直接
    // 采用指针遍历 vector, 而不是采用 vector 内置迭代器方式进行. 
    FileMetaData* const* files = &files_[level][0];
    // level-0 比较特殊, 因为它的文件之间可能互相重叠, 所以需要单独处理. 
    // 找到全部可能包含 user_key 的文件, 然后从最新到最旧顺序进行处理.
    if (level == 0) {
      // 已知存储上限, 预分配, 避免后续重分配消耗性能
      tmp.reserve(num_files); 
      for (uint32_t i = 0; i < num_files; i++) {
        // 遍历 level-0 全部文件, 找出包含 user_key 的文件
        FileMetaData* f = files[i];
        if (ucmp->Compare(user_key, f->smallest.user_key()) >= 0 &&
            ucmp->Compare(user_key, f->largest.user_key()) <= 0) {
          // 将可能包含 user_key 的文件加入到临时存储
          tmp.push_back(f);
        }
      }
      // level-0 没有文件可能包含 user_key, 返回继续处理下一层
      if (tmp.empty()) continue;

      // level-0 有文件可能包含 user_key

      // 按照 file number 对文件进行从最新到最旧排序.
      // 排序的原因是 level-0 文件之间可能存在重叠, 针对相同 key 如果
      // 存在不同数据, 那么后加入的数据才是有效的.
      std::sort(tmp.begin(), tmp.end(), NewestFirst);
      // files 指向可能结果, 后面集中处理 files
      files = &tmp[0];
      // 可能包含 user_key 的文件总个数
      num_files = tmp.size(); 
    } else {
      // 先找可能包含目标 key 的文件: 
      // 在该层采用二分查找定位那个满足最大 key >= ikey 的第一个文件的索引
      uint32_t index = FindFile(vset_->icmp_, files_[level], ikey);
      // 没找到文件
      if (index >= num_files) { 
        files = nullptr;
        num_files = 0;
      } else {
        // 找到文件了, 再在文件内确认目标 key 是否存在
        tmp2 = files[index];
        // 不在
        if (ucmp->Compare(user_key, tmp2->smallest.user_key()) < 0) {
          // All of "tmp2" is past any data for user_key
          files = nullptr;
          num_files = 0;
        } else {
          // 可能存在
          files = &tmp2;
          num_files = 1;
        }
      }
    }

    // 遍历可能包含目标 key 的每一个文件(如果是在 level-0 找到的, 
    // 那可能存在多个文件; 如果是其它 level, 只会是一个)
    for (uint32_t i = 0; i < num_files; ++i) {
      if (last_file_read != nullptr && stats->seek_file == nullptr) {
        // We have had more than one seek for this read.  Charge the 1st file.
        // 如果针对某个 user_key, 需要查询多个文件(从底向上逐个 level 扫描, 下面的因为和上面的键空间重叠都是疑似包含),
        // 做了不必要的查询, 那这就是要进行文件压实的信号, 从下向上合并文件尽量消除键空间重叠,
        // 这可以加速后续的查询. 这就要求我们记录下第一个疑似包含 user_key 的文件及其 level.
        // 后续会进行压实处理.
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
      // sstable 文件 f 对应的 table 文件可能已经在 cache 中了
      // (不在的话读取后也会加入 cache), 
      // 从该文件中查找有无 internal_key 为 ikey 的数据项, 
      // 如果找到, 则调用 SaveValue 将
      // 对应的 value 保存到 saver 数据结构中. 
      s = vset_->table_cache_->Get(options, f->number, f->file_size,
                                   ikey, &saver, SaveValue);
      if (!s.ok()) {
        return s;
      }
      switch (saver.state) {
        case kNotFound:
          // 未在该文件中找到, 继续查找下个文件
          break; 
        case kFound:
          // 找到了, 返回
          return s;
        case kDeleted:
          // 被删除了, 返回
          // 这也是为啥上面说从最新文件向最旧文件查找, 因为 leveldb 的增和删都是一种插入操作, 
          // 如果一个文件先增后删, 删除操作对应的插入肯定晚于增加操作. 
          s = Status::NotFound(Slice());  // Use empty error message for speed
          return s;
        case kCorrupt:
          // 文件损坏了, 返回
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
    // 该文件被查询过, 其允许查询的次数减一
    f->allowed_seeks--;
    // 如果该文件查询次数达到上限, 且当前没有待压实文件, 则将该文件设置为待压实文件
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
  // 在每个与 user_key 重叠的文件上运行上面定义的 Match 方法, 
  // 并将第一个重叠的文件记录到 state 中, 其它重叠文件只是在 state 中计数. 
  ForEachOverlapping(ikey.user_key, internal_key, &state, &State::Match);

  // Must have at least two matches since we want to merge across
  // files. But what if we have a single file that contains many
  // overwrites and deletions?  Should we have another mechanism for
  // finding such files?
  //
  // 至少有两次匹配(即至少存在两个文件的范围与 user_key 重叠)我们才去合并文件即压实. 
  // 但是如果存在单个文件包含很多复写和删除怎么办? 我们应该为发现此类文件建立另外的机制吗? 
  if (state.matches >= 2) {
    // 1MB cost is about 1 seek (see comment in Builder::Apply).
    // 一次查询大约消耗 1MB(具体见 Builder::Apply 方法注释)
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

// 检查指定的 level 是否与 [*smallest, *largest] 区间相交. 
// 当且仅当指定 level 的某个文件与 [*smallest, *largest] 指定的 user key 区间重叠返回 true; 
// 意思是 [*smallest, *largest] 与指定的 level 有重叠. 
// smallest==nullptr 表示小于 DB 中全部 keys 的 key. 
// largest==nullptr 表示大于 DB 中全部 keys 的 key. 
bool Version::OverlapInLevel(int level,
                             const Slice* smallest_user_key,
                             const Slice* largest_user_key) {
  // (level > 0) 表示 level-0 以上的 level 内部文件之间才不会有重叠(有序当然是默认成立的)
  return SomeFileOverlapsRange(vset_->icmp_, (level > 0), files_[level],
                               smallest_user_key, largest_user_key);
}

// 该方法负责为一个 memtable 在当前 level 架构(保存在当前 version 中)找一个落脚的 level.
// 如果该 memtable 与 level-0 文件有重叠, 则放到 level-0; 否则, 它的判断条件就从从 level-1 开始寻找,
// 主要是借用了压实磁盘 level 某个文件时生成新文件的判断条件之二, 即
// "在合并 level-L 和 level-(L+1) 文件时生成新文件要满足两个条件:
// 条件一是达到了 2MB, 条件二是如果 level-L 和 level-(L+2) 重叠文件超过 10 个".
//
// 一个 memtable 对应一个 [smallest_user_key,largest_user_key] 区间,
// 我们将该 memtable 构造成一个 sstable 文件后, 需要为该文件寻找一个落脚的 level.
// 该方法所作的即是依据与区间 [smallest_user_key,largest_user_key] 的重叠情况获取可以存储对应 sstable 文件的 level.
// 具体选取过程与压实策略有关.
int Version::PickLevelForMemTableOutput(
    const Slice& smallest_user_key,
    const Slice& largest_user_key) {
  int level = 0;
  // 检查 level-0 是否有文件与 [smallest_user_key, largest_user_key] 有重叠, 
  // 如果存在重叠, 返回 level-0; 否则进一步检查其它 levels, 最后不管选中谁, 这个目标 level 和 sstable 文件肯定没有重叠.
  if (!OverlapInLevel(0, &smallest_user_key, &largest_user_key)) {
    // 接下来就是尽量把 sstable 文件要落脚的目的地 level 尽量上推, 只要满足两个条件:
    // 1. sstable 文件和 next level 没有重叠.
    // 2. sstable 和 next of next level 重叠字节数不超过阈值.
    //
    // 注意, 此时 level 取值为 0.
    //
    // 分别构建 smallest_user_key 和 largest_user_key 对应的 internal_key. 
    // 针对 internal_key, user_key 部分越大越大, 序列号越小越大, 类型越小越大. 
    InternalKey start(smallest_user_key, kMaxSequenceNumber, kValueTypeForSeek);
    InternalKey limit(largest_user_key, 0, static_cast<ValueType>(0));
    std::vector<FileMetaData*> overlaps;
    // 压实过程: 
    //    当 level-L 大小超过了上限, 我们就在后台线程中将其压实. 
    //    压实过程会从 level-L 挑一个文件, 然后将 level-(L+1) 中与该文件键区间重叠的文件都找出来. 
    // 一次压实会合并多个被挑选文件的内容从而生成一系列新的 level-(L+1) 文件, 生成一个新文件的条件有两个: 
    //    - 当前文件大小达到了 2MB
    //    - 当前文件的键区间与超过 10 个 level-(L+2) 文件发生了重叠. 
    // 第二个条件的目的在于避免后续对 level-(L+1) 文件进行压实时需要从 level-(L+2) 读取过多的数据. 
    while (level < config::kMaxMemCompactLevel) {
      // 检查 level-(L+1) 中是否存在与 [smallest_user_key, largest_user_key] 重叠的文件
      if (OverlapInLevel(level + 1, &smallest_user_key, &largest_user_key)) {
        // 如果 level-(L+1) 中存在与 [smallest_user_key, largest_user_key] 重叠的文件则跳出循环, 返回该 level
        break;
      }
      // 如果 level-(L+1) 中不存在与 [smallest_user_key, largest_user_key] 重叠的文件, 
      // 则检查 level-(L+2) 是否存在过多(过多即是看重叠文件个数是否超出阈值)与
      // [smallest_user_key, largest_user_key] 重叠的文件.
      // 如果重叠的文件数超过阈值, 则表示 level-L 需要进行压实了.
      if (level + 2 < config::kNumLevels) {
        // Check that file does not overlap too many grandparent bytes.
        // 获取 level-(L+2) 中与 [smallest_user_key, largest_user_key] 有重叠的全部文件, 
        // 并保存到 overlaps. 
        GetOverlappingInputs(level + 2, &start, &limit, &overlaps);
        // 计算 level-(L+2) 中与 [smallest_user_key, largest_user_key] 有重叠的全部文件的总大小
        const int64_t sum = TotalFileSize(overlaps);
        // 如果大小超过了阈值, 表示 level-(L+2) 存在过多与 [smallest_user_key, largest_user_key] 重叠的文件, 
        // 则跳出循环返回 level-L. 
        if (sum > MaxGrandParentOverlapBytes(vset_->options_)) {
          break;
        }
      }
      level++;
    }
  }
  return level;
}

// 获取指定 level 中与区间 [begin,end] 有重叠的全部文件保存到 "*inputs"
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
  // 遍历 level 层每一个文件, 检查是否与 [user_begin,user_end] 有重叠
  for (size_t i = 0; i < files_[level].size(); ) {
    FileMetaData* f = files_[level][i++];
    const Slice file_start = f->smallest.user_key();
    const Slice file_limit = f->largest.user_key();
    if (begin != nullptr && user_cmp->Compare(file_limit, user_begin) < 0) {
      // "f" is completely before specified range; skip it
      // f 最大 key 小于 user_begin, 则肯定无交集, 跳过. 
    } else if (end != nullptr && user_cmp->Compare(file_start, user_end) > 0) {
      // "f" is completely after specified range; skip it
      // f 最小 key 大于 user_end, 则肯定无交集, 跳过. 
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
    // 每个 level 输出就是下面这个形式, 
    // 17 表示的 file number, 123 表示的 file size, 
    // 中括号第一个值表示该 file 最小的 internal_key, 然后省略号, 第二个值表示该 file 最大的 internal_key
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

// 辅助类, 与 VerisonEdit 配套.
// 帮助我们在不创建中间 Versions(包含中间状态的全量拷贝)的前提下
// 高效地将一系列增量更新(VersionEdit) 叠加到当前 Version.
class VersionSet::Builder {
 private:
  // 辅助类: 基于 v->files_[file_number].smallest 进行排序
  struct BySmallestKey {
    const InternalKeyComparator* internal_comparator;

    // 先基于文件的最小 key 进行比较, 谁的最小 key 更小谁排前面; 
    // 当两个文件的最小 key 一样大的时候, 谁的文件编号更小谁排前面. 
    bool operator()(FileMetaData* f1, FileMetaData* f2) const {
      int r = internal_comparator->Compare(f1->smallest, f2->smallest);
      if (r != 0) {
        return (r < 0);
      } else {
        // 当两个文件的最小 key 一样大的时候,
        // 谁的文件编号更小谁排前面
        return (f1->number < f2->number);
      }
    }
  };

  // 第一个参数是 key 类型, 第二个参数是比较器类型
  typedef std::set<FileMetaData*, BySmallestKey> FileSet;
  // 描述 level 的状态, 包括被删除的文件编号集合和新增的文件集合
  struct LevelState {
    std::set<uint64_t> deleted_files;
    FileSet* added_files;
  };

  VersionSet* vset_;
  // 该值会被 VersionSet 初始化为其当前 version
  Version* base_;
  // 覆盖整个 level 架构, 每个 level 一个状态(包含待删除文件列表和新增文件列表)
  LevelState levels_[config::kNumLevels];

 public:
  // Initialize a builder with the files from *base and other info from *vset
  // 使用 Version base 和 VersionSet vset 中其它信息初始化一个 Builder, 通常 base 即为
  // vset 的 Version current_. 
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
    // 遍历每一层, 如果新增文件集合中的文件不再使用则删除之
    for (int level = 0; level < config::kNumLevels; level++) {
      const FileSet* added = levels_[level].added_files;
      std::vector<FileMetaData*> to_unref;
      to_unref.reserve(added->size());
      // todo 不知道为啥要拷贝一份, 为啥不把下面循环里的事情一起干了? 
      // 可能是避免每次删除都要调整树的平衡吧(红黑数插入、删除、检索时间复杂度都为 O(logN). 
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

  // 将 edit 中包含的针对文件的新增和删除操作导入当前 Builder 中,
  // 后续会调用 Builder::SaveTo 方法将其与 VersionSet 中当前 version
  // 进行合并然后生成新的 version. 
  void Apply(VersionEdit* edit) {
    // 更新压实指针信息
    // 将 edit 中保存的每一层下次压实起始 key 复制到 VersionSet 中
    for (size_t i = 0; i < edit->compact_pointers_.size(); i++) {
      const int level = edit->compact_pointers_[i].first;
      // 与下面新增和删除不同, 这里直接修改 vset
      vset_->compact_pointer_[level] =
          edit->compact_pointers_[i].second.Encode().ToString();
    }

    // 删除文件
    // 将 edit 中保存的待删除文件集合导入到 levels_[].deleted_files 中
    const VersionEdit::DeletedFileSet& del = edit->deleted_files_;
    for (VersionEdit::DeletedFileSet::const_iterator iter = del.begin();
         iter != del.end();
         ++iter) {
      const int level = iter->first;
      const uint64_t number = iter->second;
      levels_[level].deleted_files.insert(number);
    }

    // 添加新文件
    // 将 edit 中保存的新增文件集合导入到 levels_[].added_files 中
    for (size_t i = 0; i < edit->new_files_.size(); i++) {
      // pair 第一个参数为 level
      const int level = edit->new_files_[i].first;
      // pair 第二个参数为 FileMetaData
      FileMetaData* f = new FileMetaData(edit->new_files_[i].second);
      f->refs = 1;

      // leveldb 针对经过一定查询次数的文件进行自动压实. 我们假设:
      //    (1)一次查询消耗 10ms
      //    (2)写或者读 1MB 数据消耗 10ms(即 100MB/s, 这是一般磁盘 IO 速度)
      //    (3)1MB 数据的压实做了 25MB 数据的 IO 工作: 
      //        从 level-L 读取了 1MB
      //        从 level-(L+1) 读取了 10-12MB(边界可能没有对齐)重叠数据
      //        将压实后的 10-12MB 数据写入到 level-(L+1)
      // 基于上述假设, 我们可以得出, 执行 25 次查询消耗的时间与压实 1MB 数据
      // 的时间相同, 都是 250ms. 也就是说, 一次查询大约相当于压实 40KB (=1MB/25)数据.
      // 现实可能没这么理想, 我们保守一些, 假设每次查询大约相当于压实 16KB 数据, 这样
      // 我们就可以得出压实之前一个文件被允许查询的次数 == [文件字节数/16KB],
      // 一个文件最大 2MB, 则在压实前最多允许查询 128 次, 超过次数会触发压实操作.
      f->allowed_seeks = (f->file_size / 16384);
      // 如果允许查询次数小于 100, 则按 100 次处理. 
      if (f->allowed_seeks < 100) f->allowed_seeks = 100;

      // todo 一个文件会同时出现在删除列表和新增列表? 
      levels_[level].deleted_files.erase(f->number);
      levels_[level].added_files->insert(f);
    }
  }

  // 将当前 version 与 builder 保存的新增文件按序合并
  // 追加到新 Version v 中.
  void SaveTo(Version* v) {
    BySmallestKey cmp;
    cmp.internal_comparator = &vset_->icmp_;
    // 从低到高将当前 Version base_ 每个 level 文件列表和 Builder::levels_ 每个对应 level
    // 新增文件列表合并, 并保存到 Version v 对应 level 中.
    for (int level = 0; level < config::kNumLevels; level++) {
      // 把新加的文件和已有文件进行合并, 丢弃已被删除的文件, 最终结果保存到 *v.

      // Version base_ 中 level-L 对应的文件列表
      const std::vector<FileMetaData*>& base_files = base_->files_[level];
      std::vector<FileMetaData*>::const_iterator base_iter = base_files.begin();
      std::vector<FileMetaData*>::const_iterator base_end = base_files.end();
      // builder 保存的 level-L 对应的新增文件集合
      const FileSet* added = levels_[level].added_files;
      v->files_[level].reserve(base_files.size() + added->size());
      // 下面两个循环按照文件包含的 key 从小到大顺序合并前述两个文件列表.
      // (具体逻辑就是将两个有序列表合并的过程.)
      for (FileSet::const_iterator added_iter = added->begin();
           added_iter != added->end();
           ++added_iter) {
        // 针对 builder 中每个新增文件 *added_iter,
        // 从 base_ 对应 level 寻找第一个大于它的文件,
        // 然后将这个文件之前的文件(builder 里文件列表从小到大有序)
        // 都追加到 v 中.
        // 寻找过程采用 BySmallestKey 比较器(这个抽象极好).
        for (std::vector<FileMetaData*>::const_iterator bpos
                 = std::upper_bound(base_iter, base_end, *added_iter, cmp);
             base_iter != bpos; // 如果相等说明 builder 全部文件都比 added_iter 大
             ++base_iter) {
          // bpos 位置处文件小于 added_iter,
          // 将其追加到 Version v 对应 level 的文件列表中
          MaybeAddFile(v, level, *base_iter);
        }

        // builder 中小于 added_iter 的文件都追加过了,
        // 将 *added_iter 追加到 Version v 的对应 level 的文件列表中.
        MaybeAddFile(v, level, *added_iter);
      }

      // Add remaining base files
      // 将 Version base_ 中 level-L 对应的文件列表剩余的文件追加到 Version v 的对应 level-L 的文件列表中
      for (; base_iter != base_end; ++base_iter) {
        MaybeAddFile(v, level, *base_iter);
      }
// NDEBUG disables standard-C assertions
// 下面代码用来断言合并结果 V 中大于 0 的 level 的文件不存在重叠
#ifndef NDEBUG
      // Make sure there is no overlap in levels > 0
      // 确保 Version v 中大于 0 的 level-L 内部文件之间不互相重叠
      if (level > 0) {
        // 注意 i 从 1 开始
        for (uint32_t i = 1; i < v->files_[level].size(); i++) {
          // 前一个文件的最大 key
          const InternalKey& prev_end = v->files_[level][i-1]->largest;
          // 当前文件的最小 key
          const InternalKey& this_begin = v->files_[level][i]->smallest;
          // "前一个文件的最大 key" 如果大于 "当前文件的最小 key"  就说明发生重叠了, 终止并报错
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
    // 如果 f 在 deleted_files 中, 表示它已经被安排删除了, 则什么也不做
    if (levels_[level].deleted_files.count(f->number) > 0) {
      // File is deleted: do nothing
    } else {
      std::vector<FileMetaData*>* files = &v->files_[level];
      if (level > 0 && !files->empty()) {
        // 针对 level-0 之外的 level, 其所含文件之间不许重叠.
        // 确保新加入的 f 的最小 key 大于当前文件列表最后的文件的最大 key
        assert(vset_->icmp_.Compare((*files)[files->size()-1]->largest,
                                    f->smallest) < 0);
      }
      // 将 f 引用计数加一并将其加入到 Version v 的文件列表中
      f->refs++;
      // 将 f 追加到文件列表后
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
  // 用一个新 Version(未包含实际内容)替换当前 current_
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
  // current_ 引用了 v, 将 v 引用计数加一
  v->Ref();

  // Append to linked list
  // 将 v 加入到双向循环链表中, 新插入的永远是 dummy_versions_ 的前驱.
  v->prev_ = dummy_versions_.prev_;
  v->next_ = &dummy_versions_;
  v->prev_->next_ = v;
  v->next_->prev_ = v;
}

// 1, 将 *edit 内容(可看作当前针对 level 架构的增量更新)和当前 version 内容合并构成一个新的 version;
// 2, 然后将这个新 version 内容序列化为一条日志写到新建的 manifest 文件;
// 3, 同时将该 manifest 文件名写入 current 文件;
// 4, 最后把新的 version 替换当前 version.
//
// 前提: *mu 在进入方法之前就被持有了. 
// 前提: 没有其它线程并发调用该方法. 
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
    // 将当前 VersionSet 及其 current version 作为输入构建一个新的 Builder
    Builder builder(this, current_);
    // 将新的 VersionEdit 与 current version 内容合并
    builder.Apply(edit);
    // 将 Builder 内容输出到 Version v 中
    builder.SaveTo(v);
  }
  // 计算 Version v 下个最适合做压实的 level. 
  Finalize(v);

  // Initialize new descriptor log file if necessary by creating
  // a temporary file that contains a snapshot of the current version.
  // 如有必要通过创建一个临时文件来初始化一个新的文件描述符, 这个临时文件包含了当前 Version 的一个快照
  std::string new_manifest_file;
  Status s;
  // 如果 MANIFEST 文件指针为空则新建一个
  if (descriptor_log_ == nullptr) {
    // No reason to unlock *mu here since we only hit this path in the
    // first call to LogAndApply (when opening the database).
    // 因为仅仅在打开数据库时才会走到这里, 所以没必要持有锁, 毕竟数据库不打开
    // 任何线程都无法进行读写.
    assert(descriptor_file_ == nullptr);
    // manifest 文件又叫 descriptor 文件
    new_manifest_file = DescriptorFileName(dbname_, manifest_file_number_);
    edit->SetNextFile(next_file_number_);
    s = env_->NewWritableFile(new_manifest_file, &descriptor_file_);
    if (s.ok()) {
      descriptor_log_ = new log::Writer(descriptor_file_);
      // 将当前 Version 保存的 level 架构信息保存到一个新 VersionEdit 中后将其序列化到 MANIFEST 文件.
      s = WriteSnapshot(descriptor_log_);
    }
  }

  // Unlock during expensive MANIFEST log write
  {
    mu->Unlock();

    // Write new record to MANIFEST log
    // 将通过参数传入的 VersionEdit 记录到 manifest 文件. 这个地方相对于上面的 snapshot 相当于是一个增量.
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
    // 如果 manifest 文件是新建的, 则这里需要将其文件名保存到 current 文件中.
    if (s.ok() && !new_manifest_file.empty()) {
      s = SetCurrentFile(env_, dbname_, manifest_file_number_);
    }

    mu->Lock();
  }

  // 更新 VersionSet 中的 current_ version.
  if (s.ok()) {
    // 将基于当前 Version 和增量 VersionEdit 构建的新 version 
    // 插入到 versionset 替换当前 version. 它是最新的 level 架构.
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

// Recover the last saved descriptor from persistent storage.
// 该方法负责从最后一个 MANIFEST 文件解析内容出来与当前 Version 保存的 level 架构合并保存到一个
// 新建的 Version 中, 然后将这个新的 version 作为当前的 version.
// 参数是输出型的, 负责保存一个指示当前 MANIFEST 文件是否可以续用.
Status VersionSet::Recover(bool *save_manifest) {
  struct LogReporter : public log::Reader::Reporter {
    Status* status;
    virtual void Corruption(size_t bytes, const Status& s) {
      if (this->status->ok()) *this->status = s;
    }
  };

  // Read "CURRENT" file, which contains a pointer to the current manifest file
  // 读取 CURRENT 文件获取 MANIFEST 文件名称
  std::string current;
  Status s = ReadFileToString(env_, CurrentFileName(dbname_), &current);
  if (!s.ok()) {
    return s;
  }
  if (current.empty() || current[current.size()-1] != '\n') {
    return Status::Corruption("CURRENT file does not end with newline");
  }
  current.resize(current.size() - 1);

  // 构造 MANIFEST 文件路径
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
    // 循环读取 MANIFEST 文件日志, 每一行日志就是一个 VersionEdit
    while (reader.ReadRecord(&record, &scratch) && s.ok()) {
      VersionEdit edit;
      // 将 record 反序列化为 version_edit
      s = edit.DecodeFrom(record);
      if (s.ok()) {
        if (edit.has_comparator_ &&
            edit.comparator_ != icmp_.user_comparator()->Name()) {
          s = Status::InvalidArgument(
              edit.comparator_ + " does not match existing comparator ",
              icmp_.user_comparator()->Name());
        }
      }

      // 将 VersionEdit 保存到 VersionSet 的 builder 中, 
      // 后者可以一次性将这些文件变更与当前 Version 合并构成新 version.
      if (s.ok()) {
        builder.Apply(&edit);
      }

      if (edit.has_log_number_) {
        // 保存最新的日志文件名, 越后面的日志(record)记录的日志文件名越新
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

  // 至此解析 MANIFEST 文件结束, 根据其保存的全部文件变更创建新的 Version
  if (s.ok()) {
    Version* v = new Version(this);
    // 将当前 version 和 builder 的 level 架构合并放到新的 v 中
    builder.SaveTo(v);
    // Install recovered version
    // 确定下一个待压实的 level
    Finalize(v);
    // 将 v 作为当前 version
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
      // 当前的 MANIFEST 文件不能重用了, 压实.
      *save_manifest = true;
    }
  }

  return s;
}

// 确认当前的 MANIFEST 是否可以重用, 如果超过大小上限(2MB, 防止太大导致重启时重建 level 耗时过多做无用操作, 具体见 Recover)
// 就不能重用了, 返回 false;
// 如果不存在 MANIFEST 文件则新建一个, 成功返回 true, 否则返回 false.
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
      // manifest 文件大小超过阈值就不能继续向里面写了, 目前上限是 2MB.
      // 如果太大, 则数据库每次启动重建 level 架构耗时会非常长, 而且会做太多无用工作.
      manifest_size >= TargetFileSize(options_)) {
    return false;
  }

  assert(descriptor_file_ == nullptr);
  assert(descriptor_log_ == nullptr);
  // 如果当前无 MANIFEST 文件, 则创建之并存放到 descriptor_file_
  Status r = env_->NewAppendableFile(dscname, &descriptor_file_);
  if (!r.ok()) {
    Log(options_->info_log, "Reuse MANIFEST: %s\n", r.ToString().c_str());
    assert(descriptor_file_ == nullptr);
    return false;
  }

  Log(options_->info_log, "Reusing MANIFEST %s\n", dscname.c_str());
  // 为新的 MANIFEST 文件创建新的 writer
  descriptor_log_ = new log::Writer(descriptor_file_, manifest_size);
  manifest_file_number_ = manifest_number;
  return true;
}

void VersionSet::MarkFileNumberUsed(uint64_t number) {
  if (next_file_number_ <= number) {
    next_file_number_ = number + 1;
  }
}

// 计算指定 Version 下个最适合做压实的 level, 保存到 v 中.
// 计算逻辑如下: 
// - 针对 level-0, 计算当前文件个数相对上限的比值
// - 针对其它 levels, 计算每个 level 当前字节数相对于其上限的比值
// 上述比值最大的那个 level 即为下个适合进行压实的 level. 
void VersionSet::Finalize(Version* v) {
  // 计算最适合做压实的 level
  int best_level = -1;
  double best_score = -1;

  // 遍历每个 level, 计算每个 level 的 score(当前大小相对于上限的比值),
  // 并选择最大的 score 及其 level
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
      // 我们会特殊对待 level-0, 即限制文件数目而不是字节数目, 原因有二: 
      // (1)写缓冲空间大一些(如果用字节数限制, 上限仅为 10^0MB=1MB), level-0 压实次数少一些.
      // (2)每次读取的时候都会对 level-0 文件做合并,
      // 因此当单个文件很小(可能因为写缓冲设置的太小或者太高的压缩比或者太多的覆写或者删除)
      // 的时候我们希望避免产生太多的文件. 
      score = v->files_[level].size() /
          static_cast<double>(config::kL0_CompactionTrigger);
    } else {
      // 计算 level 当前大小与该 level 字节上限(一般是 10^L MB)的比值
      const uint64_t level_bytes = TotalFileSize(v->files_[level]);
      score =
          static_cast<double>(level_bytes) / MaxBytesForLevel(options_, level);
    }

    // best_score 保存最大的 score, best_level 保存具有最大 score 的 level
    if (score > best_score) {
      best_level = level;
      best_score = score;
    }
  }

  // score 最大的那个 level 就是下个要进行压实的 level
  v->compaction_level_ = best_level;
  v->compaction_score_ = best_score;
}

// 将当前 Version 保存的 level 架构信息保存到一个新 VersionEdit 中后将其序列化到 MANIFEST 文件.
// 将当前 versionset 维护的当前 version 的每个 level 的下次压实起始 key 以及每个 level 的
// 文件列表全都追加到 versionedit 中, 然后将 versionedit 序列化成一条日志记录到 manifest 文件.
// 该方法类似 VersionSet::Builder::SaveTo 方法, 不过这里是把当前 version 信息追加到 versionedit 中,
// 而不是合并当前 version 和 versionedit 然后构造新的 version.
Status VersionSet::WriteSnapshot(log::Writer* log) {
  // TODO: Break up into multiple records to reduce memory usage on recovery?

  // Save metadata
  VersionEdit edit;
  // 记录当前使用的比较器名称
  edit.SetComparatorName(icmp_.user_comparator()->Name());

  // Save compaction pointers
  // 保存每个 level 下次压实起始 key 到 edit 中
  for (int level = 0; level < config::kNumLevels; level++) {
    if (!compact_pointer_[level].empty()) {
      InternalKey key;
      key.DecodeFrom(compact_pointer_[level]);
      edit.SetCompactPointer(level, key);
    }
  }

  // Save files
  // 遍历当前 version, 将其维护的 level 架构逐层逐文件追加到 edit 中
  for (int level = 0; level < config::kNumLevels; level++) {
    const std::vector<FileMetaData*>& files = current_->files_[level];
    for (size_t i = 0; i < files.size(); i++) {
      const FileMetaData* f = files[i];
      edit.AddFile(level, f->number, f->file_size, f->smallest, f->largest);
    }
  }

  std::string record;
  // 将 edit 序列化为一条日志, 然后写入当前 MANIFEST 文件
  edit.EncodeTo(&record);
  return log->AddRecord(record);
}

// 查询当亲 Version, 取出对应 level 文件个数
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

// 把全部 levels 拉平, 确定指定 key 大约在 v 对应的 level 架构中第几个字节处.
uint64_t VersionSet::ApproximateOffsetOf(Version* v, const InternalKey& ikey) {
  uint64_t result = 0;
  // 遍历保存在 v 中的 level 架构信息, 从低到高
  for (int level = 0; level < config::kNumLevels; level++) {
    const std::vector<FileMetaData*>& files = v->files_[level];
    // 文件按照 key 从小到大
    for (size_t i = 0; i < files.size(); i++) {
      // 如果整个文件都位于目标 key 之前, 则其实际位置肯定更靠后, 直接累加该文件字节数
      if (icmp_.Compare(files[i]->largest, ikey) <= 0) {
        result += files[i]->file_size;
      } else if (icmp_.Compare(files[i]->smallest, ikey) > 0) {
        // 如果整个文件都位于目标 key 之后, 则后面文件都不用看了, 偏移肯定在此文件之前
        if (level > 0) {
          // 如果当前计算 level 不为 0, 则其文件相互无重叠, 没必要考虑后面文件了.
          break;
        }
      } else {
        // 目标 key 大小位于文件键空间内
        Table* tableptr;
        Iterator* iter = table_cache_->NewIterator(
            ReadOptions(), files[i]->number, files[i]->file_size, &tableptr);
        if (tableptr != nullptr) {
          // 看目标 key 大约在文件内什么位置
          result += tableptr->ApproximateOffsetOf(ikey.Encode());
        }
        delete iter;
      }
    }
  }
  return result;
}

// Add all files listed in any live version to *live.
// May also mutate some internal state.
//
// 从旧到新遍历 version(因为新 version 是其前一个 version 合并 versionedit 构造的), 
// 在每个 version 中从低到高遍历 level(针对同样的 key, level 越低其对应数据越新),
// 将 level 中的文件都插入到集合 live 中.
// 注意, 虽然这里强调了顺序的特点, 但是由于 live 是个集合, 集合本身无序, 所以这些特点在这里无意义.
// 但有一点很重要, 由于后面的 version 是由前面的 verison 和 versionedit 合并而来, 而且,
// 注意, sstable 不可变且 set 集合有互异性特征所以最后保存的都是一个个需要保留下来的文件而不会
// 重复。
void VersionSet::AddLiveFiles(std::set<uint64_t>* live) {
  // 逐个 version, 从老到新
  for (Version* v = dummy_versions_.next_;
       v != &dummy_versions_;
       v = v->next_) {
    // 逐个 level, 从低到高     
    for (int level = 0; level < config::kNumLevels; level++) {
      const std::vector<FileMetaData*>& files = v->files_[level];
      // 逐个文件
      for (size_t i = 0; i < files.size(); i++) {
        // 添加到 live 集合中
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

// 将 inputs 中最小和最大 key 提取保存到 *smallest, *largest.
// 前提: inputs 绝对不能为空。
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

// 把 input1 和 input2 合二为一, 然后调用 GetRange 找到最小 key 和最大 key.
// 前提: inputs 不能为空.
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

  // 针对 level-0 的文件不得不进行合并处理(因为这一层文件可能彼此重叠);
  // 针对其它层, 每层创建一个级联迭代器.
  // TODO(opt): use concatenating iterator for level-0 if there is no overlap
  const int space = (c->level() == 0 ? c->inputs_[0].size() + 1 : 2);
  Iterator** list = new Iterator*[space];
  int num = 0;
  // 注意这个迭代顺序, which 从 0 到 1, 意思是 level 从低到高依次追加迭代器到列表
  for (int which = 0; which < 2; which++) {
    if (!c->inputs_[which].empty()) {
      // 如果是 level-0, 则特殊处理(每个文件一个迭代器, 后面的文件比前面的新,
      // 数据如果重叠意味着后面的数据覆盖了前面的)
      if (c->level() + which == 0) {
        const std::vector<FileMetaData*>& files = c->inputs_[which];
        // 注意这个迭代顺序, 越后面文件越新
        for (size_t i = 0; i < files.size(); i++) {
          // 每个文件创建一个迭代器, 依次追加到列表中
          list[num++] = table_cache_->NewIterator(
              options, files[i]->number, files[i]->file_size);
        }
      } else {
        // 为 level-0 之外的每层创建一个级联迭代器.
        // level 从低到高, 每个 level 对应迭代器依次追加到列表中.
        list[num++] = NewTwoLevelIterator(
            new Version::LevelFileNumIterator(icmp_, &c->inputs_[which]),
            &GetFileIterator, table_cache_, options);
      }
    }
  }
  assert(num <= space);
  // 将上面构造的全部迭代器合并成一个
  Iterator* result = NewMergingIterator(&icmp_, list, num);
  delete[] list;
  return result;
}

Compaction* VersionSet::PickCompaction() {
  Compaction* c;
  int level;

  const bool size_compaction = (current_->compaction_score_ >= 1);
  const bool seek_compaction = (current_->file_to_compact_ != nullptr);

  // 我们倾向于因为某层数据太多而触发的压实,
  // 而非因为查询次数超过上限(即 FileMetaData->allowed_seeks)触发的压实.
  // 实现办法就是先检查大小后检查查询次数.

  // 先看有无 level 存储比值已经超过上限
  if (size_compaction) {
    level = current_->compaction_level_;
    assert(level >= 0);
    assert(level+1 < config::kNumLevels);
    c = new Compaction(options_, level);

    // 找到待压实 level 第一个可能包含 compact_pointer_[level] 的文件
    for (size_t i = 0; i < current_->files_[level].size(); i++) {
      FileMetaData* f = current_->files_[level][i];
      if (compact_pointer_[level].empty() ||
          icmp_.Compare(f->largest.Encode(), compact_pointer_[level]) > 0) {
        // 把这个文件追加到 level 对应的待压实文件集合中
        c->inputs_[0].push_back(f);
        break;
      }
    }
    // 如果 level 对应的待压实文件集合为空(说明 compact_pointer_[level]
    // 位于 level 最后一个文件之后), 则回绕到开头, 将其第一个
    // 文件加入到待压实集合.
    if (c->inputs_[0].empty()) {
      // Wrap-around to the beginning of the key space
      c->inputs_[0].push_back(current_->files_[level][0]);
    }
  } else if (seek_compaction) { // 再看是否有文件因为查询次数过多
    // (Version::Get() 时候疑似包含但实际不包含目标 key 的最底层
    // level 的第一个文件会被记录到统计信息中, 然后会被 Version::UpdateStats() 处理)
    // 而可以触发压实
    level = current_->file_to_compact_level_;
    c = new Compaction(options_, level);
    c->inputs_[0].push_back(current_->file_to_compact_);
  } else {
    return nullptr;
  }

  c->input_version_ = current_;
  c->input_version_->Ref();

  // level-0 文件可能彼此重叠, 所以要把全部重叠文件都加入到待压实文件集合中
  if (level == 0) {
    InternalKey smallest, largest;
    GetRange(c->inputs_[0], &smallest, &largest);
    // Note that the next call will discard the file we placed in
    // c->inputs_[0] earlier and replace it with an overlapping set
    // which will include the picked file.
    // 注意下面这个方法会清除 inputs[0] 内容, 不过不用担心, 由于已经提前提取到了
    // inputs[0] 键范围所以下面这个方法会把那个被清除的文件重新捞回来.
    current_->GetOverlappingInputs(0, &smallest, &largest, &c->inputs_[0]);
    assert(!c->inputs_[0].empty());
  }

  // 将 level+1 中与 level 对应待压实集合重叠的文件拿出来做压实
  SetupOtherInputs(c);

  return c;
}

// 设置 inputs[1] 即 level+1 对应的重叠文件列表,
// 同时根据实际情况决定是否扩大 level 层的待压实文件列表,
// 即 inputs[0].
void VersionSet::SetupOtherInputs(Compaction* c) {
  const int level = c->level();
  InternalKey smallest, largest;
  // 将待压实文件的最小最大 key 找到， 放到 &smallest, &largest
  GetRange(c->inputs_[0], &smallest, &largest);

  // 将待压实文件与自己高一层的重叠文件找到， 放到 inputs_[1] 中
  current_->GetOverlappingInputs(level+1, &smallest, &largest, &c->inputs_[1]);

  // 确定 level 和 level+1 待压实的全部 key 的范围
  InternalKey all_start, all_limit;
  GetRange2(c->inputs_[0], c->inputs_[1], &all_start, &all_limit);

  // 为了尽可能多的压实数据, 确认我们是否可以在不改变 level+1 层文件个数
  // 的前提下增加 level 层的文件个数.
  // 之所以这么限制, 是因为 level 文件个数增加可能导致 level+1 有新的重叠文件,
  // 我们要避免最后把 level 和 level+1 文件都纳入到本次压实.
  if (!c->inputs_[1].empty()) {
    std::vector<FileMetaData*> expanded0;
    // 寻找"漏网之鱼", 将 level 层落入待压实范围的全部文件捞出来, 放到 &expanded0 以与 inputs[0] 区别
    current_->GetOverlappingInputs(level, &all_start, &all_limit, &expanded0);
    const int64_t inputs0_size = TotalFileSize(c->inputs_[0]);
    const int64_t inputs1_size = TotalFileSize(c->inputs_[1]);
    const int64_t expanded0_size = TotalFileSize(expanded0);
    // 如果 expanded0.size() > c->inputs_[0].size() 说明真有漏网之鱼,
    // 否则这两个值应该相等.
    if (expanded0.size() > c->inputs_[0].size() &&
    // 如果 level+1 层文件总大小加上 level 层落在压实范围内全部文件大小
    // 小于一次压实字节数的硬上限(25 个文件大小), 则将漏网之鱼包含进来.
        inputs1_size + expanded0_size <
            ExpandedCompactionByteSizeLimit(options_)) {
      InternalKey new_start, new_limit;
      // 获取扩展后的 level 层的起止键(文件集扩张可能导致起止键变更),
      // 放到 &new_start, &new_limit
      GetRange(expanded0, &new_start, &new_limit);
      std::vector<FileMetaData*> expanded1;
      // 重新获取 level+1 与扩展后的 level 层待压实文件的重叠文件列表， 放到 &expanded1
      current_->GetOverlappingInputs(level+1, &new_start, &new_limit,
                                     &expanded1);
      // 如果 &expanded1 大小等于 level 层待压实文件集扩大前获取的 level+1 文件集大小,
      // 这正是我们期待的(只扩张 level 待压实文件集而不改变 level+1 的).
      // 否则, 压实文件集(inputs) 不做任何变更.
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

  // 计算祖父(即 level+2)与本次压实重叠的文件列表, 放到 &c->grandparents_
  if (level + 2 < config::kNumLevels) {
    current_->GetOverlappingInputs(level + 2, &all_start, &all_limit,
                                   &c->grandparents_);
  }

  // 更新 level 层下一次压实的起点.
  // 我们不等着 VersionEdit 被应用(写入 manifest)就直接在这里更新, 为的是如果本次压实
  // 失败, 我们下次就会换个键范围进行尝试.
  compact_pointer_[level] = largest.Encode().ToString();
  // 更新 VersionEdit 中关于下次压实的信息
  c->edit_.SetCompactPointer(level, largest);
}

Compaction* VersionSet::CompactRange(
    int level,
    const InternalKey* begin,
    const InternalKey* end) {
  std::vector<FileMetaData*> inputs;
  // 获取待压实文件列表
  current_->GetOverlappingInputs(level, begin, end, &inputs);
  if (inputs.empty()) {
    return nullptr;
  }

  // 要避免每次压实工作量过大(衡量标准就是待压实 level 单个文件大小的上限).
  // 但 level-0 无需这样, 因为 level-0 文件存在
  // 重叠, 如果两个文件重叠, 不能挑选某个然后丢弃另一个较老的文件.
  if (level > 0) {
    // 获取指定 level 文件大小上限
    const uint64_t limit = MaxFileSizeForLevel(options_, level);
    uint64_t total = 0;
    for (size_t i = 0; i < inputs.size(); i++) {
      uint64_t s = inputs[i]->file_size;
      total += s;
      // 如果待压实文件总大小超过了单个文件大小上限
      if (total >= limit) {
        // 则丢掉部分文件, 以避免一次压实工作量过大.
        // 其实为了保险 resize 时候应该用 i, 肯定能保证 total < limit,
        // 这里用 i+1, 应该是考虑到压实过程会去掉冗余.
        inputs.resize(i + 1);
        break;
      }
    }
  }

  // 构造一个 Compaction 对象
  Compaction* c = new Compaction(options_, level);
  c->input_version_ = current_;
  c->input_version_->Ref();
  // 待压实文件列表
  c->inputs_[0] = inputs;
  // level 层待压实文件列表已经确认了, 下面需要
  // 找到与 level 层待压实文件列表重叠的 level+1
  // 层文件列表并加入到压实列表中.
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
  // 如果 level 与祖父(即 level+2) 有大量重叠数据, 则避免直接移动(即把文件从
  // level 移动到 level+1);
  // 否则会创建一个父文件(即 level+1), 很显然这个文件和自己
  // 父亲 level(即上面说的 level+2)存在大量重叠数据, 这个情况会导致
  // 非常昂贵的合并.
  //
  // 如果 level 层只有 1 个待压实文件 && level+1 层没有与 level 待压实文件发生
  // 重叠的文件 && level+2 层与 level 待压实文件重叠的字节数不大于上限,
  // 则可以用移动替代压实.
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
  // 从祖父层开始向上遍历
  for (int lvl = level_ + 2; lvl < config::kNumLevels; lvl++) {
    // 第 lvl 层的文件列表
    const std::vector<FileMetaData*>& files = input_version_->files_[lvl];
    // 遍历 lvl 层文件列表
    for (; level_ptrs_[lvl] < files.size(); ) {
      FileMetaData* f = files[level_ptrs_[lvl]];
      // user_key 可能落在了 f 文件里
      if (user_cmp->Compare(user_key, f->largest.user_key()) <= 0) {
        // We've advanced far enough
        if (user_cmp->Compare(user_key, f->smallest.user_key()) >= 0) {
          // Key falls in this file's range, so definitely not base level
          // user_key 确实落在了文件 f 里, 这就意味着
          // 祖父层或之上的 level 也包含有 user_key
          return false;
        }
        // user_key 既然小于 f->largest 但又未落在了文件 f 里,
        // 那它肯定不在 lvl 层里.
        break;
      }
      // 遍历下个文件
      level_ptrs_[lvl]++;
    }
  }
  return true;
}

bool Compaction::ShouldStopBefore(const Slice& internal_key) {
  const VersionSet* vset = input_version_->vset_;
  const InternalKeyComparator* icmp = &vset->icmp_;
  // 扫描寻找包含 internal_key 的第一个祖父 level 文件
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
    // 当前 internal_key 在 level+2 太靠后了, 也说明 level 与 level+2
    // 重叠数据量太大了, 压实应该停止了; 否则生成的 level+1 文件依然与 level+2
    // 存在大量重叠, 压实合并工作量依然巨大.
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

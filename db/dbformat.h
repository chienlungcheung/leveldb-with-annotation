// Copyright (c) 2011 The LevelDB Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file. See the AUTHORS file for names of contributors.

#ifndef STORAGE_LEVELDB_DB_DBFORMAT_H_
#define STORAGE_LEVELDB_DB_DBFORMAT_H_

#include <stdio.h>
#include "leveldb/comparator.h"
#include "leveldb/db.h"
#include "leveldb/filter_policy.h"
#include "leveldb/slice.h"
#include "leveldb/table_builder.h"
#include "util/coding.h"
#include "util/logging.h"

namespace leveldb {

// Grouping of constants.  We may want to make some of these
// parameters set via options.
//
// leveldb 相关的常量. 我们可以通过在 options 中设置对应的配置. 
namespace config {
// leveldb 的 level 数目
static const int kNumLevels = 7;

// Level-0 compaction is started when we hit this many files.
//
// 当 level-0 文件数达到该值(默认为 4 个)时开始进行压缩
static const int kL0_CompactionTrigger = 4;

// Soft limit on number of level-0 files.  We slow down writes at this point.
//
// level-0 文件数软限制, 当达到这个限制时减缓写入. 
static const int kL0_SlowdownWritesTrigger = 8;

// Maximum number of level-0 files.  We stop writes at this point.
//
// level-0 文件数最大值, 达到这个限制时停止写入. 
static const int kL0_StopWritesTrigger = 12;

// Maximum level to which a new compacted memtable is pushed if it
// does not create overlap.  We try to push to level 2 to avoid the
// relatively expensive level 0=>1 compactions and to avoid some
// expensive manifest file operations.  We do not push all the way to
// the largest level since that can generate a lot of wasted disk
// space if the same key space is being repeatedly overwritten.
//
// 指示 memtable 可以直接放到的最高 level.
// 我们尝试将数据 push 到 level-2, 这样可以避免代价相对较高的从 level-0 到 level-1 的 push, 同时避免
// 代价较高的 manifest 文件操作. 
// 我们不会一直往这个 level 上 push, 因为如果同样的键空间被重复的覆写会造成大量的磁盘空间浪费. 
static const int kMaxMemCompactLevel = 2;

// Approximate gap in bytes between samples of data read during iteration.
// 2^20 = 2^10*2^10 = 1MB todo 具体含义和使用待进一步明确. 
static const int kReadBytesPeriod = 1048576;

}  // namespace config

class InternalKey;

// Value types encoded as the last component of internal keys.
// DO NOT CHANGE THESE ENUM VALUES: they are embedded in the on-disk
// data structures.
//
// 操作类型, 包括删除操作和正常的插入操作, 标识对应数据项是否被删除了. 
// 它将会被编码到 internal_key 的最后一个字节. 
// 下面的枚举值不要修改：他们会被写到磁盘的数据结构里, 改了就对应不上了. 
enum ValueType {
  kTypeDeletion = 0x0,
  kTypeValue = 0x1
};
// kValueTypeForSeek defines the ValueType that should be passed when
// constructing a ParsedInternalKey object for seeking to a particular
// sequence number (since we sort sequence numbers in decreasing order
// and the value type is embedded as the low 8 bits in the sequence
// number in internal keys, we need to use the highest-numbered
// ValueType, not the lowest).
//
// 在 leveldb 查找一个指定的序列号对应的数据项时, 我们需要传给 leveldb 一个事先构造的
// ParsedInternalKey 对象, 构造该对象时就需要传递一个 ValueType 给它. 下面这个变量就是
// 用来定义要传何种类型的 ValueType 的. 
//
// 因为我们排序数据项时会考虑序列号, 而且会在 user_key 部分相等时按照 tag (由七个字节序列号后跟一个字节 ValueType 构成)降序排列
// (tag 越大 internal_key 越小), 所以我们应该使用最大的 ValueType, 
// 这样调用 MemTable.Seek(k) 确保找到的第一个大于等于 k 的数据项(memtable 中数据项从小到大排序)就是我们要找的数据项. todo?
static const ValueType kValueTypeForSeek = kTypeValue;

typedef uint64_t SequenceNumber;

// We leave eight bits empty at the bottom so a type and sequence#
// can be packed together into 64-bits.
// 序列号最大值为 7 字节, 留出一个字节为后面与 TypeValue 打包构成 tag
static const SequenceNumber kMaxSequenceNumber =
    ((0x1ull << 56) - 1); // ull 为 unsigned long long, 序列号占 7 个字节, 这个值就是 7 个字节能表示的最大值. 

// internal_key 就是由三部分构成的, user_key、序列号、type
struct ParsedInternalKey {
  Slice user_key;
  SequenceNumber sequence; // 7 个字节
  ValueType type; // 1 个字节

  // 为了效率故意不初始化
  ParsedInternalKey() { }  // Intentionally left uninitialized (for speed)
  ParsedInternalKey(const Slice& u, const SequenceNumber& seq, ValueType t)
      : user_key(u), sequence(seq), type(t) { }
  std::string DebugString() const;
};

// Return the length of the encoding of "key".
//
// 返回 internal_key 长度
inline size_t InternalKeyEncodingLength(const ParsedInternalKey& key) {
  return key.user_key.size() + 8;
}

// Append the serialization of "key" to *result.
//
// 将 ParsedInternalKey 序列化到 result 指定位置. 
void AppendInternalKey(std::string* result, const ParsedInternalKey& key);

// Attempt to parse an internal key from "internal_key".  On success,
// stores the parsed data in "*result", and returns true.
//
// On error, returns false, leaves "*result" in an undefined state.
//
// 将 internal_key 解析为 ParsedInternalKey 格式并存储到 result, 并返回 true; 
// 如果解析失败, 返回 false. 
bool ParseInternalKey(const Slice& internal_key, ParsedInternalKey* result);

// Returns the user key portion of an internal key.
//
// 从一个 internal_key 中将 user_key 部分抽取出来(internal_key 除了末尾 8 字节都是 user_key)
inline Slice ExtractUserKey(const Slice& internal_key) {
  assert(internal_key.size() >= 8);
  return Slice(internal_key.data(), internal_key.size() - 8);
}

// A comparator for internal keys that uses a specified comparator for
// the user key portion and breaks ties by decreasing sequence number.
//
// 为 internal_key (该 key 分为两部分, 前面是 user_key, 
// 后面是 8 字节 tag, 该 tag 由 7 字节序列号和 1 字节操作类型构成)
// 比较实现的 comparator, 
// 具体比较逻辑见该类的 Compare(const Slice& a, const Slice& b) 方法. 
//
// 该类在 MemTable.KeyComparator 类中使用. 
class InternalKeyComparator : public Comparator {
 private:
  const Comparator* user_comparator_;
 public:
  // 需要用户传入一个用于比较 user_key 的 comparator
  explicit InternalKeyComparator(const Comparator* c) : user_comparator_(c) { }
  virtual const char* Name() const;
  virtual int Compare(const Slice& a, const Slice& b) const;
  virtual void FindShortestSeparator(
      std::string* start,
      const Slice& limit) const;
  virtual void FindShortSuccessor(std::string* key) const;

  const Comparator* user_comparator() const { return user_comparator_; }

  int Compare(const InternalKey& a, const InternalKey& b) const;
};

// Filter policy wrapper that converts from internal keys to user keys
//
// 一个 wrapper, 负责将 internal_key 转换为 user_key, 然后使用内部封装的用户定义的过滤器策略. 
class InternalFilterPolicy : public FilterPolicy {
 private:
  const FilterPolicy* const user_policy_;
 public:
  explicit InternalFilterPolicy(const FilterPolicy* p) : user_policy_(p) { }
  virtual const char* Name() const;
  virtual void CreateFilter(const Slice* keys, int n, std::string* dst) const;
  virtual bool KeyMayMatch(const Slice& key, const Slice& filter) const;
};

// Modules in this directory should keep internal keys wrapped inside
// the following class instead of plain strings so that we do not
// incorrectly use string comparisons instead of an InternalKeyComparator.
//
// 这是一个非常重要的数据结构, leveldb 里的 key 就是这个 InternalKey. 
//
// 虽然 internal_key 最终被编码为了字符串, 但是也要封装到该结构里. 
// 因为这样才能尽量保证使用 InternalKeyComparator 而不是普通字符串的比较函数来比较 internal_key.
class InternalKey {
 private:
  std::string rep_; // 存储序列化后的 internal_key
 public:
  InternalKey() { }   // Leave rep_ as empty to indicate it is invalid
  InternalKey(const Slice& user_key, SequenceNumber s, ValueType t) {
    AppendInternalKey(&rep_, ParsedInternalKey(user_key, s, t));
  }

  // 将 Slice 类型的 s 解码为 InternalKey
  void DecodeFrom(const Slice& s) { rep_.assign(s.data(), s.size()); }
  // 将 InternalKey 编码为 Slice 类型
  Slice Encode() const {
    assert(!rep_.empty());
    return rep_;
  }

  Slice user_key() const { return ExtractUserKey(rep_); }

  // 用 p 重置该 internal_key
  void SetFrom(const ParsedInternalKey& p) {
    rep_.clear();
    AppendInternalKey(&rep_, p);
  }

  void Clear() { rep_.clear(); }

  std::string DebugString() const;
};

inline int InternalKeyComparator::Compare(
    const InternalKey& a, const InternalKey& b) const {
  return Compare(a.Encode(), b.Encode());
}

// 将 Slice 类型的 internal_key 反序列化为 ParsedInternalKey 类型, 并存储到 result, 并返回 true; 
// 失败返回 false. 
inline bool ParseInternalKey(const Slice& internal_key,
                             ParsedInternalKey* result) {
  const size_t n = internal_key.size();
  if (n < 8) return false;
  uint64_t num = DecodeFixed64(internal_key.data() + n - 8);
  unsigned char c = num & 0xff;
  result->sequence = num >> 8;
  result->type = static_cast<ValueType>(c);
  result->user_key = Slice(internal_key.data(), n - 8);
  return (c <= static_cast<unsigned char>(kTypeValue));
}

// A helper class useful for DBImpl::Get()
//
// 一个 LookupKey 就是一个 MemTable 的 Key. 
//
// LookupKey 主要是为了方便 DBImpl::Get(), 它由 varint32 类型的 internal_key 长度 和 internal_key 构成. 
// internal_key, 它包括了用户提供的 key, 即 user_key, 还包括一个紧随在 user_key 后的 tag 构成. 
// tag 由序列号(7 字节)和操作类型(1 字节)构成. 
class LookupKey {
 public:
  // Initialize *this for looking up user_key at a snapshot with
  // the specified sequence number.
  //
  // 根据传入的 user_key 和序列号构造, 最后得到 internal_key 长度 + internal_key 格式的 LookupKey
  LookupKey(const Slice& user_key, SequenceNumber sequence);

  ~LookupKey();

  // Return a key suitable for lookup in a MemTable.
  //
  // 返回一个 MemTable key(就是 LookupKey)
  Slice memtable_key() const { return Slice(start_, end_ - start_); }

  // Return an internal key (suitable for passing to an internal iterator)
  //
  // 返回一个 internal_key
  Slice internal_key() const { return Slice(kstart_, end_ - kstart_); }

  // Return the user key
  //
  // 返回 internal_key 的 user_key 部分
  Slice user_key() const { return Slice(kstart_, end_ - kstart_ - 8); }

 private:
  // We construct a char array of the form:
  //    klength  varint32               <-- start_ // internal_key 长度
  //    userkey  char[klength]          <-- kstart_ // 这个地方有错误, userKey 所占数组大小应为 klength - 8, 后面 8 字节是序列号加 ValueType
  //    tag      uint64
  //                                    <-- end_
  // The array is a suitable MemTable key.
  // The suffix starting with "userkey" can be used as an InternalKey.
  const char* start_; // [start, kstart_) 之间存的是 internal_key 的长度
  const char* kstart_; // [kstart_, end) 之间存的是 user_key 和 tag
  const char* end_;
  char space_[200];      // Avoid allocation for short keys 避免为一些短的 key 做单独的内存分配

  // No copying allowed
  LookupKey(const LookupKey&);
  void operator=(const LookupKey&);
};

inline LookupKey::~LookupKey() {
  if (start_ != space_) delete[] start_;
}

}  // namespace leveldb

#endif  // STORAGE_LEVELDB_DB_DBFORMAT_H_

// Copyright (c) 2012 The LevelDB Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file. See the AUTHORS file for names of contributors.
//
// A database can be configured with a custom FilterPolicy object.
// This object is responsible for creating a small filter from a set
// of keys.  These filters are stored in leveldb and are consulted
// automatically by leveldb to decide whether or not to read some
// information from disk. In many cases, a filter can cut down the
// number of disk seeks form a handful to a single disk seek per
// DB::Get() call.
//
// Most people will want to use the builtin bloom filter support (see
// NewBloomFilterPolicy() below).

#ifndef STORAGE_LEVELDB_INCLUDE_FILTER_POLICY_H_
#define STORAGE_LEVELDB_INCLUDE_FILTER_POLICY_H_

#include <string>
#include "leveldb/export.h"

namespace leveldb {

class Slice;

// 可以为数据库配置一个定制的 FilterPolicy 对象。这个对象负责从一个 keys 集合创建一个小的过滤器。
// 这些过滤器被存储到了 leveldb 并且被 leveldb 自动使用来决定是否从磁盘读取某些信息。在很多情况下，
// 过滤器可以让每个 DB::Get() 调用显著减少读盘次数。
//
// 大多数人可能要想使用内置的布隆过滤器（详见下面的 NewBloomFilterPolicy()）
class LEVELDB_EXPORT FilterPolicy {
 public:
  virtual ~FilterPolicy();

  // Return the name of this policy.  Note that if the filter encoding
  // changes in an incompatible way, the name returned by this method
  // must be changed.  Otherwise, old incompatible filters may be
  // passed to methods of this type.
  //
  // 返回过滤策略的名称。注意，如果过滤器的编码被改为了不兼容的方式，该方法返回的名称必须要改变。
  // 否则，老的不兼容的过滤器可能被误用。
  virtual const char* Name() const = 0;

  // keys[0,n-1] contains a list of keys (potentially with duplicates)
  // that are ordered according to the user supplied comparator.
  // Append a filter that summarizes keys[0,n-1] to *dst.
  //
  // Warning: do not change the initial contents of *dst.  Instead,
  // append the newly constructed filter to *dst.
  //
  // keys[0,n-1] 包含了一个键列表（可能有重复的），这些键已经按照用户提供的比较器被排序了。
  // 该方法会针对 keys[0,n-1] 全部 keys 计算得到一个过滤器，并将该过滤器对应的位图追加到 *dst 中。
  //
  // 警告：不要修改 *dst 的初始内容，相反，追加新构造的过滤器到 *dst 中。
  virtual void CreateFilter(const Slice* keys, int n, std::string* dst)
      const = 0;

  // "filter" contains the data appended by a preceding call to
  // CreateFilter() on this class.  This method must return true if
  // the key was in the list of keys passed to CreateFilter().
  // This method may return true or false if the key was not on the
  // list, but it should aim to return false with a high probability.
  //
  // 检查 key 是否存在。
  //
  // 入参 filter 包含了之前调用 CreateFilter 时得到的过滤器。如果入参 key 包含在之前传给 CreateFilter 的 keys 列表中
  // 则一定返回 true；如果该 key 不包含在之前传给 CreateFilter 的 keys 列表中，那么可能返回 true 也可能返回 false，
  // 不过目标应该是以高概率返回 false。
  virtual bool KeyMayMatch(const Slice& key, const Slice& filter) const = 0;
};

// Return a new filter policy that uses a bloom filter with approximately
// the specified number of bits per key.  A good value for bits_per_key
// is 10, which yields a filter with ~ 1% false positive rate.
//
// Callers must delete the result after any database that is using the
// result has been closed.
//
// Note: if you are using a custom comparator that ignores some parts
// of the keys being compared, you must not use NewBloomFilterPolicy()
// and must provide your own FilterPolicy that also ignores the
// corresponding parts of the keys.  For example, if the comparator
// ignores trailing spaces, it would be incorrect to use a
// FilterPolicy (like NewBloomFilterPolicy) that does not ignore
// trailing spaces in keys.
//
// 返回一个使用布隆过滤器的过滤器策略。针对入参 bits_per_key，一个好的取值是 10，
// 该值能够生成一个大约 1% 假阳性的过滤器。
//
// 当使用该过滤策略的数据库都关闭后，调用者必须删除该策略。
//
// 注意：如果你正在使用一个定制的 comparator，而且它忽略了被比较的 keys 的某些部分，
// 你不能使用 NewBloomFilterPolicy() 并且需要提供你自己的 FilterPolicy，
// 而且该策略忽略了被 comparator 忽略的部分。比如，如果 comparator 忽略了了尾部的空格，
// 那么使用一个不忽略 keys 尾部空格的过滤器策略（比如 NewBloomFilterPolicy）就错了。
LEVELDB_EXPORT const FilterPolicy* NewBloomFilterPolicy(int bits_per_key);

}  // namespace leveldb

#endif  // STORAGE_LEVELDB_INCLUDE_FILTER_POLICY_H_

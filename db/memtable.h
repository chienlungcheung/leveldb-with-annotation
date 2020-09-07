// Copyright (c) 2011 The LevelDB Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file. See the AUTHORS file for names of contributors.

#ifndef STORAGE_LEVELDB_DB_MEMTABLE_H_
#define STORAGE_LEVELDB_DB_MEMTABLE_H_

#include <string>
#include "leveldb/db.h"
#include "db/dbformat.h"
#include "db/skiplist.h"
#include "util/arena.h"

namespace leveldb {

class InternalKeyComparator;
class MemTableIterator;

// 基于 SkipList, 一个 MemTable 对应磁盘中的一个 log 文件(*.log), 后者保存着最近一系列更新操作. 
// 每个更新操作都被追加到当前的 log 文件中, 当 log 文件大小达到一个预定义的大小时(默认大约 4MB), 
// 这个 log 文件就会被转换为一个有序 table 文件, 然后一个新的 log 文件就会被创建以保存未来的更新操作. 
// 当前 log 文件的一个拷贝被保存在一个内存数据结构中(即 memtable). 
// 这份拷贝会在每次执行读操作时被查询以让每个读操作都能反映全部已被记录到 log 文件的更新. 
class MemTable {
 public:
  // MemTables are reference counted.  The initial reference count
  // is zero and the caller must call Ref() at least once.
  //
  // MemTable 是基于引用计数的, 每个 MemTable 实例初始引用计数为 0, 调用者必须
  // 至少调用一次 Ref() 方法. 
  explicit MemTable(const InternalKeyComparator& comparator);

  // Increase reference count.
  //
  // 递增引用计数
  void Ref() { ++refs_; }

  // Drop reference count.  Delete if no more references exist.
  //
  // 递减引用计数, 计数变为 0 后删除该 MemTable 实例. 
  void Unref() {
    --refs_;
    assert(refs_ >= 0);
    if (refs_ <= 0) {
      delete this;
    }
  }

  // Returns an estimate of the number of bytes of data in use by this
  // data structure. It is safe to call when MemTable is being modified.
  //
  // 返回当前 memtable 中数据字节数的估计值, 
  // 当 memtable 被修改时调用该方法也是安全的, 该方法底层实现直接用的 arena_ 持有内存的字节数. 
  size_t ApproximateMemoryUsage();

  // Return an iterator that yields the contents of the memtable.
  //
  // The caller must ensure that the underlying MemTable remains live
  // while the returned iterator is live.  The keys returned by this
  // iterator are internal keys encoded by AppendInternalKey in the
  // db/format.{h,cc} module.
  //
  // 返回一个迭代器, 该迭代器可以用来遍历整个 memtable 的内容. 
  //
  // 注意, 迭代器返回的 key 是 internal_key, 不是用户提供的 user_key.
  Iterator* NewIterator();

  // Add an entry into memtable that maps key to value at the
  // specified sequence number and with the specified type.
  // Typically value will be empty if type==kTypeDeletion.
  //
  // 根据指定的序列号和操作类型将 user_key 转换为 internal_key 然后和 value 一起向 memtable 新增一个数据项. 
  // 该数据项是 key 到 value 的映射, 如果操作类型 type==kTypeDeletion 则 value 为空. 
  // 最后数据项写入了底层的 SkipList 中. 
  // 每个数据项编码格式为 [varint32 类型的 internal_key_size, internal_key, varint32 类型的 value_size, value]
  // internal_key 由 [user_key, tag] 构成. 
  void Add(SequenceNumber seq, ValueType type,
           const Slice& key,
           const Slice& value);

  // If memtable contains a value for key, store it in *value and return true.
  // If memtable contains a deletion for key, store a NotFound() error
  // in *status and return true.
  // Else, return false.
  //
  // 如果 memtable 包含 key 对应的 value, 则将 value 保存在 *value 并返回 true. 
  // 如果 memtable 包含 key 对应的 deletion(memtable 的删除不是真的删除, 而是一个带有删除标记的 Add 操作, 
  // 而且 key 对应的 value 为空), 则将 NotFound 错误存在 *status, 并返回 true; 
  // 其它情况返回 false. 
  // LookupKey 就是 memtable 数据项的前半部分. 
  bool Get(const LookupKey& key, std::string* value, Status* s);

 private:
  ~MemTable();  // Private since only Unref() should be used to delete it 通过 Unref 可以间接删除 memtable 实例

  // 一个基于 internal_key 的比较器
  // 注意下它的函数调用运算符重载方法的参数类型, 都是 char*, 原因就是 memtable 底层的 SkipList
  // 的 key 类型就是 char*, 而类 KeyComparator 对象会被传给 SkipList 作为 key 比较器. 
  struct KeyComparator {
    const InternalKeyComparator comparator;
    explicit KeyComparator(const InternalKeyComparator& c) : comparator(c) { }
    // 注意, 这个操作符重载方法很关键, 该方法的会先从 char* 类型地址获取 internal_key, 然后对 internal_key 进行比较. 
    int operator()(const char* a, const char* b) const;
  };
  friend class MemTableIterator;
  friend class MemTableBackwardIterator;

  typedef SkipList<const char*, KeyComparator> Table;

  // 用于 SkipList 比较 key
  KeyComparator comparator_;
  // 该 memtable 引用计数
  int refs_;
  // 内存池, 给 SkipList 分配 Node 时候使用
  Arena arena_;
  // SkipList, 存储 memtable 里的数据
  Table table_;

  // No copying allowed 
  // 不允许拷贝和赋值 memtable 实例, 只能通过增加引用计数复用
  MemTable(const MemTable&);
  void operator=(const MemTable&);
};

}  // namespace leveldb

#endif  // STORAGE_LEVELDB_DB_MEMTABLE_H_

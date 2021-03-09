// Copyright (c) 2011 The LevelDB Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file. See the AUTHORS file for names of contributors.
//
// An iterator yields a sequence of key/value pairs from a source.
// The following class defines the interface.  Multiple implementations
// are provided by this library.  In particular, iterators are provided
// to access the contents of a Table or a DB.
//
// Multiple threads can invoke const methods on an Iterator without
// external synchronization, but if any of the threads may call a
// non-const method, all threads accessing the same Iterator must use
// external synchronization.

#ifndef STORAGE_LEVELDB_INCLUDE_ITERATOR_H_
#define STORAGE_LEVELDB_INCLUDE_ITERATOR_H_

#include "leveldb/export.h"
#include "leveldb/slice.h"
#include "leveldb/status.h"

namespace leveldb {

class LEVELDB_EXPORT Iterator {
 public:
  Iterator();

  // 禁用复制构造
  Iterator(const Iterator&) = delete;
  // 禁用赋值构造
  Iterator& operator=(const Iterator&) = delete;

  virtual ~Iterator();

  // 一个迭代器要么指向 key/value 对, 要么指向非法位置.
  // 当且仅当第一种情况才为 valid.
  virtual bool Valid() const = 0;

  // 将迭代器移动到数据源的第一个 key/value 对.
  // 当前仅当数据源不空时, 调用完该方法再调用 Valid() 为 true.
  virtual void SeekToFirst() = 0;

  // 将迭代器移动到数据源的最后一个 key/value 对.
  // 当前仅当数据源不空时, 调用完该方法再调用 Valid() 为 true.
  virtual void SeekToLast() = 0;

  // 将迭代器指向移动到数据源 target 位置或之后的第一个 key.
  // 当且仅当移动后的位置存在数据项时, 调用 Valid() 才为 true.
  virtual void Seek(const Slice& target) = 0;

 
  // 将迭代器移动到数据源下一个数据项.
  // 当且仅当迭代器未指向数据源最后一个数据项时, 调用完该方法后调用 Valid() 结果为 true.
  // 注意: 调用该方法前提是迭代器当前指向必须 valid.
  virtual void Next() = 0;

  // 将迭代器移动到数据源前一个数据项.
  // 当且仅当迭代器未指向数据源第一个数据项时, 调用完该方法后调用 Valid() 结果为 true.
  // 注意: 调用该方法前提是迭代器当前指向必须 valid.  
  virtual void Prev() = 0;

  // 返回当前迭代器指向的数据项的 key, Slice 类型, 如果使用迭代器进行修改则会反映到
  // 已返回的 key 上面.
  // 注意: 调用该方法前提是迭代器当前执行必须 valid.
  virtual Slice key() const = 0;

  // 返回当前迭代器指向的数据项的 value, Slice 类型, 如果使用迭代器进行修改则会反映到
  // 已返回的 value 上面.
  // 注意: 调用该方法前提是迭代器当前执行必须 valid.
  virtual Slice value() const = 0;

  // 发生错误返回之; 否则返回 ok.
  virtual Status status() const = 0;

  // 我们允许调用方注册一个带两个参数的回调函数, 当迭代器析构时该函数会被自动调用.
  using CleanupFunction = void (*)(void* arg1, void* arg2);
  // 我们允许客户端注册 CleanupFunction 类型的回调函数, 在迭代器被销毁的时候会调用它们(可以注册多个). 
  // 注意, 跟前面的方法不同, RegisterCleanup 不是抽象的, 客户端不应该覆写他们. 
  void RegisterCleanup(CleanupFunction function, void* arg1, void* arg2);

 private:
  // 清理函数被维护在一个单向链表上, 其中头节点被 inlined 到迭代器中.
  // 该类用于保存用户注册的清理函数, 一个清理函数对应一个该类对象, 全部对象被维护在一个单向链表上. 
  struct CleanupNode {
    // 清理函数及其两个参数
    CleanupFunction function;
    void* arg1;
    void* arg2;
    // 下个清理函数
    CleanupNode* next;

    // 判断清理函数是否为空指针.
    bool IsEmpty() const { return function == nullptr; }
    // 运行调用方通过 Iterator::RegisterCleanup 注册的清理函数
    void Run() { assert(function != nullptr); (*function)(arg1, arg2); }
  };
  // 清理函数列表的头节点
  CleanupNode cleanup_head_;
};

// 返回一个空迭代器(啥也不干)
LEVELDB_EXPORT Iterator* NewEmptyIterator();

// 返回带有指定状态的空迭代器
LEVELDB_EXPORT Iterator* NewErrorIterator(const Status& status);

}  // namespace leveldb

#endif  // STORAGE_LEVELDB_INCLUDE_ITERATOR_H_

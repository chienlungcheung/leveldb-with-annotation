// Copyright (c) 2011 The LevelDB Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file. See the AUTHORS file for names of contributors.

#ifndef STORAGE_LEVELDB_TABLE_ITERATOR_WRAPPER_H_
#define STORAGE_LEVELDB_TABLE_ITERATOR_WRAPPER_H_

#include "leveldb/iterator.h"
#include "leveldb/slice.h"

namespace leveldb {

// Iterator 的封装, 会为底层 Iterator 缓存 valid() 和 key() 的结果. 
// 这能够帮助我们避免虚函数调用, 同时提供了更好的缓存局部性. 
class IteratorWrapper {
 public:
  IteratorWrapper(): iter_(nullptr), valid_(false) { }
  explicit IteratorWrapper(Iterator* iter): iter_(nullptr) {
    Set(iter);
  }
  // 析构时销毁所封装的迭代器
  ~IteratorWrapper() { delete iter_; }
  // 返回封装的迭代器
  Iterator* iter() const { return iter_; }

  // Takes ownership of "iter" and will delete it when destroyed, or
  // when Set() is invoked again.
  //
  // 获取所封装的迭代器的所有权, wrapper 销毁时或者再次调用 Set() 时会释放它对应的内存. 
  void Set(Iterator* iter) {
    // 先销毁之前封装的迭代器
    delete iter_;
    // 将新迭代器封装进来
    iter_ = iter;
    if (iter_ == nullptr) {
      valid_ = false;
    } else {
      // 更新缓存
      Update();
    }
  }


  // 迭代器接口方法, 这里的实现直接返回所缓存的状态
  bool Valid() const        { return valid_; }
  // 迭代器接口方法, 这里的实现直接返回所缓存的状态
  Slice key() const         { assert(Valid()); return key_; }
  // 迭代器接口方法, 因为没缓存 value 所以这里的实现通过所封装的迭代器间接获取
  Slice value() const       { assert(Valid()); return iter_->value(); }
  // 下述迭代器接口方法调用前需确保 iter() != nullptr
  Status status() const     { assert(iter_); return iter_->status(); }
  void Next()               { assert(iter_); iter_->Next();        Update(); }
  void Prev()               { assert(iter_); iter_->Prev();        Update(); }
  void Seek(const Slice& k) { assert(iter_); iter_->Seek(k);       Update(); }
  void SeekToFirst()        { assert(iter_); iter_->SeekToFirst(); Update(); }
  void SeekToLast()         { assert(iter_); iter_->SeekToLast();  Update(); }

 private:
  // 用于更新为 iter_ 所缓存的 valid_ 和 key_
  void Update() {
    // 调用 key() 前提是迭代器指向必须 valid
    valid_ = iter_->Valid();
    if (valid_) {
      key_ = iter_->key();
    }
  }

  // 被封装的迭代器
  Iterator* iter_;
  // 用于缓存 iter_ 当前指向的 valid 状态
  bool valid_;
  // 用于缓存 iter_ 当前指向的数据项的 key
  Slice key_;
};

}  // namespace leveldb

#endif  // STORAGE_LEVELDB_TABLE_ITERATOR_WRAPPER_H_

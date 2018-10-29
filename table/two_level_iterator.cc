// Copyright (c) 2011 The LevelDB Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file. See the AUTHORS file for names of contributors.

#include "table/two_level_iterator.h"

#include "leveldb/table.h"
#include "table/block.h"
#include "table/format.h"
#include "table/iterator_wrapper.h"

namespace leveldb {

namespace {

typedef Iterator* (*BlockFunction)(void*, const ReadOptions&, const Slice&);

// 两级迭代器，这个设计比较巧妙，但也是由 table 文件结构决定的。
//
// 要想找到某个 <key, value> 对，肯定先要找到其对应的 data block，而要找的 data block
// 就要先在 index block 找到对应的 BlockHandle。
// 这个类就是这个寻找过程的实现。
//
// 该类包含两个迭代器封装：
// - 一个是 index_iter_，它指向 index block 数据项。
//   针对每个 data block 都有一个对应的 entry 包含在 index block 中：
//  - 其中 key 为大于等于对应 data block 最后（也是最大的，因为排序过了）
//    一个 key 同时小于接下来的 data block 第一个 key 的字符串；
//  - value 是指向一个对应 data block 的 BlockHandle。
// - 另一个是 data_iter_，它指向 data block 的数据项，
//   至于这个 data block 是否与 index_iter_ 所指数据项对应 data block 一致，
//   那要看实际情况，不过即使不一致也无碍。
class TwoLevelIterator: public Iterator {
 public:
  TwoLevelIterator(
    Iterator* index_iter,
    BlockFunction block_function,
    void* arg,
    const ReadOptions& options);

  virtual ~TwoLevelIterator();

  virtual void Seek(const Slice& target);
  virtual void SeekToFirst();
  virtual void SeekToLast();
  virtual void Next();
  virtual void Prev();

  virtual bool Valid() const {
    return data_iter_.Valid();
  }
  virtual Slice key() const {
    assert(Valid());
    return data_iter_.key();
  }
  virtual Slice value() const {
    assert(Valid());
    return data_iter_.value();
  }

  // index_iter_ 或 data_iter_ 有一个不 ok 则双层指针就不 ok
  virtual Status status() const {
    // It'd be nice if status() returned a const Status& instead of a Status
    if (!index_iter_.status().ok()) {
      return index_iter_.status();
    } else if (data_iter_.iter() != nullptr && !data_iter_.status().ok()) {
      return data_iter_.status();
    } else {
      return status_;
    }
  }

 private:
  void SaveError(const Status& s) {
    if (status_.ok() && !s.ok()) status_ = s;
  }
  void SkipEmptyDataBlocksForward();
  void SkipEmptyDataBlocksBackward();
  void SetDataIterator(Iterator* data_iter);
  void InitDataBlock();

  BlockFunction block_function_; // 具体实现见 Table::BlockReader
  void* arg_;
  const ReadOptions options_;
  Status status_;
  // 指向 index block 数据项的迭代器 wrapper。
  // 针对每个 data block 都有一个对应的 entry 包含在 index block 中：
  // - 其中 key 为大于等于对应 data block 最后（也是最大的，因为排序过了）一个 key
  // 同时小于接下来的 data block 第一个 key 的字符串；
  // - value 是指向一个对应 data block 的 BlockHandle。
  IteratorWrapper index_iter_;
  // 与 index_iter_ 指向对应的 data block（或之后 data block，因为 data_iter_ 调用 Next 会后移）
  // 的数据项的迭代器
  IteratorWrapper data_iter_; // May be nullptr
  // If data_iter_ is non-null, then "data_block_handle_" holds the
  // "index_value" passed to block_function_ to create the data_iter_.
  //
  // 如果 data_iter_ 不为空，则下面的成员将会持有要传给 block_function_ 的 index_value，
  // block_function_ 会把它转换为一个指向某个 block 的迭代器 data_iter_（具体见 Table::BlockReader）。
  std::string data_block_handle_;
};

TwoLevelIterator::TwoLevelIterator(
    Iterator* index_iter,
    BlockFunction block_function,
    void* arg,
    const ReadOptions& options)
    : block_function_(block_function),
      arg_(arg),
      options_(options),
      index_iter_(index_iter),
      data_iter_(nullptr) {
}

TwoLevelIterator::~TwoLevelIterator() {
}

// 根据 target 将 index_iter 和 data_iter 移动到对应位置
void TwoLevelIterator::Seek(const Slice& target) {
  // 因为 index block 每个数据项的 key 是对应 data block 中最大的那个 key，
  // 所以 index block 数据项也是有序的，不过比较“宏观”。
  index_iter_.Seek(target); // 先找到目标 data block
  InitDataBlock();
  if (data_iter_.iter() != nullptr) data_iter_.Seek(target); // 然后在目标 data block 找到目标数据项
  SkipEmptyDataBlocksForward(); // data_iter_.iter() 为空则直接向前移动找到第一个不为空的 data block 的第一个数据项
}

// 将 index_iter_ 指向第一个非空 data block 数据项，
// 将 data_iter_ 指向第一个非空 data block 的第一个数据项。
void TwoLevelIterator::SeekToFirst() {
  index_iter_.SeekToFirst();
  InitDataBlock();
  if (data_iter_.iter() != nullptr) data_iter_.SeekToFirst();
  SkipEmptyDataBlocksForward(); // data_iter_.iter() 为空则直接向前移动找到第一个不为空的 data block 的第一个数据项
}

// 将 index_iter_ 指向最后一个非空 data block 数据项，
// 将 data_iter_ 指向最后一个非空 data block 的最后一个数据项。
void TwoLevelIterator::SeekToLast() {
  index_iter_.SeekToLast();
  InitDataBlock();
  if (data_iter_.iter() != nullptr) data_iter_.SeekToLast();
  SkipEmptyDataBlocksBackward(); // data_iter_.iter() 为空则直接向后移动找到第一个不为空的 data block 的最后一个数据项
}

// 使得 data_iter_ 指向下一个数据项
void TwoLevelIterator::Next() {
  assert(Valid());
  // data_iter_ 调用 Next 之前如果恰好指向 index_iter_ 所指 data block 最后一个数据项，
  // 那么调用 Next 后 data_iter_ 和 index_iter_ 就不保持一致了，这个无碍。
  data_iter_.Next();
  SkipEmptyDataBlocksForward();
}

// 使得 data_iter_ 指向前一个数据项
void TwoLevelIterator::Prev() {
  assert(Valid());
  data_iter_.Prev();
  SkipEmptyDataBlocksBackward();
}

// 向前移动 index_iter_ 和 data_iter_ 跳过空的 data block 直到找到一个非空 data block，
// 并将 data_iter_ 指向该非空 data block 的第一个数据项。
void TwoLevelIterator::SkipEmptyDataBlocksForward() {
  while (data_iter_.iter() == nullptr || !data_iter_.Valid()) {
    // Move to next block
    if (!index_iter_.Valid()) {
      SetDataIterator(nullptr);
      return;
    }
    index_iter_.Next();
    InitDataBlock();
    if (data_iter_.iter() != nullptr) data_iter_.SeekToFirst();
  }
}

// 向后移动 index_iter_ 和 data_iter_ 跳过空的 data block 直到找到一个非空 data block，
// 并将 data_iter_ 指向该非空 data block 的最后一个数据项。
void TwoLevelIterator::SkipEmptyDataBlocksBackward() {
  while (data_iter_.iter() == nullptr || !data_iter_.Valid()) {
    // Move to next block
    if (!index_iter_.Valid()) {
      SetDataIterator(nullptr);
      return;
    }
    index_iter_.Prev();
    InitDataBlock();
    if (data_iter_.iter() != nullptr) data_iter_.SeekToLast();
  }
}

// 设置 data_iter_ 为 data_iter
void TwoLevelIterator::SetDataIterator(Iterator* data_iter) {
  if (data_iter_.iter() != nullptr) SaveError(data_iter_.status());
  data_iter_.Set(data_iter);
}

// 根据 index_iter_ 获取 data_iter_
void TwoLevelIterator::InitDataBlock() {
  if (!index_iter_.Valid()) {
    SetDataIterator(nullptr);
  } else {
    Slice handle = index_iter_.value(); // 获得 index_iter_ 当前所指 block 的 BlockHandle
    if (data_iter_.iter() != nullptr && handle.compare(data_block_handle_) == 0) {
      // data_iter_ is already constructed with this iterator, so
      // no need to change anything
    } else {
      // 将 BlockHandle 转换为一个指向对应 block 数据项的 Iterator
      Iterator* iter = (*block_function_)(arg_, options_, handle);
      data_block_handle_.assign(handle.data(), handle.size());
      SetDataIterator(iter);
    }
  }
}

}  // namespace

Iterator* NewTwoLevelIterator(
    Iterator* index_iter,
    BlockFunction block_function,
    void* arg,
    const ReadOptions& options) {
  return new TwoLevelIterator(index_iter, block_function, arg, options);
}

}  // namespace leveldb

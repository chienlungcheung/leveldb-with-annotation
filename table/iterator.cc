// Copyright (c) 2011 The LevelDB Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file. See the AUTHORS file for names of contributors.

#include "leveldb/iterator.h"

namespace leveldb {

Iterator::Iterator() {
  cleanup_head_.function = nullptr;
  cleanup_head_.next = nullptr;
}

Iterator::~Iterator() {
  if (!cleanup_head_.IsEmpty()) {
    cleanup_head_.Run();
    // 线性的，太多了应该会影响性能，但总要做释放操作，时间总归省不了
    for (CleanupNode* node = cleanup_head_.next; node != nullptr; ) {
      node->Run();
      CleanupNode* next_node = node->next;
      delete node;
      node = next_node;
    }
  }
}

// 将用户定制的清理函数挂到单向链表上，待迭代器销毁时挨个调用（见 ~Iterator()）。
void Iterator::RegisterCleanup(CleanupFunction func, void* arg1, void* arg2) {
  assert(func != nullptr);
  CleanupNode* node;
  if (cleanup_head_.IsEmpty()) {
    node = &cleanup_head_;
  } else {
    node = new CleanupNode();
    node->next = cleanup_head_.next;
    cleanup_head_.next = node;
  }
  node->function = func;
  node->arg1 = arg1;
  node->arg2 = arg2;
}

namespace {

class EmptyIterator : public Iterator {
 public:
  EmptyIterator(const Status& s) : status_(s) { }
  ~EmptyIterator() override = default;

  bool Valid() const override { return false; }
  void Seek(const Slice& target) override { }
  void SeekToFirst() override { }
  void SeekToLast() override { }
  void Next() override { assert(false); }
  void Prev() override { assert(false); }
  Slice key() const override { assert(false); return Slice(); }
  Slice value() const override { assert(false); return Slice(); }
  Status status() const override { return status_; }

 private:
  Status status_;
};

}  // anonymous namespace

Iterator* NewEmptyIterator() {
  return new EmptyIterator(Status::OK());
}

Iterator* NewErrorIterator(const Status& status) {
  return new EmptyIterator(status);
}

}  // namespace leveldb

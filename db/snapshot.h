// Copyright (c) 2011 The LevelDB Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file. See the AUTHORS file for names of contributors.

#ifndef STORAGE_LEVELDB_DB_SNAPSHOT_H_
#define STORAGE_LEVELDB_DB_SNAPSHOT_H_

#include "db/dbformat.h"
#include "leveldb/db.h"

namespace leveldb {

class SnapshotList;

// Snapshots are kept in a doubly-linked list in the DB.
// Each SnapshotImpl corresponds to a particular sequence number.
//
// Snapshots 保存在 DB 的一个双向循环链表中，每个 SnapshotImpl 对应一个具体的序列号。
class SnapshotImpl : public Snapshot {
 public:
  // 构造函数允许一个序列号隐式地转换为一个 SnapshotImpl 对象
  SnapshotImpl(SequenceNumber sequence_number)
      : sequence_number_(sequence_number) {}

  SequenceNumber sequence_number() const { return sequence_number_; }

 private:
  friend class SnapshotList;

  // SnapshotImpl is kept in a doubly-linked circular list. The SnapshotList
  // implementation operates on the next/previous fields direcly.
  SnapshotImpl* prev_;
  SnapshotImpl* next_;

  const SequenceNumber sequence_number_;

#if !defined(NDEBUG)
  SnapshotList* list_ = nullptr;
#endif  // !defined(NDEBUG)
};

class SnapshotList {
 public:
  SnapshotList() : head_(0) {
    head_.prev_ = &head_;
    head_.next_ = &head_;
  }

  bool empty() const { return head_.next_ == &head_; }
  SnapshotImpl* oldest() const { assert(!empty()); return head_.next_; }
  SnapshotImpl* newest() const { assert(!empty()); return head_.prev_; }

  // Creates a SnapshotImpl and appends it to the end of the list.
  //
  // 创建一个指定版本号的快照，并将其挂到双向循环链表上
  SnapshotImpl* New(SequenceNumber sequence_number) {
    assert(empty() || newest()->sequence_number_ <= sequence_number); // 最新版本的快照，版本号必须最大

    SnapshotImpl* snapshot = new SnapshotImpl(sequence_number);

#if !defined(NDEBUG)
    snapshot->list_ = this;
#endif  // !defined(NDEBUG)
    snapshot->next_ = &head_;
    snapshot->prev_ = head_.prev_;
    snapshot->prev_->next_ = snapshot;
    snapshot->next_->prev_ = snapshot;
    return snapshot;
  }

  // Removes a SnapshotImpl from this list.
  //
  // The snapshot must have been created by calling New() on this list.
  //
  // The snapshot pointer should not be const, because its memory is
  // deallocated. However, that would force us to change DB::ReleaseSnapshot(),
  // which is in the API, and currently takes a const Snapshot.
  //
  // 从双向链表上移除指定的快照。
  //
  // 要移除的快照必须是通过 New() 方法创建的。
  //
  // 指向快照的指针不应该是 const 的，因为它指向的内存会被释放掉。
  void Delete(const SnapshotImpl* snapshot) {
#if !defined(NDEBUG)
    assert(snapshot->list_ == this);
#endif  // !defined(NDEBUG)
    snapshot->prev_->next_ = snapshot->next_;
    snapshot->next_->prev_ = snapshot->prev_;
    delete snapshot;
  }

 private:
  // Dummy head of doubly-linked list of snapshots
  SnapshotImpl head_;
};

}  // namespace leveldb

#endif  // STORAGE_LEVELDB_DB_SNAPSHOT_H_

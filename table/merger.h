// Copyright (c) 2011 The LevelDB Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file. See the AUTHORS file for names of contributors.

#ifndef STORAGE_LEVELDB_TABLE_MERGER_H_
#define STORAGE_LEVELDB_TABLE_MERGER_H_

namespace leveldb {

class Comparator;
class Iterator;

// Return an iterator that provided the union of the data in
// children[0,n-1].  Takes ownership of the child iterators and
// will delete them when the result iterator is deleted.
//
// The result does no duplicate suppression.  I.e., if a particular
// key is present in K child iterators, it will be yielded K times.
//
// REQUIRES: n >= 0
//
// 提供一个逻辑迭代器，它本质是在一组迭代器（children[]）上加了一层抽象，对外部看起来只有一个迭代器而且行为与 Iterator 一致。
// 该逻辑迭代器拥有 children[] 内容的所有权，析构时需要释放其内存。
// 注意，我们不保证 children[] 内部每个迭代器之间不重叠，也不保证有序。
// 要求 n 必须大于等于 0，如果 n 等于 1，则逻辑迭代器就是普通的 iterator。
Iterator* NewMergingIterator(
    const Comparator* comparator, Iterator** children, int n);

}  // namespace leveldb

#endif  // STORAGE_LEVELDB_TABLE_MERGER_H_

// Copyright (c) 2011 The LevelDB Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file. See the AUTHORS file for names of contributors.

#include "table/merger.h"

#include "leveldb/comparator.h"
#include "leveldb/iterator.h"
#include "table/iterator_wrapper.h"

namespace leveldb {

namespace { // 匿名 namespace 把这个类藏了起来，只能通过下面的 leveldb namespace 的 NewMergingIterator 获得
// 一个逻辑迭代器，它本质是在一组迭代器（children[]）上加了一层抽象，对外部看起来只有一个迭代器而且行为与 Iterator 一致。
// 该逻辑迭代器拥有 children[] 内容的所有权，析构时需要释放其内存。
// 注意，我们不保证 children[] 内部每个迭代器之间不重叠，也不保证有序。
// 要求 n 必须大于等于 0，如果 n 等于 1，则逻辑迭代器就是普通的 iterator。
class MergingIterator : public Iterator {
 public:
  MergingIterator(const Comparator* comparator, Iterator** children, int n)
      : comparator_(comparator),
        children_(new IteratorWrapper[n]),
        n_(n),
        current_(nullptr),
        direction_(kForward) {
    for (int i = 0; i < n; i++) {
      children_[i].Set(children[i]);
    }
  }

  virtual ~MergingIterator() {
    delete[] children_;
  }

  virtual bool Valid() const {
    return (current_ != nullptr);
  }

  virtual void SeekToFirst() {
    for (int i = 0; i < n_; i++) {
      children_[i].SeekToFirst(); // 每个 child 对应的迭代范围中最小的肯定是第一个
    }
    FindSmallest(); // 从全部 child 挑那个数据项最小的
    direction_ = kForward;
  }

  virtual void SeekToLast() {
    for (int i = 0; i < n_; i++) {
      children_[i].SeekToLast(); // 每个 child 对应的迭代范围中最大的肯定是最后一个
    }
    FindLargest(); // 从全部 child 挑那个最大的
    direction_ = kReverse; // FindLargest 是倒着找的
  }

  virtual void Seek(const Slice& target) {
    for (int i = 0; i < n_; i++) {
      children_[i].Seek(target); // 最后每个 child 指向的都是各自迭代范围第一个大于等于 target 的数据项
    }
    FindSmallest(); // 从全部 child 指向的值挑最小的
    direction_ = kForward;
  }

  // 在全部迭代器范围内寻找第一个大于 current_->key 的数据项
  virtual void Next() {
    assert(Valid());

    // Ensure that all children are positioned after key().
    // If we are moving in the forward direction, it is already
    // true for all of the non-current_ children since current_ is
    // the smallest child and key() == current_->key().  Otherwise,
    // we explicitly position the non-current_ children.
    //
    // 该方法执行完毕要确保全部 child 所指都在方法执行之前的 current_->key 之后（todo 不一定，也可能等于？）。
    // 如果我们正在正向移动（即刚调用过 Seek() 或者 SeekToFirst()或者 Next()），那么全部非 current 的 child 都满足上面说的这个状态了，
    // 因为从 current_ 为最小的那个 child 而且显然的 key() == current_->key()，所以直接指向判断后面的 next 即可（todo 这个地方有问题，因为此时其它非 child 指向的 key 可能等于 current->key）。
    // 其它情况下，我们需要显式地移动非 current child 到合适的位置。
    if (direction_ != kForward) {
      for (int i = 0; i < n_; i++) {
        IteratorWrapper* child = &children_[i];
        if (child != current_) { // 让每个非 current_ child 都指向各自迭代范围内第一个大于等于 current_->key 的数据项
          child->Seek(key());
          if (child->Valid() &&
              comparator_->Compare(key(), child->key()) == 0) {
            // 如果某个 child 当前指向恰好等于 current_->key 则将其向前移动一个数据项，因为我们要找 next.
            // 注意，由于每个 child 对应的迭代范围保证是单调递增的，所以不会由重复 key 出现；但是多个 child 之间不保证由重叠 key。
            child->Next();
          }
        }
      }
      direction_ = kForward; // 下面会执行 FindSmallest
    }

    // todo 这个地方有问题，如果不执行上面 if 块，那么此时其它非 child 指向的 key 可能等于 current->key，除非针对这种情况的 child 执行了 next

    current_->Next(); // 此时 current 肯定指向最小的那个 child，移动 current 指向迭代范围内下一个数据项
    FindSmallest(); // 在全部之前 child 的 next 寻找最小的那个
  }

  // 在全部迭代器范围内寻找第一个小于 current_->key 的数据项
  virtual void Prev() {
    assert(Valid());

    // Ensure that all children are positioned before key().
    // If we are moving in the reverse direction, it is already
    // true for all of the non-current_ children since current_ is
    // the largest child and key() == current_->key().  Otherwise,
    // we explicitly position the non-current_ children.
    //
    // 确保全部 child 都位于当前 current->key 之后（todo 不一定，也可能等于？）。
    // 如果我们正在反向移动（执行了 SeekToLast()），全部非 current child 都满足上面的条件，因为 current 当前指向
    // 最大的 child 而且显然地 key() == current_->key。
    // 其它情况下，我们需要显式地将全部非 child 移动到合适满足上述条件的位置。
    if (direction_ != kReverse) {
      for (int i = 0; i < n_; i++) {
        IteratorWrapper* child = &children_[i];
        if (child != current_) {
          child->Seek(key()); // 将每个 child 移动到各自范围内第一个大于等于 current_->key 的数据项位置
          if (child->Valid()) { // child 内部存在大于等于 current_->key 的数据项，向前移动一个位置则会保证小于 current_->key
            // Child is at first entry >= key().  Step back one to be < key()
            child->Prev();
          } else { // child 内部全部 key 都小于 current_->key，则将该 child 移动到自己范围内最大那个数据项，此位置 key 是最接近 current_->key 的
            // Child has no entries >= key().  Position at last entry.
            child->SeekToLast();
          }
        }
      }
      direction_ = kReverse; // 下面会执行 FindLargest
    }

    // todo 这个地方有问题，如果不执行上面 if 块，那么此时其它非 child 指向的 key 可能等于 current->key，除非针对这种情况的 child 执行了 pre

    current_->Prev();
    FindLargest(); // 在全部之前 child 的 pre 里面找最大那个
  }

  // current 指向数据项的 key
  virtual Slice key() const {
    assert(Valid());
    return current_->key();
  }

  // current 指向数据项的 value
  virtual Slice value() const {
    assert(Valid());
    return current_->value();
  }

  // 全部 child 迭代器 ok 才算 ok
  virtual Status status() const {
    Status status;
    for (int i = 0; i < n_; i++) {
      status = children_[i].status();
      if (!status.ok()) {
        break;
      }
    }
    return status;
  }

 private:
  void FindSmallest();
  void FindLargest();

  // We might want to use a heap in case there are lots of children.
  // For now we use a simple array since we expect a very small number
  // of children in leveldb.
  const Comparator* comparator_; // 全部 child 都遵循这个 comparator
  IteratorWrapper* children_; // 全部 child 迭代器数组，child 各自范围内保证有序且无重复，但是 child 之间顺序不保证（可能有交叉而且可能重叠）
  int n_;
  // 虽然各个 child 之间无序，可能还重叠，但是这些 child 肯定能排出一个顺序来的，
  // current_ 就在这个逻辑有序顺序上进行移动。
  IteratorWrapper* current_;

  // Which direction is the iterator moving?
  // current 在 children[] 哪个方向上移动
  enum Direction {
    kForward, // 表示 current 在 children[] 数组上从左向右移动
    kReverse // 表示 current 在 children[] 数组上从右向左移动
  };
  Direction direction_; // 当前 current 在 children[] 上的移动方向
};

// 当 children_  n 个 iterator 都指向各自的最小值的时候，
// 从这 n 个 iterator 挑一个最小的，如果有多个最小的那最后 current 指向的是最左边那个。
// 注意循环从 0 开始，移动方向为正向即 kForward。
void MergingIterator::FindSmallest() {
  IteratorWrapper* smallest = nullptr;
  for (int i = 0; i < n_; i++) {
    IteratorWrapper* child = &children_[i];
    if (child->Valid()) {
      if (smallest == nullptr) {
        smallest = child;
      } else if (comparator_->Compare(child->key(), smallest->key()) < 0) {
        smallest = child; // 注意由于是正向移动，所以最后 smallest 肯定是多个最小的当中那个最左边的
      }
    }
  }
  current_ = smallest;
}

// 当 children_  n 个 iterator 都指向各自的最大值的时候，
// 从这 n 个 iterator 挑一个最大的而且是最右边那个，如果有多个最大的那最后 current 指向的是最右边那个。
// 注意循环从 n-1 开始，移动方向为反向即 kReverse。
void MergingIterator::FindLargest() {
  IteratorWrapper* largest = nullptr;
  for (int i = n_-1; i >= 0; i--) {
    IteratorWrapper* child = &children_[i];
    if (child->Valid()) {
      if (largest == nullptr) {
        largest = child;
      } else if (comparator_->Compare(child->key(), largest->key()) > 0) {
        largest = child; // 注意由于是正向移动，所以最后 largest 肯定是多个最大的当中那个最右边的
      }
    }
  }
  current_ = largest;
}
}  // namespace

Iterator* NewMergingIterator(const Comparator* cmp, Iterator** list, int n) {
  assert(n >= 0);
  if (n == 0) {
    return NewEmptyIterator();
  } else if (n == 1) {
    return list[0];
  } else {
    return new MergingIterator(cmp, list, n);
  }
}

}  // namespace leveldb

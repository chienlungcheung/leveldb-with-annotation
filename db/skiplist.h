// Copyright (c) 2011 The LevelDB Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file. See the AUTHORS file for names of contributors.

#ifndef STORAGE_LEVELDB_DB_SKIPLIST_H_
#define STORAGE_LEVELDB_DB_SKIPLIST_H_

// Thread safety
// -------------
//
// Writes require external synchronization, most likely a mutex.
// Reads require a guarantee that the SkipList will not be destroyed
// while the read is in progress.  Apart from that, reads progress
// without any internal locking or synchronization.
//
// Invariants:
//
// (1) Allocated nodes are never deleted until the SkipList is
// destroyed.  This is trivially guaranteed by the code since we
// never delete any skip list nodes.
//
// (2) The contents of a Node except for the next/prev pointers are
// immutable after the Node has been linked into the SkipList.
// Only Insert() modifies the list, and it is careful to initialize
// a node and use release-stores to publish the nodes in one or
// more lists.
//
// ... prev vs. next pointer ordering ...

#include <assert.h>
#include <stdlib.h>
#include "port/port.h"
#include "util/arena.h"
#include "util/random.h"

namespace leveldb {

class Arena;

// 线程安全相关说明：
// - 写操作需要外部同步设施，比如 mutex。
// - 读操作需要一个保证，即读操作执行期间，SkipList 不能被销毁；只要保证这一点，读操作不需要额外的同步措施。
//
// 不变式：
// - （1）已分配的 nodes 直到 SkipList 被销毁才能被删除。这很容易保证，因为我们不会删除任何 skip list nodes。
// - （2）一个 Node 一旦被链接到 SkipList 上，那这个 Node 的内容，除了 next/pre 指针以外，都是 immutable 的。
//
// 只有 Insert() 方法才会修改 SkipList。而且，初始化一个 node，或者使用 release-store 来发布 node 到一个或
// 多个 SkipList 时要格外小心。
template<typename Key, class Comparator>
class SkipList {
 private:
  struct Node;

 public:
  // Create a new SkipList object that will use "cmp" for comparing keys,
  // and will allocate memory using "*arena".  Objects allocated in the arena
  // must remain allocated for the lifetime of the skiplist object.
  //
  // cmp 用于比较 keys，arena 用做内存池。
  // 从 arena 分配的对象生命期要与 skiplist 实例一致。
  explicit SkipList(Comparator cmp, Arena* arena);

  // Insert key into the list.
  // REQUIRES: nothing that compares equal to key is currently in the list.
  //
  // 将 key 插入到 skiplist 实例中。
  // 要求： skiplist 中当前不存在等于 key 的节点（我们的数据结构不允许出现同一个 key 的数据项）。
  void Insert(const Key& key);

  // Returns true iff an entry that compares equal to key is in the list.
  //
  // 当且仅当 sliplist 中存在与 key 相等的数据项时才返回 true。
  bool Contains(const Key& key) const;

  // Iteration over the contents of a skip list
  //
  // 用于迭代 skiplist 内容的迭代器
  class Iterator {
   public:
    // Initialize an iterator over the specified list.
    // The returned iterator is not valid.
    //
    // 构造方法返回的迭代器是无效的
    explicit Iterator(const SkipList* list);

    // Returns true iff the iterator is positioned at a valid node.
    //
    // 当且仅当迭代器指向有效的 node 时才返回 true。
    bool Valid() const;

    // Returns the key at the current position.
    // REQUIRES: Valid()
    //
    // 返回迭代器当前位置的 key。
    // 要求：当前迭代器有效。
    const Key& key() const;

    // Advances to the next position.
    // REQUIRES: Valid()
    //
    // 将迭代器移动到下个位置。
    // 要求：当前迭代器有效。
    void Next();

    // Advances to the previous position.
    // REQUIRES: Valid()
    //
    // 将迭代器倒退一个位置。
    // 要求：当前迭代器有效。
    void Prev();

    // Advance to the first entry with a key >= target
    //
    // 将迭代器移动到第一个 key >= target 的数据项所在位置。
    void Seek(const Key& target);

    // Position at the first entry in list.
    // Final state of iterator is Valid() iff list is not empty.
    //
    // 将迭代器移动到 skiplist 第一个数据项所在位置。
    // 迭代器的最终状态是有效的，当且仅当 skiplist 不为空。
    void SeekToFirst();

    // Position at the last entry in list.
    // Final state of iterator is Valid() iff list is not empty.
    //
    // 将迭代器移动到 skiplist 最后一个数据项所在位置。
    // 迭代器的最终状态是有效的，当且仅当 skiplist 不为空。
    void SeekToLast();

   private:
    const SkipList* list_;
    Node* node_;
    // Intentionally copyable
  };

 private:
  enum { kMaxHeight = 12 }; // 默认 SkipList 最多 12 个 level

  // Immutable after construction
  Comparator const compare_; // 初始化以后不可更改
  Arena* const arena_;    // Arena used for allocations of nodes 用于分配 node 内存

  Node* const head_; // dummy node

  // Modified only by Insert().  Read racily by readers, but stale
  // values are ok.
  //
  // 原子指针类型。
  // 指向存储当前 skiplist 最大高度的变量的地址，max_height_ <= kMaxHeight。
  // 只能被 Insert() 方法修改。
  // 可以被多个线程使用无内存屏障的方法并发读，即使读取到老的数据也没事。
  port::AtomicPointer max_height_;   // Height of the entire list

  // 获取 SkipList 当前最大高度
  inline int GetMaxHeight() const {
    // 注意这里首先把 void* 重新解释为有符号长整型，然后又强制转换为 int。
    // 最后效果就是把一个指针转换为了整数值。
    return static_cast<int>(
        reinterpret_cast<intptr_t>(max_height_.NoBarrier_Load()));
  }

  // Read/written only by Insert().
  // 该值只会被 Insert() 方法读写。
  Random rnd_;

  Node* NewNode(const Key& key, int height);
  int RandomHeight();
  bool Equal(const Key& a, const Key& b) const { return (compare_(a, b) == 0); }

  // Return true if key is greater than the data stored in "n"
  // 当 key 大于 n 所指节点的 key 时返回 true。
  bool KeyIsAfterNode(const Key& key, Node* n) const;

  // Return the earliest node that comes at or after key.
  // Return nullptr if there is no such node.
  //
  // If prev is non-null, fills prev[level] with pointer to previous
  // node at "level" for every level in [0..max_height_-1].
  //
  // 返回第一个 key 大于等于参数中  key 的 node 的指针；返回 nullptr 意味着不存在此类 nodes.
  //
  // 如果参数 pre 非空，则将所找到的 node 在每一个 level 的前驱节点的 node 的指针赋值到 pre[level]。
  Node* FindGreaterOrEqual(const Key& key, Node** prev) const;

  // Return the latest node with a key < key.
  // Return head_ if there is no such node.
  //
  // 返回最后一个 key 小于参数 key 的 Node 的指针
  Node* FindLessThan(const Key& key) const;

  // Return the last node in the list.
  // Return head_ if list is empty.
  //
  // 返回 skiplist 最后一个 node 的指针，如果 skiplist 为空则返回 head_。
  Node* FindLast() const;

  // No copying allowed
  SkipList(const SkipList&);
  void operator=(const SkipList&);
};

// Implementation details follow
template<typename Key, class Comparator>
struct SkipList<Key,Comparator>::Node {
  explicit Node(const Key& k) : key(k) { }

  Key const key;

  // Accessors/mutators for links.  Wrapped in methods so we can
  // add the appropriate barriers as necessary.
  //
  // 返回该 node 在 level n 的后继节点的指针
  Node* Next(int n) {
    assert(n >= 0);
    // Use an 'acquire load' so that we observe a fully initialized
    // version of the returned Node.
    return reinterpret_cast<Node*>(next_[n].Acquire_Load());
  }
  // 设置该 node 在 level n 的后继节点
  void SetNext(int n, Node* x) {
    assert(n >= 0);
    // Use a 'release store' so that anybody who reads through this
    // pointer observes a fully initialized version of the inserted node.
    next_[n].Release_Store(x);
  }

  // No-barrier variants that can be safely used in a few locations.
  Node* NoBarrier_Next(int n) {
    assert(n >= 0);
    return reinterpret_cast<Node*>(next_[n].NoBarrier_Load());
  }
  void NoBarrier_SetNext(int n, Node* x) {
    assert(n >= 0);
    next_[n].NoBarrier_Store(x);
  }

 private:
  // Array of length equal to the node height.  next_[0] is lowest level link.
  //
  // 长度等于节点高度的数组，next_[0] 存放该 node 在最低 level 上的指向下一个节点的原子指针。
  port::AtomicPointer next_[1];
};

template<typename Key, class Comparator>
typename SkipList<Key,Comparator>::Node*
SkipList<Key,Comparator>::NewNode(const Key& key, int height) {
  char* mem = arena_->AllocateAligned(
      sizeof(Node) + sizeof(port::AtomicPointer) * (height - 1)); // 为啥减 1？因为 Node.next_ 已默认分配了一项
  return new (mem) Node(key); // 定位 new
}

template<typename Key, class Comparator>
inline SkipList<Key,Comparator>::Iterator::Iterator(const SkipList* list) {
  list_ = list;
  node_ = nullptr;
}

template<typename Key, class Comparator>
inline bool SkipList<Key,Comparator>::Iterator::Valid() const {
  return node_ != nullptr;
}

template<typename Key, class Comparator>
inline const Key& SkipList<Key,Comparator>::Iterator::key() const {
  assert(Valid());
  return node_->key;
}

template<typename Key, class Comparator>
inline void SkipList<Key,Comparator>::Iterator::Next() {
  assert(Valid());
  node_ = node_->Next(0); // 因为 level 0 存的是全部 nodes，所以迭代整个 skiplist 时只访问 level 0 即可。
}

template<typename Key, class Comparator>
inline void SkipList<Key,Comparator>::Iterator::Prev() {
  // Instead of using explicit "prev" links, we just search for the
  // last node that falls before key.
  //
  // 注意，Node 结构是没有 pre 指针的，但因为 SkipList nodes 本来就是按序从左到右排列，所以
  // 直接采用二分查找来定位最后一个 key 小于迭代器当前指向的 node 的 key 节点即可。
  assert(Valid());
  node_ = list_->FindLessThan(node_->key);
  if (node_ == list_->head_) {
    node_ = nullptr;
  }
}

// 定位 key >= target 的第一个 node
template<typename Key, class Comparator>
inline void SkipList<Key,Comparator>::Iterator::Seek(const Key& target) {
  node_ = list_->FindGreaterOrEqual(target, nullptr); // 我们只是要查找，后续不做插入，所以第二个用于存储 target 前驱节点的数组为 nullptr
}

template<typename Key, class Comparator>
inline void SkipList<Key,Comparator>::Iterator::SeekToFirst() {
  node_ = list_->head_->Next(0);
}

template<typename Key, class Comparator>
inline void SkipList<Key,Comparator>::Iterator::SeekToLast() {
  node_ = list_->FindLast();
  if (node_ == list_->head_) {
    node_ = nullptr;
  }
}

// 返回一个高度值，返回值介于 [1, kMaxHeight] 之间
template<typename Key, class Comparator>
int SkipList<Key,Comparator>::RandomHeight() {
  // Increase height with probability 1 in kBranching
  // 以 1/kBranching 概率循环递增 height。
  // 只要 kBranching 大于 1，就倾向于返回较小的高度。假设 kBranching == 4，则返回 1 概率为 1/4，返回 2 概率为 1/16，...。
  static const unsigned int kBranching = 4;
  int height = 1;
  while (height < kMaxHeight && ((rnd_.Next() % kBranching) == 0)) {
    height++;
  }
  assert(height > 0);
  assert(height <= kMaxHeight);
  return height;
}

// 判断 key 是否大于 Node n 的 key
// 如果 n 为 nullptr 意味着它的 key 无限大，所以返回 false。
template<typename Key, class Comparator>
bool SkipList<Key,Comparator>::KeyIsAfterNode(const Key& key, Node* n) const {
  // null n is considered infinite
  return (n != nullptr) && (compare_(n->key, key) < 0);
}

// 返回第一个 key 大于等于参数中 key 的 node 的指针；返回 nullptr 意味着全部 nodes 的 key 都小于参数 key.
//
// 如果参数 pre 非空，则将所找到的 node 在每一个 level 的前驱节点的指针赋值到 pre[level]，方便先查找再插入操作。
template<typename Key, class Comparator>
typename SkipList<Key,Comparator>::Node* SkipList<Key,Comparator>::FindGreaterOrEqual(const Key& key, Node** prev)
    const {
  Node* x = head_;
  int level = GetMaxHeight() - 1; // 获取 SkipList 当前的最高的 level，下面找的时候是从最上 level 逐层向下寻找。
  while (true) {
    Node* next = x->Next(level);
    if (KeyIsAfterNode(key, next)) {
      // Keep searching in this list
      x = next; // key 大于 next，继续在该 level 继续向后找
    } else {
      // 如果 key 比 SkipList 中每个 node 的 key 都小，
      // 那么最后返回的的 node 为 head_->Next(0)，同时 pre 里面存的都是 dummy head；
      // 调用者使用返回的 node 的 key 与自己持有 key（internal_key)的进一步进行对比，确定是否找到目标。
      if (prev != nullptr) prev[level] = x;
      if (level == 0) {
        return next; // 就是它！如果 key 比 SkipList 里每个 node 的都大，则 next 最终为 nullptr。
      } else {
        // Switch to next list 确定目标范围，但是粒度太粗，下沉一层继续找
        level--;
      }
    }
  }
}

// 返回最后一个 key 小于参数 key 的 node 的指针
template<typename Key, class Comparator>
typename SkipList<Key,Comparator>::Node*
SkipList<Key,Comparator>::FindLessThan(const Key& key) const {
  Node* x = head_;
  int level = GetMaxHeight() - 1;
  while (true) {
    assert(x == head_ || compare_(x->key, key) < 0);
    Node* next = x->Next(level);
    if (next == nullptr || compare_(next->key, key) >= 0) {
      if (level == 0) {
        return x; // x 就是小于 key 的最后那个 node 了。
      } else {
        // Switch to next list 确定存在目标节点，但是粒度太粗，下沉一层继续找
        level--;
      }
    } else {
      x = next;
    }
  }
}

// 返回 skiplist 最后一个 node 的指针，如果 skiplist 为空则返回 head_。
template<typename Key, class Comparator>
typename SkipList<Key,Comparator>::Node* SkipList<Key,Comparator>::FindLast()
    const {
  Node* x = head_;
  int level = GetMaxHeight() - 1;
  while (true) {
    Node* next = x->Next(level);
    if (next == nullptr) {
      if (level == 0) {
        return x;
      } else {
        // Switch to next list
        level--;
      }
    } else {
      x = next;
    }
  }
}

template<typename Key, class Comparator>
SkipList<Key,Comparator>::SkipList(Comparator cmp, Arena* arena)
    : compare_(cmp),
      arena_(arena),
      head_(NewNode(0 /* any key will do */, kMaxHeight)), // 注意，dummy head 的高度默认为最大
      max_height_(reinterpret_cast<void*>(1)), // 高度初始值为 1
      rnd_(0xdeadbeef) {
  for (int i = 0; i < kMaxHeight; i++) {
    head_->SetNext(i, nullptr);
  }
}

// 该方法非线程安全，需要外部同步设施。
template<typename Key, class Comparator>
void SkipList<Key,Comparator>::Insert(const Key& key) {
  // TODO(opt): We can use a barrier-free variant of FindGreaterOrEqual()
  // here since Insert() is externally synchronized.
  Node* prev[kMaxHeight];
  Node* x = FindGreaterOrEqual(key, prev); // 找到第一个 >= key 的节点，如果为 nullptr 表示都比 key 小

  // Our data structure does not allow duplicate insertion
  // 我们的数据结构不允许重复插入相同 key 的数据项，所以下面断言需要成立
  assert(x == nullptr || !Equal(key, x->key));

  int height = RandomHeight();
  if (height > GetMaxHeight()) {
    for (int i = GetMaxHeight(); i < height; i++) {
      prev[i] = head_; // 新生成了几个 level，key 对应的前驱节点肯定都是 dummy head
    }
    //fprintf(stderr, "Change height from %d to %d\n", max_height_, height);

    // It is ok to mutate max_height_ without any synchronization
    // with concurrent readers.  A concurrent reader that observes
    // the new value of max_height_ will see either the old value of
    // new level pointers from head_ (nullptr), or a new value set in
    // the loop below.  In the former case the reader will
    // immediately drop to the next level since nullptr sorts after all
    // keys.  In the latter case the reader will use the new node.
    //
    // 这里在修改 max_height_ 时候没有插入 barrier。其它并发读线程如果观察到新的 max_height_ 值，
    // 那它们将会要么看到 dummy head 新的层（注意 SkipList 初始化时会把 dummy head 的高度直接初始化为最大，所以不存在越界问题）
    // 的值都为 nullptr，要么看到的是下面循环将要赋值的新节点 x。
    max_height_.NoBarrier_Store(reinterpret_cast<void*>(height));
  }

  x = NewNode(key, height);
  // 将 x 插入到每一层前后节点之间，注意是每一层，
  // 插入的时候都是先采用 no barrier 方式为 x 后继赋值，此时 x 还不会被其它线程看到；
  // 然后插入一个 barrier，则上面 no barrier 的修改针对全部线程都可见了（其中也包括
  // 了 NewNode 时可能发生的通过 NoBarrier_Store 方式修改的 arena_.memory_usage_），
  // 最后修改 x 前驱的后继为自己。
  for (int i = 0; i < height; i++) {
    // NoBarrier_SetNext() suffices since we will add a barrier when
    // we publish a pointer to "x" in prev[i].
    x->NoBarrier_SetNext(i, prev[i]->NoBarrier_Next(i));
    prev[i]->SetNext(i, x);
  }
}

template<typename Key, class Comparator>
bool SkipList<Key,Comparator>::Contains(const Key& key) const {
  Node* x = FindGreaterOrEqual(key, nullptr);
  if (x != nullptr && Equal(key, x->key)) {
    return true;
  } else {
    return false;
  }
}

}  // namespace leveldb

#endif  // STORAGE_LEVELDB_DB_SKIPLIST_H_

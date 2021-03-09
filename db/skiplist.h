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

// 线程安全相关说明: 
// - 写操作需要外部同步设施, 比如 mutex. 
// - 读操作需要一个保证, 即读操作执行期间, SkipList 不能被销毁; 只要保证这一点, 读操作不需要额外的同步措施. 
//
// 不变式: 
// - (1)已分配的 nodes 直到 SkipList 被销毁才能被删除. 这很容易保证, 因为我们不会删除任何 skip list nodes. 
// - (2)一个 Node 一旦被链接到 SkipList 上, 那这个 Node 的内容, 除了 next/pre 指针以外, 都是 immutable 的. 
//
// 只有 Insert() 方法才会修改 SkipList. 而且, 初始化一个 node, 或者使用 release-store 来发布 node 到一个或
// 多个 SkipList 时要格外小心. 
template<typename Key, class Comparator>
class SkipList {
 private:
  // 前向声明
  struct Node;

 public:
  // Create a new SkipList object that will use "cmp" for comparing keys,
  // and will allocate memory using "*arena".  Objects allocated in the arena
  // must remain allocated for the lifetime of the skiplist object.
  //
  // cmp 用于比较 keys, arena 用做内存池. 
  // 从 arena 分配的对象生命期要与 skiplist 实例一致. 
  explicit SkipList(Comparator cmp, Arena* arena);

  // Insert key into the list.
  // REQUIRES: nothing that compares equal to key is currently in the list.
  //
  // 将 key 插入到 skiplist 实例中. 
  // 要求:  skiplist 中当前不存在等于 key 的节点(我们的数据结构不允许出现同一个 key 的数据项). 
  void Insert(const Key& key);

  // Returns true iff an entry that compares equal to key is in the list.
  //
  // 当且仅当 sliplist 中存在与 key 相等的数据项时才返回 true. 
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
    // 当且仅当迭代器指向有效的 node 时才返回 true. 
    bool Valid() const;

    // Returns the key at the current position.
    // REQUIRES: Valid()
    //
    // 返回迭代器当前位置的 key. 
    // 要求: 当前迭代器有效. 
    const Key& key() const;

    // Advances to the next position.
    // REQUIRES: Valid()
    //
    // 将迭代器移动到下个位置. 
    // 要求: 当前迭代器有效. 
    void Next();

    // Advances to the previous position.
    // REQUIRES: Valid()
    //
    // 将迭代器倒退一个位置. 
    // 要求: 当前迭代器有效. 
    void Prev();

    // Advance to the first entry with a key >= target
    //
    // 将迭代器移动到第一个 key >= target 的数据项所在位置. 
    void Seek(const Key& target);

    // Position at the first entry in list.
    // Final state of iterator is Valid() iff list is not empty.
    //
    // 将迭代器移动到 skiplist 第一个数据项所在位置. 
    // 迭代器的最终状态是有效的, 当且仅当 skiplist 不为空. 
    void SeekToFirst();

    // Position at the last entry in list.
    // Final state of iterator is Valid() iff list is not empty.
    //
    // 将迭代器移动到 skiplist 最后一个数据项所在位置. 
    // 迭代器的最终状态是有效的, 当且仅当 skiplist 不为空. 
    void SeekToLast();

   private:
    const SkipList* list_;
    Node* node_;
    // Intentionally copyable
  };

 private:
  // 默认 SkipList 最多 12 个 level
  enum { kMaxHeight = 12 }; 

  // Immutable after construction
  Comparator const compare_; // 初始化以后不可更改
  Arena* const arena_;    // Arena used for allocations of nodes 用于分配 node 内存

  Node* const head_; // dummy node

  // Modified only by Insert().  Read racily by readers, but stale
  // values are ok.
  //
  // 原子指针类型. 
  // 指向存储当前 skiplist 最大高度的变量的地址, max_height_ <= kMaxHeight. 
  // 只能被 Insert() 方法修改. 
  // 可以被多个线程使用无内存屏障的方法并发读, 即使读取到老的数据也没事. 
  port::AtomicPointer max_height_;   // Height of the entire list

  // 获取 SkipList 当前最大高度
  inline int GetMaxHeight() const {
    // 注意这里首先把 void* 重新解释为有符号长整型, 然后又强制转换为 int. 
    // 最后效果就是把一个指针转换为了整数值. 
    return static_cast<int>(
        reinterpret_cast<intptr_t>(max_height_.NoBarrier_Load()));
  }

  // Read/written only by Insert().
  // 该值只会被 Insert() 方法读写. 
  Random rnd_;

  Node* NewNode(const Key& key, int height);
  int RandomHeight();
  bool Equal(const Key& a, const Key& b) const { return (compare_(a, b) == 0); }

  // Return true if key is greater than the data stored in "n"
  // 当 key 大于 n 所指节点的 key 时返回 true. 
  bool KeyIsAfterNode(const Key& key, Node* n) const;

  // Return the earliest node that comes at or after key.
  // Return nullptr if there is no such node.
  //
  // If prev is non-null, fills prev[level] with pointer to previous
  // node at "level" for every level in [0..max_height_-1].
  //
  // 返回第一个 key 大于等于参数中  key 的 node 的指针; 返回 nullptr 意味着不存在此类 nodes.
  //
  // 如果参数 pre 非空, 则将所找到的 node 在每一个 level 的前驱节点的 node 的指针赋值到 pre[level]. 
  Node* FindGreaterOrEqual(const Key& key, Node** prev) const;

  // Return the latest node with a key < key.
  // Return head_ if there is no such node.
  //
  // 返回最后一个 key 小于参数 key 的 Node 的指针
  Node* FindLessThan(const Key& key) const;

  // Return the last node in the list.
  // Return head_ if list is empty.
  //
  // 返回 skiplist 最后一个 node 的指针, 如果 skiplist 为空则返回 head_. 
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

  // 自带 acquire 语义, 返回该 node 在第 n 级(计数从 0 开始) 索引层的后继节点的指针
  Node* Next(int n) {
    assert(n >= 0);
    // 采用 acquire 语义可以确保如下两点:
    // - 当前线程后续针对 next_[n] 节点的读写不会被重排序到此 load 之前;
    // - 其它线程在此 load 之前针对 next_[n] 节点的全部写操作此时均可见,
    //   并且, 其它线程对其它变量的写操作在其 release 当前变量后对当前线程也是
    //   可见的.
    return reinterpret_cast<Node*>(next_[n].Acquire_Load());
  }

  // 自带 release 语义, 设置该 node 在第 n 级(计数从 0 开始) 索引层的后继节点
  void SetNext(int n, Node* x) {
    assert(n >= 0);
    // 采用 release 语义可以确保如下两点:
    // - 在此 store 之前, 当前线程针对 next_[n] 节点的读写不会被重排序到此 store 之后;
    // - 在此 store 之后, 其它线程针对 next_[n] 节点的读写看到的都是此 store 写入的值,
    //   并且, 执行 store 之前对其它变量的写操作, 执行 store 之后对其它线程也是可见的.
    next_[n].Release_Store(x);
  }

  // 同 Next, 但无同步防护.
  Node* NoBarrier_Next(int n) {
    assert(n >= 0);
    return reinterpret_cast<Node*>(next_[n].NoBarrier_Load());
  }

  // 同 SetNext, 但无同步防护.
  void NoBarrier_SetNext(int n, Node* x) {
    assert(n >= 0);
    next_[n].NoBarrier_Store(x);
  }

 private:
  // Array of length equal to the node height. 
  // next_[0] is lowest level link.
  //
  // 长度等于节点高度的数组, next_[0] 存放该 node 在
  // 最低 level 上的指向下一个节点的原子指针. 
  port::AtomicPointer next_[1];
};

template<typename Key, class Comparator>
typename SkipList<Key,Comparator>::Node*
SkipList<Key,Comparator>::NewNode(const Key& key, int height) {
  // 要分配的空间存储的是用户数据和当前节点在 SkipList 各个索引层的后向指针, 
  // 其中后者是现算出来的.
  char* mem = arena_->AllocateAligned(
      // 为啥减 1? 因为 Node.next_ 已默认分配了一项
      sizeof(Node) + sizeof(port::AtomicPointer) * (height - 1));
  // 此乃定位 new, 即在 mem 指向内存位置创建 Node 对象
  return new (mem) Node(key); 
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
  node_ = node_->Next(0); // 因为 level 0 存的是全部 nodes, 所以迭代整个 skiplist 时只访问 level 0 即可. 
}

template<typename Key, class Comparator>
inline void SkipList<Key,Comparator>::Iterator::Prev() {
  // Instead of using explicit "prev" links, we just search for the
  // last node that falls before key.
  //
  // 注意, Node 结构是没有 pre 指针的, 但因为 SkipList nodes 本来就是按序从左到右排列, 所以
  // 直接采用二分查找来定位最后一个 key 小于迭代器当前指向的 node 的 key 节点即可. 
  assert(Valid());
  node_ = list_->FindLessThan(node_->key);
  if (node_ == list_->head_) {
    node_ = nullptr;
  }
}

// 定位 key >= target 的第一个 node
template<typename Key, class Comparator>
inline void SkipList<Key,Comparator>::Iterator::Seek(const Key& target) {
  // 我们只是要查找, 后续不做插入, 所以第二个用于存储 target 前驱节点的数组为 nullptr
  node_ = list_->FindGreaterOrEqual(target, nullptr); 
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

// 返回一个高度值, 返回值落于 [1, kMaxHeight], 
// SkipList 实现默认索引层最多 12 个.
template<typename Key, class Comparator>
int SkipList<Key,Comparator>::RandomHeight() {
  // 以 1/kBranching 概率循环递增 height. 
  // 每次拔擢都是在前一次拔擢成功的前提下再进行, 如果前一次失败则停止拔擢. 
  // 假设 kBranching == 4, 则返回 1 概率为 1/4, 返回 2 概率为 1/16, .... 
  static const unsigned int kBranching = 4;
  // 每个节点最少有一层索引(就是原始链表)
  int height = 1;
  while (height < kMaxHeight && ((rnd_.Next() % kBranching) == 0)) {
    height++;
  }
  assert(height > 0);
  assert(height <= kMaxHeight);
  return height;
}

// 如果 key 在 node 后面(即比它的 key 大)则返回 true; 否则返回 false.
// 如果 n 为 nullptr 意味着它的 key 无限大, 所以返回 false. 
template<typename Key, class Comparator>
bool SkipList<Key,Comparator>::KeyIsAfterNode(const Key& key, Node* n) const {
  // null n is considered infinite
  // 这里的 compare_ 请见 leveldb::InternalKeyComparator::Compare 实现.
  return (n != nullptr) && (compare_(n->key, key) < 0);
}

// 返回第一个大于等于目标 key 的 node 的指针; 
// 返回 nullptr 意味着全部 nodes 的 key 都小于参数 key.
//
// 如果想获取目标节点的前驱, 则令参数 pre 非空, 
// 所找到的 node 所在索引层的前驱节点将被保存到 pre[] 对应层.
template<typename Key, class Comparator>
typename SkipList<Key,Comparator>::Node* 
SkipList<Key,Comparator>::FindGreaterOrEqual(const Key& key, Node** prev)
    const {
  // head_ 为 SkipList 原始数据链表的起始节点,
  // 该节点不存储用户数据, 仅用作哨兵.
  Node* x = head_;
  // 每次查找都是从最高索引层开始查找, 只要确认可能存在
  // 才会降到下一级更细致索引层继续查找.
  // 索引层计数从 0 开始, 所以这里减一才是最高层.
  int level = GetMaxHeight() - 1; 
  while (true) {
    // 下面用的 Next 方法是带同步设施的, 其实由于 SkipList 对外开放的操作
    // 需要调用者自己提供同步, 所以这里可以直接用 NoBarrier_Next.
    Node* next = x->Next(level);
    if (KeyIsAfterNode(key, next)) {
      // key 大于 next, 在该索引层继续向后找
      x = next; 
    } else {
      // key 可能存在.
      //
      // 如果 key 比 SkipList 中每个 node 的 key 都小, 
      // 那么最后返回的 node 为 head_->Next(0), 
      // 同时 pre 里面存的都是 dummy head; 
      // 调用者需要使用返回的 node 与自己持有 key进一步进行对比,
      // 以确定是否找到目标节点. 
      if (prev != nullptr) prev[level] = x;
      if (level == 0) {
        // 就是它！如果 key 比 SkipList 里每个 node 的都大, 则 next 最终为 nullptr.
        return next;  
      } else {
        // 确定目标范围, 但是粒度太粗, 下沉一层继续找
        level--;
      }
    }
  }
}

// 返回最后一个小于 key 的 node 的指针
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
        return x; // x 就是小于 key 的最后那个 node 了. 
      } else {
        // Switch to next list 
        // 确定存在目标节点, 但是粒度太粗, 下沉一层继续找
        level--;
      }
    } else {
      x = next;
    }
  }
}

// 返回 skiplist 最后一个 node 的指针, 如果 skiplist 为空则返回 head_. 
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
      head_(NewNode(0 /* any key will do */, kMaxHeight)), // 注意, dummy head 的高度默认为最大
      max_height_(reinterpret_cast<void*>(1)), // 高度初始值为 1
      rnd_(0xdeadbeef) {
  for (int i = 0; i < kMaxHeight; i++) {
    head_->SetNext(i, nullptr);
  }
}

// 该方法非线程安全, 需要外部同步设施. 
template<typename Key, class Comparator>
void SkipList<Key,Comparator>::Insert(const Key& key) {
  // pre 将用于存储 key 对应的各个索引层的前驱节点
  Node* prev[kMaxHeight];
  // 找到第一个大约等于目标 key 的节点, 一会会把 key
  // 插到这个节点前面.
  // 如果为 nullptr 表示当前 SkipList 节点都比 key 小.
  Node* x = FindGreaterOrEqual(key, prev); 

  // 虽然 x 是我们找到的第一个大于等于目标 key 的节点, 
  // 但是 leveldb 不允许重复插入 key 相等的数据项.
  assert(x == nullptr || !Equal(key, x->key));

  // 确定待插入节点的最大索引层数
  int height = RandomHeight();
  // 更新 SkipList 实例维护的最大索引层数
  if (height > GetMaxHeight()) {
    // 如果最大索引层数有变, 则当前节点将是索引层数最多的节点,
    // 需要将前面求得的待插入节点的前驱节点高度补齐.
    for (int i = GetMaxHeight(); i < height; i++) {
      // 新生成了几个 level, key 对应的前驱节点肯定都是 dummy head
      prev[i] = head_; 
    }
    //fprintf(stderr, "Change height from %d to %d\n", max_height_, height);

    // 这里在修改 max_height_ 无需同步, 哪怕同时有多个并发读线程. 
    // 其它并发读线程如果观察到新的 max_height_ 值, 
    // 那它们将会要么看到 dummy head 新的索引层(注意 SkipList 
    // 初始化时会把 dummy head 的索引高度直接初始化为最大, 默认是 12, 
    // 所以不存在越界问题)的值都为 nullptr, 要么看到的是
    // 下面循环将要赋值的新节点 x. 
    max_height_.NoBarrier_Store(reinterpret_cast<void*>(height));
  }

  // 为待插入数据创建一个新节点
  x = NewNode(key, height);
  // 将 x 插入到每一层前后节点之间, 注意是每一层, 
  // 插入的时候都是先采用 no barrier 方式为 x 后继赋值, 此时 x 还不会被其它线程看到; 
  // 然后插入一个 barrier, 则上面 no barrier 的修改针对全部线程都可见了(其中也包括
  // 了 NewNode 时可能发生的通过 NoBarrier_Store 方式修改的 arena_.memory_usage_), 
  // 最后修改 x 前驱的后继为自己. 
  for (int i = 0; i < height; i++) {
    // 注意该循环就下面两步, 而且只有第二步采用了同步设施, 尽管如此,
    // 第一步的写操作对其它线程也是可见的. 
    // 这是 Release-Acquire ordering 语义所保证的. 
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

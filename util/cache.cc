// Copyright (c) 2011 The LevelDB Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file. See the AUTHORS file for names of contributors.

#include <assert.h>
#include <stdio.h>
#include <stdlib.h>

#include "leveldb/cache.h"
#include "port/port.h"
#include "port/thread_annotations.h"
#include "util/hash.h"
#include "util/mutexlock.h"

namespace leveldb {

Cache::~Cache() {
}
/**
 * leveldb namespace 内部嵌套了一个匿名 namespace, 后者里面定义了一个 Cache 的具体实现; 
 * 通过匿名 namespace 可以避免外部直接引用这个具体 Cache 类. 
 *
 * 匿名空间结束后会定义一个 NewLRUCache 方法用于生成一个指向 Cache 实现类的指针. 
 */
namespace {

// LRU cache implementation
//
// Cache entries have an "in_cache" boolean indicating whether the cache has a
// reference on the entry.  The only ways that this can become false without the
// entry being passed to its "deleter" are via Erase(), via Insert() when
// an element with a duplicate key is inserted, or on destruction of the cache.
//
// The cache keeps two linked lists of items in the cache.  All items in the
// cache are in one list or the other, and never both.  Items still referenced
// by clients but erased from the cache are in neither list.  The lists are:
// - in-use:  contains the items currently referenced by clients, in no
//   particular order.  (This list is used for invariant checking.  If we
//   removed the check, elements that would otherwise be on this list could be
//   left as disconnected singleton lists.)
// - LRU:  contains the items not currently referenced by clients, in LRU order
// Elements are moved between these lists by the Ref() and Unref() methods,
// when they detect an element in the cache acquiring or losing its only
// external reference.
/**
 * 采用 LRU 算法实现的 cache. 
 *
 * cache 每一个数据项都包含一个布尔类型的 in_cache 变量用于指示该 cache 是否有一个指向数据项的引用. 
 * 除了将数据项传给它的 deleter 以外, 该变量变为 false 的方式为调用 Erase()、或者在 key 存在时又调用 Insert() 插入具有同样的 key 的映射、
 * 或者 cache 析构时. 
 *
 * cache 持有两个包含数据项的链表, 某个数据项若存在则只包含在其中某一个链表中, 被客户端引用但已经被 Erase() 的数据项不在任何一个链表中. 
 *
 * 这两个链表的状态为：
 *
 * - in-use：包含当前被客户端引用的全部数据项, 无序排列. (这个链表用于不变式检查. 如果我们移除了这个检查, 
 *   仍然存在于这个链表中的元素可以从该链表剥离构成一个单例链表. )
 * - LRU：包含当前已经不被客户端引用的全部数据项, 以 LRU 顺序排列. 
 *
 * 当 Ref() 和 Unref() 这两个方法检测到 cache 中某个元素获取到或者丢失了它的外部引用的时候, 可以在这些链表中对该数据项进行移动. 
 */

// An entry is a variable length heap-allocated structure.  Entries
// are kept in a circular doubly linked list ordered by access time.
/*
 * 定义 cache 中存储的数据项结构, 一个数据项是一个变长的基于堆内存的数据结构. 
 *
 * 全部数据项按照访问时间被保存在一个双向循环链表里. 
 */
struct LRUHandle {
  void* value;
  void (*deleter)(const Slice&, void* value);
  LRUHandle* next_hash; // 用于在哈希表中指向与自己同一个桶中的后续元素
  LRUHandle* next;
  LRUHandle* prev;
  size_t charge;      // TODO(opt): Only allow uint32_t?
  size_t key_length;
  bool in_cache;      // Whether entry is in the cache. 指示该数据项是否还在 cache 中. 
  uint32_t refs;      // References, including cache reference, if present. 该数据项引用数. 
  uint32_t hash;      // Hash of key(); used for fast sharding and comparisons key() 返回值的哈希值, 用于快速分片和比较. 
  char key_data[1];   // Beginning of key key 的起始字符, 注意这个地方有个 trick, 就是因为 key 本来是变长的, 所以这里需要将 key_data 作为本数据结构最后一个元素, 方便根据实际情况延伸. 

  Slice key() const {
    // next_ is only equal to this if the LRU handle is the list head of an
    // empty list. List heads never have meaningful keys.
    assert(next != this); // 注意, 数据项所在循环链表的 head 不保存任何数据项, 如果 next == this 表示为只包含 head 的空链表. 

    return Slice(key_data, key_length);
  }
};

// We provide our own simple hash table since it removes a whole bunch
// of porting hacks and is also faster than some of the built-in hash
// table implementations in some of the compiler/runtime combinations
// we have tested.  E.g., readrandom speeds up by ~5% over the g++
// 4.4.3's builtin hashtable.
/**
 * 我们自己实现了一个简单的哈希表, 因为它移除了全部跟可移植性相关的处理, 而且通过测试发现它比很多编译器或运行时内置的哈希表实现更快, 
 * 比如, 随机读比 g++ 4.4.3 版本的内置 hashtable 快了大约 5%.
 *
 * 该哈希表基于拉链法实现, 哈希表包含一组桶, 每个桶包含一个链表用于处理哈希冲突. 
 *
 * 该哈希表会用于 cache, 用于快速查询某个数据项, 数据项就是 LRUHandle 对象. 
 */
class HandleTable {
 public:
  HandleTable() : length_(0), elems_(0), list_(nullptr) { Resize(); }
  ~HandleTable() { delete[] list_; }

  /**
   * 根据数据项的 key 和哈希值在哈希表中查找对应数据项
   * @param key 要查询数据项的 key
   * @param hash 要查询数据项的哈希值
   * @return 指向数据项的指针
   */
  LRUHandle* Lookup(const Slice& key, uint32_t hash) {
    return *FindPointer(key, hash);
  }

  /**
   * 将某个数据项插入到哈希表中
   * @param h 指向待插入数据项的指针
   * @return 如果哈希表已经存在与 h 所指数据项相同的 key 和 hash 的 old, 则用 h 指向是的数据项替换 old, 返回 old; 
   *        如果不存在, 则插入 h 返回 nullptr. 
   */
  LRUHandle* Insert(LRUHandle* h) {
    LRUHandle** ptr = FindPointer(h->key(), h->hash);
    LRUHandle* old = *ptr;
    // 如果 old 不为 nullptr, 则说明哈希表已经存在要插入的数据项 h 了, 虽然 old 的 key 和 hash 与 h 一样, 但是 value 可能不一样, 
    // 用 h 指向的数据项替换 old 指向的数据项
    h->next_hash = (old == nullptr ? nullptr : old->next_hash);
    *ptr = h; // 将 h 替换 old(如果 old 不为 nullptr)或者将 h 作为桶首元素或者最后一个元素桶对应的链表(此时 old 为 nullptr)
    if (old == nullptr) { // h 不在哈希表中
      ++elems_;
      if (elems_ > length_) { // 用于控制哈希表的负载因子(即 elems_ / length_)不超过 1, 如果超过则扩大哈希表并进行 rehash
        // Since each cache entry is fairly large, we aim for a small
        // average linked list length (<= 1).
        Resize();
      }
    }
    return old;
  }

  /**
   * 删除 key 和 hash 都相等的数据项
   * @param key 要删除的数据项的 key
   * @param hash 要删除的数据项的 hash
   * @return 如果该数据项存在, 则删除并返回它的指针; 否则返回 nullptr. 
   */
  LRUHandle* Remove(const Slice& key, uint32_t hash) {
    LRUHandle** ptr = FindPointer(key, hash);
    LRUHandle* result = *ptr;
    if (result != nullptr) {
      *ptr = result->next_hash; // 将要删除的 result 同一个桶里的下一个数据项替换 result
      --elems_;
    }
    return result;
  }

 private:
  // The table consists of an array of buckets where each bucket is
  // a linked list of cache entries that hash into the bucket.
  uint32_t length_; // 桶的个数
  uint32_t elems_; // 哈希表中包含的元素的个数
  LRUHandle** list_; // 指向一组桶; 每个桶是一个指针, 指向所含链表的首元素. 

  // Return a pointer to slot that points to a cache entry that
  // matches key/hash.  If there is no such cache entry, return a
  // pointer to the trailing slot in the corresponding linked list.
  /**
   * 在哈希表里查找 key 和 hash 都相等的数据项的指针的地址(一个二级地址, 方便后续的修改)
   * @param key 要查找的数据项的 key
   * @param hash 要查找的数据项的 hash
   * @return 如果找到, 则返回对应数据项的指针的地址; 
   *        如果找不到, 要么返回桶首元素应存储到的位置(此时为空桶), 
   *            要么返回桶最后一个元素的 next_hash 的地址(桶不为空但没找到, 注意不是 next_hash 保存的地址, 此时 next_hash 保存的 nullptr(见 insert 方法)). 
   */
  LRUHandle** FindPointer(const Slice& key, uint32_t hash) {
    LRUHandle** ptr = &list_[hash & (length_ - 1)]; // 通过取模获取目标桶首元素的指针的地址
    while (*ptr != nullptr &&
           ((*ptr)->hash != hash || key != (*ptr)->key())) { // 在桶里根据 hash 和 key 寻找数据项, hash 比较好比较, 不相等再比较字符串
      ptr = &(*ptr)->next_hash;
    }
    return ptr;
  }

  /**
   * 将哈希表扩大并进行 rehash, 确保负载因子(即 elem_ / length_)不超过 1
   */
  void Resize() {
    uint32_t new_length = 4;
    while (new_length < elems_) { // 哈希表 resize 后, 桶的个数是现有元素数的 1.x 倍
      new_length *= 2;
    }
    LRUHandle** new_list = new LRUHandle*[new_length];
    memset(new_list, 0, sizeof(new_list[0]) * new_length);
    uint32_t count = 0;
    for (uint32_t i = 0; i < length_; i++) { // 遍历每个桶, 针对非空桶, 将桶里的元素进行 rehash
      LRUHandle* h = list_[i]; // 指向第 i 个桶的首元素的指针
      while (h != nullptr) {
        LRUHandle* next = h->next_hash; // 与 h 在同一个桶里的下一个元素
        uint32_t hash = h->hash;
        LRUHandle** ptr = &new_list[hash & (new_length - 1)]; // 将 h 进行 rehash
        h->next_hash = *ptr;
        *ptr = h; // 将 h 作为该桶首元素(下一次循环会被哈希到同一个桶里的新 h 挤到后面)
        h = next; // 将 h 指向老哈希表与自己同一个桶的下一个元素
        count++; // 新表元素个数加 1
      }
    }
    assert(elems_ == count);
    delete[] list_;
    list_ = new_list;
    length_ = new_length;
  }
};

// A single shard of sharded cache.
/**
 * 定义了 sharded cache 的 shard, 下面会定义 ShardedLRUCache. 
 *
 * 注意包括两个循环链表, 一个是 in_use_ 链表, 一个是 lru_ 链表, 同时还有一个用于快速查询数据项是否存在的哈希表. 
 * 前者 in_use_ 存的是在用的数据项, lru_ 存的是可以淘汰以节省空间也可以重新提升到 in_use_ 中的数据项, 
 * 哈希表存储了出现在 in_use_ 和 lru_ 中的全部数据项(用于快速判断某个数据项是否在 LRUCache 中). 
 */
class LRUCache {
 public:
  LRUCache();
  ~LRUCache();

  // Separate from constructor so caller can easily make an array of LRUCache
  void SetCapacity(size_t capacity) { capacity_ = capacity; }

  // Like Cache methods, but with an extra "hash" parameter.
  /**
   * 该方法线程安全, 因为同时可能由其它线程进行影响该 shard 状态的操作. 
   *
   * 插入一个数据项, 类似于 Cache 类的方法, 但是多了一个额外的 hash 参数. 
   * @param key 要插入的数据项的 key
   * @param hash 要插入的数据项的 hash
   * @param value 要插入的数据项的 value
   * @param charge 要插入的数据项的 charge
   * @param deleter 要插入的数据项的 deleter
   * @return 返回插入的数据项的句柄
   */
  Cache::Handle* Insert(const Slice& key, uint32_t hash,
                        void* value, size_t charge,
                        void (*deleter)(const Slice& key, void* value));
  Cache::Handle* Lookup(const Slice& key, uint32_t hash);
  void Release(Cache::Handle* handle);
  void Erase(const Slice& key, uint32_t hash);
  void Prune();
  /**
   * 返回该 shard 的使用量
   * @return
   */
  size_t TotalCharge() const {
    MutexLock l(&mutex_);
    return usage_;
  }

 private:
  // 下面 5 个私有方法都要修改本 shard 的状态, 所以需要进行同步, 虽然这几个方法内部没有采用同步设施, 但是调用它们的方法都进行了恰当的同步. 
  void LRU_Remove(LRUHandle* e);
  void LRU_Append(LRUHandle*list, LRUHandle* e);
  void Ref(LRUHandle* e);
  void Unref(LRUHandle* e);
  bool FinishErase(LRUHandle* e) EXCLUSIVE_LOCKS_REQUIRED(mutex_);

  // Initialized before use.
  size_t capacity_; // 该 shard 的容量, 可以通过 SetCapacity 进行动态调整

  // mutex_ protects the following state. 针对下面的状态变量的访问将会被 mutex 守护
  mutable port::Mutex mutex_; // 允许在 const 成员方法中修改该成员变量
  size_t usage_ GUARDED_BY(mutex_); // shard 使用量

  // Dummy head of LRU list.
  // lru.prev is newest entry, lru.next is oldest entry.
  // Entries have refs==1 and in_cache==true.
  // lru 链表的 dummy head(不保存任何数据项). 
  // 这个链表的数据项的引用数肯定为 1, 而且 in_cache 标识肯定为 true, 
  // 因为这个链表的数据项当前肯定实在 shard 中, 而且只被当前 shard 引用, 
  // 他们要么即将被彻底淘汰要么等着被提升到 in_use 链表. 
  LRUHandle lru_ GUARDED_BY(mutex_);

  // Dummy head of in-use list.
  // Entries are in use by clients, and have refs >= 2 and in_cache==true.
  // in_use 链表的 dummy head(不保存任何数据项). 
  // in_use 链表存放被客户端使用的全部数据项, 
  // 并且每个数据项的 refs >= 2(最少被一个客户端引用同时还被 shard 引用, 所以至少为 2) 且 in_cache == true. 
  LRUHandle in_use_ GUARDED_BY(mutex_);

  // 哈希表, 存放 in_use 链表和 lru 链表数据项的指针, 用于快速查询数据项是否在该 shard 中. 
  HandleTable table_ GUARDED_BY(mutex_);
};

LRUCache::LRUCache()
    : usage_(0) {
  // Make empty circular linked lists.
  lru_.next = &lru_;
  lru_.prev = &lru_;
  in_use_.next = &in_use_;
  in_use_.prev = &in_use_;
}

/**
 * 析构前要确保 in_use 链表为空了, 否则说明还有数据项仍被客户端持有. 
 */
LRUCache::~LRUCache() {
  assert(in_use_.next == &in_use_);  // Error if caller has an unreleased handle
  for (LRUHandle* e = lru_.next; e != &lru_; ) {
    LRUHandle* next = e->next;
    assert(e->in_cache);
    e->in_cache = false;
    assert(e->refs == 1);  // Invariant of lru_ list.
    Unref(e);
    e = next;
  }
}

/**
 * 负责将数据项引用数加 1, 如果有必要的话(即 e->refs == 1 && e->in_cache)将其从 lru 链表移动到 in_use 链表. 
 * @param e
 */
void LRUCache::Ref(LRUHandle* e) {
  if (e->refs == 1 && e->in_cache) {  // If on lru_ list, move to in_use_ list.
    LRU_Remove(e);
    LRU_Append(&in_use_, e);
  }
  e->refs++;
}

/**
 * 将 e 指向的数据项的引用数减 1, 然后根据引用数决定释放释放其所占空间. 
 *
 * 如果 e 引用数变为 0, 则调用它对应的 deleter 释放之, 然后 free e 占用的内存; 
 * 如果 e 引用数变为 1(表示当前除了 cache 对象没有任何客户端引用它了) 且其 in_cache 状态为 true, 则将其从 in_use 链表移动到 lru 链表; 
 * 其它情况什么也不做, 也不该发生. 
 * @param e
 */
void LRUCache::Unref(LRUHandle* e) {
  assert(e->refs > 0);
  e->refs--;
  if (e->refs == 0) {  // Deallocate.
    assert(!e->in_cache);
    (*e->deleter)(e->key(), e->value);
    free(e);
  } else if (e->in_cache && e->refs == 1) {
    // No longer in use; move to lru_ list.
    LRU_Remove(e);
    LRU_Append(&lru_, e);
  }
}

/**
 * 将数据项 e 从当前所在链表移除. 
 *
 * 注意, 虽然该表以 LRU 开头, 但它的意思不是指操作 lru 表(也可能操作 in_use 表)而是指用 LRU 算法进行操作. 
 * @param e 要移除的数据项
 */
void LRUCache::LRU_Remove(LRUHandle* e) {
  e->next->prev = e->prev;
  e->prev->next = e->next;
}

/**
 * 将 e 追加到 shard 的链表 list 中, 具体哪个表取决于具体参数. 
 * 追加后的效果为 e 变为该链表的首元素. 
 *
 * 注意, 虽然该表以 LRU 开头, 但它的意思不是指操作 lru 表(也可能操作 in_use 表)而是指用 LRU 算法进行操作. 
 * @param list 要追加到的链表 in_use 或者 lru
 * @param e 要追加的数据项
 */
void LRUCache::LRU_Append(LRUHandle* list, LRUHandle* e) {
  // Make "e" newest entry by inserting just before *list
  e->next = list;
  e->prev = list->prev;
  e->prev->next = e;
  e->next->prev = e;
}

/**
 * 该方法线程安全, 因为同时可能由其它线程进行影响该 shard 状态的操作. 
 *
 * 在该 shard 中查询具有给定 key 和 hash 的数据项
 * @param key 要查询的数据项的 key
 * @param hash 要查询的数据项的 hash
 * @return 要查询的数据项的指针; 如果不存在返回 nullptr. 
 */
Cache::Handle* LRUCache::Lookup(const Slice& key, uint32_t hash) {
  MutexLock l(&mutex_);
  LRUHandle* e = table_.Lookup(key, hash); // table_ 是个哈希表, 存储了该 shard 全部数据项的指针, 查询快. 
  if (e != nullptr) {
    Ref(e); // 如果查到, 则将该数据项引用数加 1, 因为要 return 给外部调用者. 
  }
  return reinterpret_cast<Cache::Handle*>(e);
}

/**
 * 该方法线程安全, 因为同时可能由其它线程进行影响该 shard 状态的操作. 
 *
 * 释放数据项 handle, 如果该数据项不再使用, 则将其从 lru 链表删除(如果该数据项之前在 in_use 链表则将其移动到 lru 链表). 
 * @param handle
 */
void LRUCache::Release(Cache::Handle* handle) {
  MutexLock l(&mutex_);
  Unref(reinterpret_cast<LRUHandle*>(handle));
}

/**
 * 该方法线程安全, 因为同时可能由其它线程进行影响该 shard 状态的操作. 
 *
 * 插入一个数据项, 类似于 Cache 类的方法, 但是多了一个额外的 hash 参数. 
 * @param key 要插入的数据项的 key
 * @param hash 要插入的数据项的 hash
 * @param value 要插入的数据项的 value
 * @param charge 要插入的数据项的 charge
 * @param deleter 要插入的数据项的 deleter
 * @return 返回插入的数据项的句柄
 */
Cache::Handle* LRUCache::Insert(
    const Slice& key, uint32_t hash, void* value, size_t charge,
    void (*deleter)(const Slice& key, void* value)) {
  MutexLock l(&mutex_);

  // 根据实际情况, 主要是 key 的长度, 来分配空间. 
  // 减掉的 1 指的是 key_data[1] 所占的空间, 不减掉的话后面加上 key.size() 就多了一个字节. 
  LRUHandle* e = reinterpret_cast<LRUHandle*>(
      malloc(sizeof(LRUHandle)-1 + key.size()));
  e->value = value;
  e->deleter = deleter;
  e->charge = charge;
  e->key_length = key.size();
  e->hash = hash;
  e->in_cache = false;
  e->refs = 1;  // for the returned handle. 因为 e 后面会被该方法返回, 所以将引用数加 1.
  memcpy(e->key_data, key.data(), key.size());

  if (capacity_ > 0) {
    // 本 shard 容量大于 0, 将 e 引用数加 1, 表示被本 shard 引用. 
    e->refs++;  // for the cache's reference.
    // 该数据项被放到了 shard 中
    e->in_cache = true;
    // 将该数据项追加到 shard 的 in_use 链表
    LRU_Append(&in_use_, e);
    usage_ += charge;
    // 如果 shard 中存在与 e 相同的 key 相同的 hash 的项, 则将其从 shard 彻底删除
    FinishErase(table_.Insert(e));
  } else {  // don't cache. (capacity_==0 is supported and turns off caching.)
    // next is read by key() in an assert, so it must be initialized
    // 如果容量 <= 0 不缓存该数据项, 此时 cache 被关闭了. 此处的赋值是防止 key() 方法的 assert 失败. 
    e->next = nullptr;
  }
  // 如果本 shard 的使用量大于容量并且 lru 链表不为空, 则从 lru 链表里面淘汰数据项, lru 链表数据肯定不再使用了. 
  while (usage_ > capacity_ && lru_.next != &lru_) {
    LRUHandle* old = lru_.next;
    // lru 链表里面的数据项除了被该 shard 引用不会被任何客户端引用
    assert(old->refs == 1);
    // 从 shard 将 old 彻底删除
    bool erased = FinishErase(table_.Remove(old->key(), old->hash));
    if (!erased) {  // to avoid unused variable when compiled NDEBUG
      assert(erased);
    }
  }

  // 将 LRUHandle 重新解释为 Cache::Handle
  return reinterpret_cast<Cache::Handle*>(e);
}

// If e != nullptr, finish removing *e from the cache; it has already been
// removed from the hash table.  Return whether e != nullptr.
/**
 * 从 shard 彻底移除 e. 
 *
 * 即执行完该方法后, 哈希表 table_、in_use 链表、lru 链表都不包含该数据项, 且它所占内存也被释放掉了. 
 *
 * 注意, 调用该方法之前, e 一定是先从哈希表 table_ 移除过了. 
 * @param e 要从 shard 中移除的 e
 * @return 只有 e 不为 nullptr 才会移除成功, 否则看做失败. 
 */
bool LRUCache::FinishErase(LRUHandle* e) {
  if (e != nullptr) {
    assert(e->in_cache); // 移除前 e 肯定是在 cache 中
    LRU_Remove(e); // 将 e 从当前所处的 shard 链表移除, 此时它一定在 lru 链表里面, 否则就出 bug 了. 
    e->in_cache = false; // 标记 e 已从 cache 移除
    usage_ -= e->charge; // cache 使用量因为移除 e 变小了
    Unref(e); // 将 e 引用计数递减并将其所占内存释放
  }
  return e != nullptr;
}

/**
 * 该方法线程安全, 因为同时可能由其它线程进行影响该 shard 状态的操作. 
 *
 * 将具有 key 和 hash 的数据项从 shard 彻底移除. 
 * @param key 要彻底移除的数据项的 key
 * @param hash 要彻底移除的数据项的 hash
 */
void LRUCache::Erase(const Slice& key, uint32_t hash) {
  MutexLock l(&mutex_);
  FinishErase(table_.Remove(key, hash));
}

/**
 * 该方法线程安全, 因为同时可能由其它线程进行影响该 shard 状态的操作. 
 *
 * 清空 lru 链表释放空间, lru 链表里的数据项当前肯定未被客户端引用. 
 */
void LRUCache::Prune() {
  MutexLock l(&mutex_);
  while (lru_.next != &lru_) {
    LRUHandle* e = lru_.next;
    assert(e->refs == 1); // lru 链表中的数据项引用数肯定为 1, 因为这个链表的数据要么即将被彻底淘汰要么等着被提升到 in_use 链表. 
    bool erased = FinishErase(table_.Remove(e->key(), e->hash)); // 先将该数据项从哈希表 table_ 删除, 以防查询时出现不一致行为
    if (!erased) {  // to avoid unused variable when compiled NDEBUG
      assert(erased);
    }
  }
}

static const int kNumShardBits = 4;
static const int kNumShards = 1 << kNumShardBits; // 默认 cache 具有 16 个 shards

/**
 * 一个基于 LRU 算法的并且支持 shard 的 Cache 的具体实现
 */
class ShardedLRUCache : public Cache {
 private:
  // shard 数组, 每个 shard 就是一个 LRUCache
  LRUCache shard_[kNumShards];
  // 在生成新 id 时用于同步
  port::Mutex id_mutex_;
  // 最新的 id
  uint64_t last_id_;

  /**
   * 使用数据项的 key 计算数据项的 hash
   * @param s
   * @return
   */
  static inline uint32_t HashSlice(const Slice& s) {
    return Hash(s.data(), s.size(), 0);
  }

  /**
   * 根据数据项的 hash 计算该数据项应该存放的 shard
   * @param hash
   * @return
   */
  static uint32_t Shard(uint32_t hash) {
    return hash >> (32 - kNumShardBits);
  }

 public:
  /**
   * 构造方法, 设置本 Cache 的容量, 将 id 初始值设置为 0.
   * @param capacity
   */
  explicit ShardedLRUCache(size_t capacity)
      : last_id_(0) {
    // 令 capacity + (kNumShards - 1) 可以确保各个 shards 总容量不会小于预期值. 
    // 比如, capacity 为 10,  kNumShards 为 3, 如果不做任何处理, 那么每个 shard 分到 3, 那各个 shards 总容量只有 9, 小于我们预期; 
    // 但是如果先加上 (3-1) 再分配, 则每个 shard 容量为 4, 总容量为 12.
    // 加上 (kNumShards - 1) 可以确保原本 capacity / kNumShards 的余数可以分到一个单独的 shard, 
    // 即实现 capacity / kNumShards 向上取整的效果. 
    const size_t per_shard = (capacity + (kNumShards - 1)) / kNumShards;
    for (int s = 0; s < kNumShards; s++) {
      shard_[s].SetCapacity(per_shard);
    }
  }
  virtual ~ShardedLRUCache() { }
  virtual Handle* Insert(const Slice& key, void* value, size_t charge,
                         void (*deleter)(const Slice& key, void* value)) {
    const uint32_t hash = HashSlice(key);
    return shard_[Shard(hash)].Insert(key, hash, value, charge, deleter);
  }
  virtual Handle* Lookup(const Slice& key) {
    const uint32_t hash = HashSlice(key);
    return shard_[Shard(hash)].Lookup(key, hash);
  }
  virtual void Release(Handle* handle) {
    LRUHandle* h = reinterpret_cast<LRUHandle*>(handle);
    shard_[Shard(h->hash)].Release(handle);
  }
  virtual void Erase(const Slice& key) {
    const uint32_t hash = HashSlice(key);
    shard_[Shard(hash)].Erase(key, hash);
  }
  virtual void* Value(Handle* handle) {
    return reinterpret_cast<LRUHandle*>(handle)->value;
  }
  /**
   * 该方法线程安全. 
   *
   * 生成一个新的 id
   * @return
   */
  virtual uint64_t NewId() {
    MutexLock l(&id_mutex_);
    return ++(last_id_);
  }
  /**
   * 用于清空每个 shard 的 lru 链表, 并释放该链表中每个数据项所占内存空间
   */
  virtual void Prune() {
    for (int s = 0; s < kNumShards; s++) {
      shard_[s].Prune();
    }
  }
  /**
   * 返回该 cache 全部 shards 的使用量之和
   * @return
   */
  virtual size_t TotalCharge() const {
    size_t total = 0;
    for (int s = 0; s < kNumShards; s++) {
      total += shard_[s].TotalCharge();
    }
    return total;
  }
};

}  // end anonymous namespace

Cache* NewLRUCache(size_t capacity) {
  return new ShardedLRUCache(capacity);
}

}  // namespace leveldb

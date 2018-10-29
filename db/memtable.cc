// Copyright (c) 2011 The LevelDB Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file. See the AUTHORS file for names of contributors.

#include "db/memtable.h"
#include "db/dbformat.h"
#include "leveldb/comparator.h"
#include "leveldb/env.h"
#include "leveldb/iterator.h"
#include "util/coding.h"

namespace leveldb {

// 将 data 处数据转换为 Slice 并返回。
// data 存储的数据的格式为 【varint32 格式的数据长度 len，len 个字节的数据]
static Slice GetLengthPrefixedSlice(const char* data) {
  uint32_t len;
  const char* p = data;
  p = GetVarint32Ptr(p, p + 5, &len);  // +5: we assume "p" is not corrupted
  return Slice(p, len); // 拷贝构造，不过还好，都是指针和内置类型拷贝。
}

MemTable::MemTable(const InternalKeyComparator& cmp)
    : comparator_(cmp),
      refs_(0),
      table_(comparator_, &arena_) {
}

MemTable::~MemTable() {
  assert(refs_ == 0); // 断言没有任何其它客户端在引用该 memtable 了
}

size_t MemTable::ApproximateMemoryUsage() { return arena_.MemoryUsage(); }

int MemTable::KeyComparator::operator()(const char* aptr, const char* bptr)
    const {
  // Internal keys are encoded as length-prefixed strings.
  // 获取数据项的前半部分，即 internal_key
  Slice a = GetLengthPrefixedSlice(aptr);
  Slice b = GetLengthPrefixedSlice(bptr);
  // 对 internal_key 进行比较
  return comparator.Compare(a, b);
}

// Encode a suitable internal key target for "target" and return it.
// Uses *scratch as scratch space, and the returned pointer will point
// into this scratch space.
//
// 将 Slice 类型的 internal_key 编码写到起始地址为 scratch 的内存，然后返回起始地址。
static const char* EncodeKey(std::string* scratch, const Slice& target) {
  scratch->clear();
  PutVarint32(scratch, target.size());
  scratch->append(target.data(), target.size());
  return scratch->data();
}

// 一个 wrapper，内部封装了一个干活的 iterator
class MemTableIterator: public Iterator {
 public:
  explicit MemTableIterator(MemTable::Table* table) : iter_(table) { }

  virtual bool Valid() const { return iter_.Valid(); }
  virtual void Seek(const Slice& k) { iter_.Seek(EncodeKey(&tmp_, k)); }
  virtual void SeekToFirst() { iter_.SeekToFirst(); }
  virtual void SeekToLast() { iter_.SeekToLast(); }
  virtual void Next() { iter_.Next(); }
  virtual void Prev() { iter_.Prev(); }
  virtual Slice key() const { return GetLengthPrefixedSlice(iter_.key()); }
  virtual Slice value() const {
    Slice key_slice = GetLengthPrefixedSlice(iter_.key());
    return GetLengthPrefixedSlice(key_slice.data() + key_slice.size());
  }

  virtual Status status() const { return Status::OK(); }

 private:
  MemTable::Table::Iterator iter_;
  std::string tmp_;       // For passing to EncodeKey 用于 EncodeKey 方法存储编码后的 internal_key

  // No copying allowed
  MemTableIterator(const MemTableIterator&);
  void operator=(const MemTableIterator&);
};

Iterator* MemTable::NewIterator() {
  return new MemTableIterator(&table_);
}

void MemTable::Add(SequenceNumber s, ValueType type,
                   const Slice& key,
                   const Slice& value) {
  // Format of an entry is concatenation of:
  //  key_size     : varint32 of internal_key.size()
  //  key bytes    : char[internal_key.size()]
  //  value_size   : varint32 of value.size()
  //  value bytes  : char[value.size()]
  //
  // 插入到 memtable 的数据项的数据格式为（注意，memtable key 的构成与 LookupKey 一样）:
  // [varint32 类型的 internal_key_size, user_key, 序列号 + 操作类型，varint32 类型的 value_size, value]
  // 其中，internal_key_size = user_key size + 8
  size_t key_size = key.size();
  size_t val_size = value.size();
  size_t internal_key_size = key_size + 8;
  const size_t encoded_len =
      VarintLength(internal_key_size) + internal_key_size +
      VarintLength(val_size) + val_size; // 编码后的数据项总长度
  char* buf = arena_.Allocate(encoded_len); // 分配用来存储数据项的内存
  char* p = EncodeVarint32(buf, internal_key_size); // 将编码为 varint32 格式的 internal_key_size 写入内存
  memcpy(p, key.data(), key_size); // 将 user_key 写入内存
  p += key_size;
  // 注意，序列号虽然是 8 字节但只用了低 7 个字节； 将低 7 个字节左移 8 位，空出最低字节写入操作类型 type。
  EncodeFixed64(p, (s << 8) | type); // 将序列号和操作类型写入内存
  p += 8;
  p = EncodeVarint32(p, val_size); // 将编码为 varint32 格式的 value_size 写入内存
  memcpy(p, value.data(), val_size); // 将 value 写入内存
  assert(p + val_size == buf + encoded_len);
  table_.Insert(buf); // 将数据项插入跳跃表
}

bool MemTable::Get(const LookupKey& key, std::string* value, Status* s) {
  Slice memkey = key.memtable_key();
  Table::Iterator iter(&table_);
  iter.Seek(memkey.data()); // 返回第一个大于等于 memkey 的数据项（数据项在 iter 内部存着）
  if (iter.Valid()) { //iter 指向有效 node，即 node 不为 nullptr
    // entry format is:
    //    klength  varint32
    //    userkey  char[klength] // 这里有误，应该是 klength - 8
    //    tag      uint64
    //    vlength  varint32
    //    value    char[vlength]
    // Check that it belongs to same user key.  We do not check the
    // sequence number since the Seek() call above should have skipped
    // all entries with overly large sequence numbers.
    // 通过比较 user_key 部分和 ValueType 部分来确认是否是我们要找的 key。
    // 不去比较序列号的原因是上面调用 Seek() 的时候已经跳过了非常大的序列号（internal_key 比较逻辑是
    // 序列号越大 internal_key 越小，而我们通过 Seek() 寻找的是第一个大于等于某个 internal_key 的节点）。todo 完善忽略序列号的理由论述。
    //
    // 注意，memtable 将底层的 SkipList 的 key（确切应该说是数据项）声明为了 char* 类型。
    // 这里的 entry 是 SkipList.Node 里包含的整个数据项。
    const char* entry = iter.key();
    uint32_t key_length;
    const char* key_ptr = GetVarint32Ptr(entry, entry+5, &key_length); // 解析 internal_key 长度
    // 比较 user_key。
    // 因为 internal_key 包含了 tag 所以任意两个 internal_key 肯定是不一样的，而我们真正在意的是 user_key，
    // 所以这里调用 user_comparator 比较 user_key。
    if (comparator_.comparator.user_comparator()->Compare(
            Slice(key_ptr, key_length - 8),
            key.user_key()) == 0) {
      // Correct user key
      const uint64_t tag = DecodeFixed64(key_ptr + key_length - 8); // 解析 tag
      // 解析 tag 中的 ValueType。
      // leveldb 删除某个 user_key 的时候不是通过一个写操作实现的吗？那怎么确保在 SkipList.Seek() 时候返回删除操作对应的数据项，
      // 而不是之前同样 user_key 对应的真正的插入操作对应的数据项呢？
      // 机巧就在于 internal_key 的比较原理 user_key 相等的时候，tag 越大 internal_key 越小，
      // 这样后执行的删除操作的 tag（序列号递增了，即使不递增，但由于删除对应的 ValueType 大于插入对应的 ValueType 也可以确保
      // 后执行的删除操作的 tag 大于先执行的插入操作的 tag）
      // 这里有个比较技巧的地方。
      switch (static_cast<ValueType>(tag & 0xff)) {
        case kTypeValue: {
          Slice v = GetLengthPrefixedSlice(key_ptr + key_length);
          value->assign(v.data(), v.size());
          return true;
        }
        case kTypeDeletion:
          *s = Status::NotFound(Slice());
          return true;
      }
    }
  }
  return false;
}

}  // namespace leveldb

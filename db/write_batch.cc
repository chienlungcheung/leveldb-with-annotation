// Copyright (c) 2011 The LevelDB Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file. See the AUTHORS file for names of contributors.
//
// WriteBatch::rep_ :=
//    sequence: fixed64
//    count: fixed32
//    data: record[count]
// record :=
//    kTypeValue varstring varstring         |
//    kTypeDeletion varstring
// varstring :=
//    len: varint32
//    data: uint8[len]

#include "leveldb/write_batch.h"

#include "leveldb/db.h"
#include "db/dbformat.h"
#include "db/memtable.h"
#include "db/write_batch_internal.h"
#include "util/coding.h"

namespace leveldb {

// WriteBatch header has an 8-byte sequence number followed by a 4-byte count.
/**
 * WriteBatch 有一个 header，它由一个大小为 8 字节的序列号后跟一个 4 字节的计数构成，所以总长度为 12 字节。
 */
static const size_t kHeader = 12;

WriteBatch::WriteBatch() {
  Clear();
}

WriteBatch::~WriteBatch() { }

WriteBatch::Handler::~Handler() { }

void WriteBatch::Clear() {
  rep_.clear();
  rep_.resize(kHeader);
}

/**
 * 被该 batch 导致的数据库变化的大小。
 * @return
 */
size_t WriteBatch::ApproximateSize() {
  return rep_.size(); // 直接用 rep_ 大小来估计该批更新数据将会占据的文件系统空间大小
}

/**
 * 迭代 batch，并依次执行 batch 中每一项记录。
 *
 * 注意，迭代过程出错也不对已执行操作进行回滚。
 * @param handler 定义了要执行的处理逻辑
 * @return
 */
Status WriteBatch::Iterate(Handler* handler) const {
  Slice input(rep_);
  if (input.size() < kHeader) {
    return Status::Corruption("malformed WriteBatch (too small)");
  }

  input.remove_prefix(kHeader); // 移除 header，都是指针运算
  Slice key, value;
  int found = 0;
  // 迭代 batch，根据每条 record 的 ValueType（即操作类型）进行相应的处理；
  // record 构成为： ValueType + key + value（如果为删除类型，则 value 为空）
  // 当前仅支持两种类型的操作，即 put 和 delete。
  // 真正执行处理的是 Handler。
  while (!input.empty()) {
    found++;
    char tag = input[0]; // 获取当前 record 的类型，插入键值对还是删除某个键
    input.remove_prefix(1);
    switch (tag) {
      case kTypeValue:
        // 解析 key（user_key） 和 value，并将其写入数据库
        if (GetLengthPrefixedSlice(&input, &key) &&
            GetLengthPrefixedSlice(&input, &value)) {
          handler->Put(key, value);
        } else {
          return Status::Corruption("bad WriteBatch Put");
        }
        break;
      case kTypeDeletion:
        // 解析 key 并将其在数据库标记为删除。
        if (GetLengthPrefixedSlice(&input, &key)) {
          // 注意，删除前并没有确认 key 是否存在于数据库中。
          // 因为 leveldb 的删除实际是一个插入一个数据项，只不过这个数据项的 value 部分为空，
          // 与真正的数据插入区别就是插入的操作类型不同。
          // 所以，理论上 leveldb 允许删除一个并不存在的数据项。
          handler->Delete(key);
        } else {
          return Status::Corruption("bad WriteBatch Delete");
        }
        break;
      default:
        // 如果操作类型不对则停止处理，注意之前的记录并不进行回滚操作
        return Status::Corruption("unknown WriteBatch tag");
    }
  }
  // 将迭代发现的记录数与本身声称的记录个数进行对比，如果不相符说明 batch 处理前已被破坏。
  // 即使 batch 被破坏，也不对已执行操作进行回滚。
  if (found != WriteBatchInternal::Count(this)) {
    return Status::Corruption("WriteBatch has wrong count");
  } else {
    return Status::OK();
  }
}

/**
 * 获取本 batch 包含的记录数。
 * @param b
 * @return
 */
int WriteBatchInternal::Count(const WriteBatch* b) {
  // rep 前 12 字节为 header，包含 8 字节序列号和 4 字节记录数。
  // 跳过前面 8 字节长的 header，然后取接下来 4 个字节转换为 32 位长无符号整数
  return DecodeFixed32(b->rep_.data() + 8);
}

/**
 * 设置本 batch 的记录数
 * @param b batch 起始地址
 * @param n 记录数，int 类型。
 */
void WriteBatchInternal::SetCount(WriteBatch* b, int n) {
  EncodeFixed32(&b->rep_[8], n);
}

/**
 * 解析本 batch 的序列号，64 位长。
 * @param b batch 起始地址
 * @return 序列号
 */
SequenceNumber WriteBatchInternal::Sequence(const WriteBatch* b) {
  return SequenceNumber(DecodeFixed64(b->rep_.data()));
}

/**
 * 设置本 batch 的序列号
 * @param b batch 起始地址
 * @param seq 序列号
 */
void WriteBatchInternal::SetSequence(WriteBatch* b, SequenceNumber seq) {
  EncodeFixed64(&b->rep_[0], seq);
}

/**
 * 将一个 put 操作追加到本 batch 中
 * @param key put 操作的 key
 * @param value put 操作的 value
 */
void WriteBatch::Put(const Slice& key, const Slice& value) {
  WriteBatchInternal::SetCount(this, WriteBatchInternal::Count(this) + 1);
  rep_.push_back(static_cast<char>(kTypeValue));
  PutLengthPrefixedSlice(&rep_, key);
  PutLengthPrefixedSlice(&rep_, value);
}

/**
 * 将一个 delete 操作追加到本 batch 中
 * @param key 要删除的 key
 */
void WriteBatch::Delete(const Slice& key) {
  WriteBatchInternal::SetCount(this, WriteBatchInternal::Count(this) + 1);
  rep_.push_back(static_cast<char>(kTypeDeletion));
  PutLengthPrefixedSlice(&rep_, key);
}

/**
 * 将另一个 batch 中的全部操作依次追加到本 batch 中
 * @param source 另一个 batch
 */
void WriteBatch::Append(const WriteBatch &source) {
  WriteBatchInternal::Append(this, &source);
}

namespace {
/**
 * 一个基于 MemTable 的 Handler。
 * 在迭代某个 WriteBatch 时用来在对应的 memtable 上执行具体的 put 和 delete 操作。
 */
class MemTableInserter : public WriteBatch::Handler {
 public:
  SequenceNumber sequence_; // 插入的每个数据项都有一个唯一的序列号
  MemTable* mem_;

  virtual void Put(const Slice& key, const Slice& value) {
    mem_->Add(sequence_, kTypeValue, key, value);
    sequence_++;
  }
  /**
   * 删除操作本质也是一个写入操作，不过此时 value 是个空的 Slice
   * @param key 要删除的 key
   */
  virtual void Delete(const Slice& key) {
    mem_->Add(sequence_, kTypeDeletion, key, Slice());
    sequence_++;
  }
};
}  // namespace

/**
 * 将 b 中包含的操作应用到 memtable 中
 * @param b
 * @param memtable
 * @return
 */
Status WriteBatchInternal::InsertInto(const WriteBatch* b,
                                      MemTable* memtable) {
  MemTableInserter inserter;
  inserter.sequence_ = WriteBatchInternal::Sequence(b);
  inserter.mem_ = memtable;
  return b->Iterate(&inserter);
}

/**
 * 将 contents 存储的操作内容赋值给 batch b
 * @param b
 * @param contents
 */
void WriteBatchInternal::SetContents(WriteBatch* b, const Slice& contents) {
  assert(contents.size() >= kHeader);
  b->rep_.assign(contents.data(), contents.size());
}

/**
 * 将 src 中除了 header 以外内容追加到 dst 中
 * @param dst
 * @param src
 */
void WriteBatchInternal::Append(WriteBatch* dst, const WriteBatch* src) {
  SetCount(dst, Count(dst) + Count(src));
  assert(src->rep_.size() >= kHeader);
  dst->rep_.append(src->rep_.data() + kHeader, src->rep_.size() - kHeader);
}

}  // namespace leveldb

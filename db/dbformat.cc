// Copyright (c) 2011 The LevelDB Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file. See the AUTHORS file for names of contributors.

#include <stdio.h>
#include "db/dbformat.h"
#include "port/port.h"
#include "util/coding.h"

namespace leveldb {

// 将 7 字节长序列号和 1 字节长 ValueType 打包成一个长度为 8 字节的无符号整数
static uint64_t PackSequenceAndType(uint64_t seq, ValueType t) {
  assert(seq <= kMaxSequenceNumber);
  assert(t <= kValueTypeForSeek);
  return (seq << 8) | t;
}

void AppendInternalKey(std::string* result, const ParsedInternalKey& key) {
  result->append(key.user_key.data(), key.user_key.size());
  PutFixed64(result, PackSequenceAndType(key.sequence, key.type));
}

// 将 ParsedInternalKey 序列化为 'user_key' @ 序列号 : valuetype 格式的字符串并返回
std::string ParsedInternalKey::DebugString() const {
  char buf[50];
  snprintf(buf, sizeof(buf), "' @ %llu : %d",
           (unsigned long long) sequence,
           int(type));
  std::string result = "'";
  result += EscapeString(user_key.ToString());
  result += buf;
  return result;
}

std::string InternalKey::DebugString() const {
  std::string result;
  ParsedInternalKey parsed;
  if (ParseInternalKey(rep_, &parsed)) { // 先将 rep_ 反序列化为 ParsedInternalKey
    result = parsed.DebugString(); // 然后再将 ParsedInternalKey 序列化为字符串
  } else {
    result = "(bad)";
    result.append(EscapeString(rep_));
  }
  return result;
}

const char* InternalKeyComparator::Name() const {
  return "leveldb.InternalKeyComparator";
}

// 分为两部分：
// - internal_key 的 user_key 部分越大则 internal_key 越大，该部分比较采用用户传入的 comparator 进行比较；
// - 如果 user_key 相等，则序列号越小 internal_key 越大；
// - 如果序列号也相等，则 ValueType 越小 internal_key 越大（因为序列号一般都是单调递增唯一的，所以实际上用不到该字段）。
int InternalKeyComparator::Compare(const Slice& akey, const Slice& bkey) const {
  // Order by:
  //    increasing user key (according to user-supplied comparator)
  //    decreasing sequence number
  //    decreasing type (though sequence# should be enough to disambiguate)
  int r = user_comparator_->Compare(ExtractUserKey(akey), ExtractUserKey(bkey));
  if (r == 0) {
    const uint64_t anum = DecodeFixed64(akey.data() + akey.size() - 8);
    const uint64_t bnum = DecodeFixed64(bkey.data() + bkey.size() - 8);
    if (anum > bnum) {
      r = -1;
    } else if (anum < bnum) {
      r = +1;
    }
  }
  return r;
}

void InternalKeyComparator::FindShortestSeparator(
      std::string* start,
      const Slice& limit) const {
  // Attempt to shorten the user portion of the key
  Slice user_start = ExtractUserKey(*start);
  Slice user_limit = ExtractUserKey(limit);
  std::string tmp(user_start.data(), user_start.size());
  user_comparator_->FindShortestSeparator(&tmp, user_limit); // tmp 会被
  if (tmp.size() < user_start.size() &&
      user_comparator_->Compare(user_start, tmp) < 0) { // 如果 tmp 变得更短并且逻辑上大于 user_start
    // User key has become shorter physically, but larger logically.
    // Tack on the earliest possible number to the shortened user key.
    PutFixed64(&tmp, PackSequenceAndType(kMaxSequenceNumber,kValueTypeForSeek));
    assert(this->Compare(*start, tmp) < 0);
    assert(this->Compare(tmp, limit) < 0);
    start->swap(tmp);
  }
}

void InternalKeyComparator::FindShortSuccessor(std::string* key) const {
  Slice user_key = ExtractUserKey(*key);
  std::string tmp(user_key.data(), user_key.size());
  user_comparator_->FindShortSuccessor(&tmp);
  if (tmp.size() < user_key.size() &&
      user_comparator_->Compare(user_key, tmp) < 0) {
    // User key has become shorter physically, but larger logically.
    // Tack on the earliest possible number to the shortened user key.
    PutFixed64(&tmp, PackSequenceAndType(kMaxSequenceNumber,kValueTypeForSeek));
    assert(this->Compare(*key, tmp) < 0);
    key->swap(tmp);
  }
}

const char* InternalFilterPolicy::Name() const {
  return user_policy_->Name();
}

void InternalFilterPolicy::CreateFilter(const Slice* keys, int n,
                                        std::string* dst) const {
  // We rely on the fact that the code in table.cc does not mind us
  // adjusting keys[].
  // 注意，该方法依赖一个事实，即 table.cc 中的代码不介意我们在这里将 keys[] 进行修改
  // （下面这个循环将 keys[] 中的 internal_key 全都替换为了 user_key）。
  Slice* mkey = const_cast<Slice*>(keys);
  for (int i = 0; i < n; i++) {
    mkey[i] = ExtractUserKey(keys[i]);
    // TODO(sanjay): Suppress dups?
  }
  user_policy_->CreateFilter(keys, n, dst);
}

bool InternalFilterPolicy::KeyMayMatch(const Slice& key, const Slice& f) const {
  // 因为创建过滤策略的时候用的是 user_key，所以这里查找前需要将 key 的 user_key 抽出来
  return user_policy_->KeyMayMatch(ExtractUserKey(key), f);
}

LookupKey::LookupKey(const Slice& user_key, SequenceNumber s) {
  size_t usize = user_key.size();
  // 底层存储格式为 varint32 类型的 internal_key 长度（1 到 5 个字节）+ user_key（usize 字节） + tag（8 字节），
  // 所以这里预留了 usize + 13 个字节，足够了。
  size_t needed = usize + 13;  // A conservative estimate
  char* dst;
  if (needed <= sizeof(space_)) {
    dst = space_;
  } else {
    dst = new char[needed];
  }
  start_ = dst;
  dst = EncodeVarint32(dst, usize + 8); // 注意这里是 internal_key 长度，而非 user_key 长度
  kstart_ = dst;
  memcpy(dst, user_key.data(), usize);
  dst += usize;
  EncodeFixed64(dst, PackSequenceAndType(s, kValueTypeForSeek)); // 注意这个 kValueTypeForSeek，具体见定义处注释。
  dst += 8;
  end_ = dst;
}

}  // namespace leveldb

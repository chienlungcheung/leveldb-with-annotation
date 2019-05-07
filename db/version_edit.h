// Copyright (c) 2011 The LevelDB Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file. See the AUTHORS file for names of contributors.

#ifndef STORAGE_LEVELDB_DB_VERSION_EDIT_H_
#define STORAGE_LEVELDB_DB_VERSION_EDIT_H_

#include <set>
#include <utility>
#include <vector>
#include "db/dbformat.h"

namespace leveldb {

class VersionSet;

// table 文件的元信息描述符
struct FileMetaData {
  // 文件引用计数
  int refs;
  // 文件压实之前允许的查询次数
  int allowed_seeks;          // Seeks allowed until compaction
  // 文件号码
  uint64_t number;
  // 文件大小
  uint64_t file_size;         // File size in bytes
  // 对应 table 文件最小的 internal_key
  InternalKey smallest;       // Smallest internal key served by table
  // 对应 table 文件最大的 internal_key
  InternalKey largest;        // Largest internal key served by table

  FileMetaData() : refs(0), allowed_seeks(1 << 30), file_size(0) { }
};

class VersionEdit {
 public:
  VersionEdit() { Clear(); }
  ~VersionEdit() { }

  // 重置该 VersionSet 状态
  void Clear();

  // 设置比较器名称
  void SetComparatorName(const Slice& name) {
    has_comparator_ = true;
    comparator_ = name.ToString();
  }
  // 设置日志编号
  void SetLogNumber(uint64_t num) {
    has_log_number_ = true;
    log_number_ = num;
  }
  // 设置前一个日志编号
  void SetPrevLogNumber(uint64_t num) {
    has_prev_log_number_ = true;
    prev_log_number_ = num;
  }
  // 设置下个文件编号
  void SetNextFile(uint64_t num) {
    has_next_file_number_ = true;
    next_file_number_ = num;
  }
  // 设置上一个序列号
  void SetLastSequence(SequenceNumber seq) {
    has_last_sequence_ = true;
    last_sequence_ = seq;
  }
  // 设置压实指针
  void SetCompactPointer(int level, const InternalKey& key) {
    compact_pointers_.push_back(std::make_pair(level, key));
  }

  // Add the specified file at the specified number.
  // REQUIRES: This version has not been saved (see VersionSet::SaveTo)
  // REQUIRES: "smallest" and "largest" are smallest and largest keys in file
  //
  // 以指定的文件号码将文件保存到指定的 level。
  // 前提：该 version 没有被保存过（见 VersionSet::SaveTo）
  // 前提："smallest" 和 "largest" 分别是文件中最小的 key 和最大的 key
  void AddFile(int level, uint64_t file,
               uint64_t file_size,
               const InternalKey& smallest,
               const InternalKey& largest) {
    FileMetaData f;
    f.number = file;
    f.file_size = file_size;
    f.smallest = smallest;
    f.largest = largest;
    new_files_.push_back(std::make_pair(level, f));
  }

  // Delete the specified "file" from the specified "level".
  //
  // 从指定的 level 删除指定的 file
  void DeleteFile(int level, uint64_t file) {
    deleted_files_.insert(std::make_pair(level, file));
  }

  // 将该 VersionEdit 对象序列化到 dst 指定内存中
  void EncodeTo(std::string* dst) const;
  // 将 src 保存的数据反序列化到当前 VersionEdit 对象中
  Status DecodeFrom(const Slice& src);

  // 将 VersionEdit 对象以人类友好的方式打印出来
  std::string DebugString() const;

 private:
  friend class VersionSet;

  typedef std::set< std::pair<int, uint64_t> > DeletedFileSet;

  // 比较器名称
  std::string comparator_; // comparator name
  uint64_t log_number_;
  uint64_t prev_log_number_;
  uint64_t next_file_number_;
  SequenceNumber last_sequence_;
  bool has_comparator_;
  bool has_log_number_;
  bool has_prev_log_number_;
  bool has_next_file_number_;
  bool has_last_sequence_;

  std::vector< std::pair<int, InternalKey> > compact_pointers_;
  // 待删除文件列表
  DeletedFileSet deleted_files_;
  // 新增文件列表（注意第二个参数不是指针类型）
  std::vector< std::pair<int, FileMetaData> > new_files_;
};

}  // namespace leveldb

#endif  // STORAGE_LEVELDB_DB_VERSION_EDIT_H_

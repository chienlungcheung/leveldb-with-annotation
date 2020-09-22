// Copyright (c) 2011 The LevelDB Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file. See the AUTHORS file for names of contributors.

#ifndef STORAGE_LEVELDB_DB_BUILDER_H_
#define STORAGE_LEVELDB_DB_BUILDER_H_

#include "leveldb/status.h"

namespace leveldb {

struct Options;
struct FileMetaData;

class Env;
class Iterator;
class TableCache;
class VersionEdit;

// Build a Table file from the contents of *iter.  The generated file
// will be named according to meta->number.  On success, the rest of
// *meta will be filled with metadata about the generated table.
// If no data is present in *iter, meta->file_size will be set to
// zero, and no Table file will be produced.
//
// 该方法用于将 memtable 序列化为 sorted table 文件并写入磁盘, 
// 同时确保该文件对应的 table 对象会被放到 table_cache_.
// 从迭代器 *iter 指向的 memtable 构造一个 Table 文件, 新生成的 Table 文件将基于 meta->number 进行命名. 
// 如果构造成功, *meta 的其它字段将会被合理填充. 
// 如果 *iter 对应的 Memtable 为空, 则 meta->file_size 将会被置为 0, 而且不会生成任何 Table 文件. 
Status BuildTable(const std::string& dbname,
                  Env* env,
                  const Options& options,
                  TableCache* table_cache,
                  Iterator* iter,
                  FileMetaData* meta);

}  // namespace leveldb

#endif  // STORAGE_LEVELDB_DB_BUILDER_H_

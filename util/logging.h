// Copyright (c) 2011 The LevelDB Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file. See the AUTHORS file for names of contributors.
//
// Must not be included from any .h files to avoid polluting the namespace
// with macros.

#ifndef STORAGE_LEVELDB_UTIL_LOGGING_H_
#define STORAGE_LEVELDB_UTIL_LOGGING_H_

#include <stdio.h>
#include <stdint.h>
#include <string>
#include "port/port.h"

namespace leveldb {

class Slice;
class WritableFile;

// Append a human-readable printout of "num" to *str
// 把数字 num 转换为一个字符串追加到 str
void AppendNumberTo(std::string* str, uint64_t num);

// Append a human-readable printout of "value" to *str.
// Escapes any non-printable characters found in "value".
// 将 value 转换为字符串追加到 str，同时将 value 中的不可打印字符进行转义
void AppendEscapedStringTo(std::string* str, const Slice& value);

// Return a human-readable printout of "num"
// 将数字 num 转换为 string
std::string NumberToString(uint64_t num);

// Return a human-readable version of "value".
// Escapes any non-printable characters found in "value".
// 将 value 转换为 string，同时将 value 中的不可打印字符进行转义
std::string EscapeString(const Slice& value);

// Parse a human-readable number from "*in" into *value.  On success,
// advances "*in" past the consumed number and sets "*val" to the
// numeric value.  Otherwise, returns false and leaves *in in an
// unspecified state.
// 将 Slice 类型的 in 转换为数字存储到 val。
// 如果转换成功则将 in 指针向前移动跳过已转换好的字节；否则返回 false 并且让 in 处于未指定状态。
bool ConsumeDecimalNumber(Slice* in, uint64_t* val);

}  // namespace leveldb

#endif  // STORAGE_LEVELDB_UTIL_LOGGING_H_

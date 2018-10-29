// Copyright (c) 2011 The LevelDB Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file. See the AUTHORS file for names of contributors.
//
// Endian-neutral encoding:
// * Fixed-length numbers are encoded with least-significant byte first
// * In addition we support variable length "varint" encoding
// * Strings are encoded prefixed by their length in varint format

#ifndef STORAGE_LEVELDB_UTIL_CODING_H_
#define STORAGE_LEVELDB_UTIL_CODING_H_

#include <stdint.h>
#include <string.h>

#include <string>

#include "leveldb/slice.h"
#include "port/port.h"

namespace leveldb {

// Standard Put... routines append to a string
void PutFixed32(std::string* dst, uint32_t value);
void PutFixed64(std::string* dst, uint64_t value);
void PutVarint32(std::string* dst, uint32_t value);
void PutVarint64(std::string* dst, uint64_t value);
void PutLengthPrefixedSlice(std::string* dst, const Slice& value);

// Standard Get... routines parse a value from the beginning of a Slice
// and advance the slice past the parsed value.
/**
 * 将输入地址包含的 varint32 类型数据转换为长度为 32 位的无符号数据，同时还有一个副作用，
 * 即将输入包含的底层存储的指针向前移动 varint32 编码长度（1 到 5）个字节。
 * @param input 输入，Slice 类型
 * @param value 存储解析出来的 32 位长的无符号数据
 * @return
 */
bool GetVarint32(Slice* input, uint32_t* value);

/**
 * 将 input 指向空间的编码为 varint64 的数据转换为长度为 64 位的无符号数据，同时还有一个副作用，
 * 即将输入包含的底层存储的指针向前移动 varint64 编码长度（1 到 10）个字节
 * @param input varint64 编码数据起始地址，Slice 类型，函数执行完毕该指针将会指向一个新的 Slice 实例，
 * 其底层存储的起始地址将在之前基础上向前移动 varint64 编码长度（1 到 10）个字节
 * @param value 存储解析出来的 64 位长的无符号数据
 * @return 成功为 true，否则为 false
 */
bool GetVarint64(Slice* input, uint64_t* value);
/**
 * 该函数可以从指定空间解析一个格式为【长度（varint32 类型），具体内容（内容大小即为前述的长度）】的数据记录，
 * 成功解析的数据记录存放到 result.
 * @param input 要解析的数据记录所在空间
 * @param result 指向解析出的数据记录
 * @return 成功为 true，否则为 false
 */
bool GetLengthPrefixedSlice(Slice* input, Slice* result);

// Pointer-based variants of GetVarint...  These either store a value
// in *v and return a pointer just past the parsed value, or return
// nullptr on error.  These routines only look at bytes in the range
// [p..limit-1]
const char* GetVarint32Ptr(const char* p, const char* limit, uint32_t* v);
const char* GetVarint64Ptr(const char* p, const char* limit, uint64_t* v);

// Returns the length of the varint32 or varint64 encoding of "v"
int VarintLength(uint64_t v);

// Lower-level versions of Put... that write directly into a character buffer
// REQUIRES: dst has enough space for the value being written
void EncodeFixed32(char* dst, uint32_t value);
void EncodeFixed64(char* dst, uint64_t value);

// Lower-level versions of Put... that write directly into a character buffer
// and return a pointer just past the last byte written.
// REQUIRES: dst has enough space for the value being written
char* EncodeVarint32(char* dst, uint32_t value);
char* EncodeVarint64(char* dst, uint64_t value);

// Lower-level versions of Get... that read directly from a character buffer
// without any bounds checking.

/**
 * 根据字节序将一个字节数组转换为 32 位长无符号整数
 * @param ptr
 * @return
 */
inline uint32_t DecodeFixed32(const char* ptr) {
  if (port::kLittleEndian) {
    // Load the raw bytes
    uint32_t result;
    memcpy(&result, ptr, sizeof(result));  // gcc optimizes this to a plain load
    return result;
  } else {
    return ((static_cast<uint32_t>(static_cast<unsigned char>(ptr[0])))
        | (static_cast<uint32_t>(static_cast<unsigned char>(ptr[1])) << 8)
        | (static_cast<uint32_t>(static_cast<unsigned char>(ptr[2])) << 16)
        | (static_cast<uint32_t>(static_cast<unsigned char>(ptr[3])) << 24));
  }
}

inline uint64_t DecodeFixed64(const char* ptr) {
  if (port::kLittleEndian) {
    // Load the raw bytes
    uint64_t result;
    memcpy(&result, ptr, sizeof(result));  // gcc optimizes this to a plain load
    return result;
  } else {
    uint64_t lo = DecodeFixed32(ptr);
    uint64_t hi = DecodeFixed32(ptr + 4);
    return (hi << 32) | lo;
  }
}

// Internal routine for use by fallback path of GetVarint32Ptr
/**
 * 根据每个字节最高位是否为 1 判断是否需要继续处理，1 继续，0 不继续；
   每个字节只有低 7 位存储的才是待解析有效数据，而且每个字节之间采用类小端序模式。
 * @param p 待解析数据起始地址
 * @param limit 待解析数据末尾字节的下一个字节
 * @param value 指向存储解析出来的数据副本的空间
 * @return 其它待处理数据起始地址
 */
const char* GetVarint32PtrFallback(const char* p,
                                   const char* limit,
                                   uint32_t* value);
/**
 * 将 p 指向空间的值转换为无符号 32 位整数并复制到 value 指向的空间。
 * @param p 要转换的值所在的地址
 * @param limit varint32 格式数据末尾字节的下一个字节
 * @param value 存储转换后的值
 * @return 存储 varint32 编码内容空间的下一个字节
 */
inline const char* GetVarint32Ptr(const char* p,
                                  const char* limit,
                                  uint32_t* value) {
  if (p < limit) {
    // reinterpret_cast “通常为操作数的位模式提供较低层的重新解释”也就是说将数据以二进制存在形式的重新解释;
    // static_cast 是将一些隐式类型转换显式地进行，更加明确。
    // 将指针 p 重新解释为 const unsigned char 指针类型，并取出 p 指向的数据，然后隐式地转换为 32 位的无符号整数类型，
    // result 的值就是一个长度，具体值为 0x0000000y
    uint32_t result = *(reinterpret_cast<const unsigned char*>(p));
    // 如果 y 所在字节的最高位为 0，则将 result 存储到 value 指向的空间，同时返回 p + 一个 char 类型长度的地址;
    // y 最高位为 0 说明后面数据长度不超过 128，否则需要调用 GetVarint32PtrFallback 解析长度。
    if ((result & 128) == 0) {
      *value = result;
      return p + 1;
    }
  }
  // 当 p >= limit 时虽然也执行下面方法，但是并不会做什么事情，因为下面方法也要求 p < limit；
  // 所以执行下面方法时的有效情况为 result 最低字节的最高位为 1，即 varint32 用一个字节存不下的情况出现了.
  return GetVarint32PtrFallback(p, limit, value);
}

}  // namespace leveldb

#endif  // STORAGE_LEVELDB_UTIL_CODING_H_

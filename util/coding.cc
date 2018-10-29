// Copyright (c) 2011 The LevelDB Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file. See the AUTHORS file for names of contributors.

#include "util/coding.h"

namespace leveldb {

// 按照系统编码格式将一个无符号 32 位整数写到 buf 指向内存处（buf 指针不变）
void EncodeFixed32(char* buf, uint32_t value) {
  if (port::kLittleEndian) {
    memcpy(buf, &value, sizeof(value));
  } else {
    buf[0] = value & 0xff;
    buf[1] = (value >> 8) & 0xff;
    buf[2] = (value >> 16) & 0xff;
    buf[3] = (value >> 24) & 0xff;
  }
}

// 将 64 位长的 value 按照字节序写入到 buf 所在位置
// 注意，该编码格式是“类”小端模式，即 buf 低位字节要存储 value 的低位字节
void EncodeFixed64(char* buf, uint64_t value) {
  // 如果机器是小端，机器低位内存存储的是 value 低位字节，直接拷贝即可实现 value 的低字节到高字节，按照从 buf 低字节到高字节分布
  if (port::kLittleEndian) {
    memcpy(buf, &value, sizeof(value));
  } else { // 如果机器是大端，机器低位内存存储的是 value 高位字节，需要字节逆转，才能实现 value 的低字节到高字节，按照从 buf 低字节到高字节分布
    buf[0] = value & 0xff;
    buf[1] = (value >> 8) & 0xff;
    buf[2] = (value >> 16) & 0xff;
    buf[3] = (value >> 24) & 0xff;
    buf[4] = (value >> 32) & 0xff;
    buf[5] = (value >> 40) & 0xff;
    buf[6] = (value >> 48) & 0xff;
    buf[7] = (value >> 56) & 0xff;
  }
}

/**
 * 将一个无符号 32 位整数追加到 dst 指向内存处
 */
void PutFixed32(std::string* dst, uint32_t value) {
  char buf[sizeof(value)];
  EncodeFixed32(buf, value);
  dst->append(buf, sizeof(buf));
}

/**
 * 将一个无符号 64 位整数追加到 dst 指向内存处
 */
void PutFixed64(std::string* dst, uint64_t value) {
  char buf[sizeof(value)];
  EncodeFixed64(buf, value);
  dst->append(buf, sizeof(buf));
}

/**
 * 将 32 位无符号整数编码写入目的地址指向空间。
 *
 * 编码方式为每 7 位一组写入一个字节，每个字节最高位是标识位，表示是否还有后续字节，1 表示还有后续字节，0 表示无后续字节。
 * @param dst 目的地址
 * @param v 待编码 32 位无符号数
 * @return 编码数据所占空间的下一个字节
 */
char* EncodeVarint32(char* dst, uint32_t v) {
  // Operate on characters as unsigneds
  unsigned char* ptr = reinterpret_cast<unsigned char*>(dst);
  static const int B = 128; // 即 0x80
  if (v < (1<<7)) { // 如果待编码数值小于 128，则直接写入
    *(ptr++) = v;
  } else if (v < (1<<14)) { // 如果待编码数值大于等于 128 但是小于 2^14，则将低 7 位写入第一个字节，并继续处理
    *(ptr++) = v | B; // 注意这里是或操作，这可以写到 ptr 中的最高位为 1（该位为标志位，1 表示还有后续字节，0 表示无后续字节），同时确保 v 低 7 位原封不动写入 ptr 低 7 位
    *(ptr++) = v>>7; // v 第二个字节低 7 位写入 ptr，此时 ptr 最高位为 0，表示没有后续字节了。
  } else if (v < (1<<21)) {
    *(ptr++) = v | B;
    *(ptr++) = (v>>7) | B;
    *(ptr++) = v>>14;
  } else if (v < (1<<28)) {
    *(ptr++) = v | B;
    *(ptr++) = (v>>7) | B;
    *(ptr++) = (v>>14) | B;
    *(ptr++) = v>>21;
  } else {
    *(ptr++) = v | B;
    *(ptr++) = (v>>7) | B;
    *(ptr++) = (v>>14) | B;
    *(ptr++) = (v>>21) | B;
    *(ptr++) = v>>28;
  }
  return reinterpret_cast<char*>(ptr);
}

/**
 * 将一个 32 位无符号数 v 编码为 varint32 后拷贝到 dst 指定的地址
 * @param dst
 * @param v
 */
void PutVarint32(std::string* dst, uint32_t v) {
  char buf[5]; // 因为每个字节最高位为标志位，低 7 位采用来存储编码值，所以一个 32 位长无符号数**最多**需要 5 个字节才能完全存储。
  char* ptr = EncodeVarint32(buf, v);
  dst->append(buf, ptr - buf);
}

/**
 * 将一个 64 位无符号数编码存储到 dst 指定空间
 * @param dst
 * @param v
 * @return 编码数据所占空间的下一个字节
 */
char* EncodeVarint64(char* dst, uint64_t v) {
  static const int B = 128;
  unsigned char* ptr = reinterpret_cast<unsigned char*>(dst);
  // 下面到 return 之间逻辑类似 EncodeVarint32，更加简化的写法。
  while (v >= B) {
    *(ptr++) = (v & (B-1)) | B;
    v >>= 7;
  }
  *(ptr++) = static_cast<unsigned char>(v);
  return reinterpret_cast<char*>(ptr);
}

/**
 * 将一个 64 位无符号数 v 编码为 varint64 后拷贝到 dst 指定的地址
 * @param dst
 * @param v
 */
void PutVarint64(std::string* dst, uint64_t v) {
  char buf[10]; // 64 位，7 位一组最多共需要 10 个字节来存储
  char* ptr = EncodeVarint64(buf, v);
  dst->append(buf, ptr - buf);
}

/**
 * 为 Slice 类型的 value 计算一个 varint32 类型的长度前缀，并将该前缀和 value 实际值一起拷贝到 dst 指定的位置。
 * 将 value 的长度编码为 varint32 后拷贝到 dst 指定位置，然后将 value 的内容追加到长度之后。
 * @param dst
 * @param value
 */
void PutLengthPrefixedSlice(std::string* dst, const Slice& value) {
  PutVarint32(dst, value.size());
  dst->append(value.data(), value.size());
}

/**
 * 计算每个 64 位无符号整数 v 编码为 varint 后需要几个字节存储
 * @param v
 * @return
 */
int VarintLength(uint64_t v) {
  int len = 1;
  while (v >= 128) {
    v >>= 7;
    len++;
  }
  return len;
}

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
                                   uint32_t* value) {
  uint32_t result = 0;
  // 最多循环 5 次，因为一个无符号 32 位整数需要最多 5 个字节才能存下，
  // 注意每个字节最高位是标志位不是有效数据位所以 32 位数据最多需要 5 个字节。
  for (uint32_t shift = 0; shift <= 28 && p < limit; shift += 7) {
    uint32_t byte = *(reinterpret_cast<const unsigned char*>(p));
    p++;
    // 根据每个字节最高位是否为 1 判断是否需要继续处理，1 继续，0 不继续；
    // 每个字节只有低 7 位存储的才是待解析有效数据，而且每个字节之间采用类小端序模式。
    if (byte & 128) {
      // More bytes are present
      result |= ((byte & 127) << shift);
    } else {
      result |= (byte << shift);
      *value = result;
      return reinterpret_cast<const char*>(p);
    }
  }
  return nullptr;
}

/**
 * 将输入地址包含的 varint32 类型数据转换为长度为 32 位的无符号数据，同时还有一个副作用，
 * 即将输入包含的底层存储的指针向前移动 varint32 编码长度（1 到 5）个字节。
 * @param input 输入起始地址，Slice 类型，函数执行完毕该指针将会指向一个新的 Slice 实例，其底层存储的起始地址将在之前基础上向前移动 varint32 编码长度（1 到 5）个字节
 * @param value 存储解析出来的 32 位长的无符号数据
 * @return 成功为 true，否则为 false
 */
bool GetVarint32(Slice* input, uint32_t* value) {
  const char* p = input->data();
  const char* limit = p + input->size();
  const char* q = GetVarint32Ptr(p, limit, value);
  if (q == nullptr) {
    return false;
  } else {
    // 如果 q 有效，则将 input 指向新生成的 Slice 实例，其内容为起始地址为 q 长度为 limit - q 的底层存储空间。
    *input = Slice(q, limit - q);
    return true;
  }
}

/**
 * 将 p 指向空间的值转换为无符号 64 位整数并复制到 value 指向的空间。
 * @param p 要转换的值所在的地址
 * @param limit 确保 p 指向空间有效
 * @param value 存储转换后的值
 * @return 存储 varint64 编码内容空间的下一个字节
 */
const char* GetVarint64Ptr(const char* p, const char* limit, uint64_t* value) {
  uint64_t result = 0;
  for (uint32_t shift = 0; shift <= 63 && p < limit; shift += 7) {
    uint64_t byte = *(reinterpret_cast<const unsigned char*>(p));
    p++;
    if (byte & 128) {
      // More bytes are present
      result |= ((byte & 127) << shift);
    } else {
      result |= (byte << shift);
      *value = result;
      return reinterpret_cast<const char*>(p);
    }
  }
  return nullptr;
}

/**
 * 将 input 指向空间的编码为 varint64 的数据转换为长度为 64 位的无符号数据，同时还有一个副作用，
 * 即将输入包含的底层存储的指针向前移动 varint64 编码长度（1 到 10）个字节
 * @param input varint64 编码数据起始地址，Slice 类型，函数执行完毕该指针将会指向一个新的 Slice 实例，
 * 其底层存储的起始地址将在之前基础上向前移动 varint64 编码长度（1 到 10）个字节
 * @param value 存储解析出来的 64 位长的无符号数据
 * @return 成功为 true，否则为 false
 */
bool GetVarint64(Slice* input, uint64_t* value) {
  const char* p = input->data();
  const char* limit = p + input->size();
  const char* q = GetVarint64Ptr(p, limit, value);
  if (q == nullptr) {
    return false;
  } else {
    *input = Slice(q, limit - q);
    return true;
  }
}

const char* GetLengthPrefixedSlice(const char* p, const char* limit,
                                   Slice* result) {
  uint32_t len;
  p = GetVarint32Ptr(p, limit, &len);
  if (p == nullptr) return nullptr;
  if (p + len > limit) return nullptr;
  *result = Slice(p, len);
  return p + len;
}

/**
 * 该函数可以从指定空间解析一条格式为 【长度（varint32 类型），具体数据（内容大小即为前述的长度）】 的数据记录，
 * 成功解析的具体数据存放到 result.
 * @param input 要解析的数据记录所在空间
 * @param result 指向解析出的具体数据
 * @return 成功为 true，否则为 false
 */
bool GetLengthPrefixedSlice(Slice* input, Slice* result) {
  uint32_t len;
  // 获取数据长度并存储到 len，同时保证剩余数据长度不小于 len（因为可能由多条同样格式的记录，所以剩余数据长度可能大于 len）。
  // 注意，GetVarint32 有个副作用即将 input 指向新的 Slice 实例，其底层存储的指针会在之前所指实例基础上向前移动一个单位长度。
  if (GetVarint32(input, &len) &&
      input->size() >= len) {
    *result = Slice(input->data(), len);
    input->remove_prefix(len); // 将 input 底层存储指针指向下一条记录起始位置
    return true;
  } else {
    return false;
  }
}

}  // namespace leveldb

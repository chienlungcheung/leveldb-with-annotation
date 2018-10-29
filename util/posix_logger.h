// Copyright (c) 2011 The LevelDB Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file. See the AUTHORS file for names of contributors.
//
// Logger implementation that can be shared by all environments
// where enough posix functionality is available.

#ifndef STORAGE_LEVELDB_UTIL_POSIX_LOGGER_H_
#define STORAGE_LEVELDB_UTIL_POSIX_LOGGER_H_

#include <sys/time.h>

#include <cassert>
#include <cstdarg>
#include <cstdio>
#include <ctime>
#include <sstream>
#include <thread>

#include "leveldb/env.h"

namespace leveldb {

class PosixLogger final : public Logger {
 public:
  // Creates a logger that writes to the given file.
  //
  // The PosixLogger instance takes ownership of the file handle.
  explicit PosixLogger(std::FILE* fp) : fp_(fp) {
    assert(fp != nullptr);
  }

  ~PosixLogger() override {
    std::fclose(fp_);
  }

  void Logv(const char* format, va_list arguments) override {
    // Record the time as close to the Logv() call as possible.
    /**
     * 获取本次日志发生的墙上时间
     */
    struct ::timeval now_timeval;
    ::gettimeofday(&now_timeval, nullptr);
    const std::time_t now_seconds = now_timeval.tv_sec;
    struct std::tm now_components;
    ::localtime_r(&now_seconds, &now_components);

    // Record the thread ID.
    /**
     * 获取本次日志发生的线程 ID，32 位长，不足后面补零，超过则截断。
     */
    constexpr const int kMaxThreadIdSize = 32;
    std::ostringstream thread_stream;
    thread_stream << std::this_thread::get_id();
    std::string thread_id = thread_stream.str();
    if (thread_id.size() > kMaxThreadIdSize) {
      thread_id.resize(kMaxThreadIdSize);
    }

    // We first attempt to print into a stack-allocated buffer. If this attempt
    // fails, we make a second attempt with a dynamically allocated buffer.
    /**
     * 尝试将要输出的日志放到栈内存，如果分配失败则再次尝试将日志放到堆内存。
     */
    constexpr const int kStackBufferSize = 512;
    char stack_buffer[kStackBufferSize];
    /**
     * 检查 char 类型是不是占 1 个字节。
     *
     * 若第一个参数结果为 true，则忽略；否则，编译时报错，诊断信息会包含第二个参数内容。
     */
    static_assert(sizeof(stack_buffer) == static_cast<size_t>(kStackBufferSize),
                  "sizeof(char) is expected to be 1 in C++");

    int dynamic_buffer_size = 0;  // Computed in the first iteration.
    for (int iteration = 0; iteration < 2; ++iteration) { // 开始进行两次尝试
      const int buffer_size =
          (iteration == 0) ? kStackBufferSize : dynamic_buffer_size; // 第一次尝试栈内存（此时 iteration 为 0）
      char* const buffer =
          (iteration == 0) ? stack_buffer : new char[dynamic_buffer_size]; // 第二次尝试堆内存)（此时 iteration 为 1）

      // Print the header into the buffer.
      // 打印日志头，包括“年月日-时分秒.微妙 线程ID”
      // 返回值不包含 snprintf 自动追加的结尾空字符，应为 27 + kMaxThreadIdSize
      int buffer_offset = snprintf(
          buffer, buffer_size,
          "%04d/%02d/%02d-%02d:%02d:%02d.%06d %s",
          now_components.tm_year + 1900,
          now_components.tm_mon + 1,
          now_components.tm_mday,
          now_components.tm_hour,
          now_components.tm_min,
          now_components.tm_sec,
          static_cast<int>(now_timeval.tv_usec),
          thread_id.c_str());

      // The header can be at most 28 characters (10 date + 15 time +
      // 3 spacing) plus the thread ID, which should fit comfortably into the
      // static buffer.
      assert(buffer_offset <= 28 + kMaxThreadIdSize);
      static_assert(28 + kMaxThreadIdSize < kStackBufferSize,
                    "stack-allocated buffer may not fit the message header");
      assert(buffer_offset < buffer_size);

      // Print the message into the buffer.
      std::va_list arguments_copy;
      va_copy(arguments_copy, arguments);
      // 返回值不包含自动追加的空字符
      buffer_offset += std::vsnprintf(buffer + buffer_offset,
                                      buffer_size - buffer_offset, format,
                                      arguments_copy);
      va_end(arguments_copy);

      // The code below may append a newline at the end of the buffer, which
      // requires an extra character.
      // 因为 buffer_size 太小导致 vsnprintf 发生截断的时候返回值是 format + arguments_copy 的长度，
      // 此时 buffer_offset 就是本次 log 的总字符个数（不包含最后的结尾空字符），下面条件为真
      if (buffer_offset >= buffer_size - 1) {
        // The message did not fit into the buffer.
        if (iteration == 0) {
          // Re-run the loop and use a dynamically-allocated buffer. The buffer
          // will be large enough for the log message, an extra newline and a
          // null terminator.
          // 消息太长放不下，进行第二次尝试，此时期待的缓存空间大小为 buffer_offset + 2，
          // 为啥要加 2 呢，因为除了 log 包含的信息，最后要有一个换行符和一个结束字符串的空字符
          dynamic_buffer_size = buffer_offset + 2;
          continue;
        }

        // The dynamically-allocated buffer was incorrectly sized. This should
        // not happen, assuming a correct implementation of (v)snprintf. Fail
        // in tests, recover by truncating the log message in production.
        // 如果堆内存分配失败，在测试环境下直接终止程序，在生产环境里则将对 log 内容进行截断。
        assert(false);
        buffer_offset = buffer_size - 1;
      }

      // Add a newline if necessary.
      if (buffer[buffer_offset - 1] != '\n') {
        buffer[buffer_offset] = '\n';
        ++buffer_offset;
      }

      assert(buffer_offset <= buffer_size);
      std::fwrite(buffer, 1, buffer_offset, fp_);
      std::fflush(fp_);

      if (iteration != 0) {
        delete[] buffer;
      }
      break;
    }
  }

 private:
  std::FILE* const fp_;
};

}  // namespace leveldb

#endif  // STORAGE_LEVELDB_UTIL_POSIX_LOGGER_H_

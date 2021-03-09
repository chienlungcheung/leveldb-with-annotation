// Copyright (c) 2011 The LevelDB Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file. See the AUTHORS file for names of contributors.
//
// An Env is an interface used by the leveldb implementation to access
// operating system functionality like the filesystem etc.  Callers
// may wish to provide a custom Env object when opening a database to
// get fine gain control; e.g., to rate limit file system operations.
//
// All Env implementations are safe for concurrent access from
// multiple threads without any external synchronization.
//
// Env 被 leveldb 用来访问操作系统相关的功能, 如文件系统等等. 
// 调用者如果想做细粒度的控制, 可以在打开一个数据时提供一个定制的 Env 对象. 
//
// 全部 Env 实现必须是线程安全的. 

#ifndef STORAGE_LEVELDB_INCLUDE_ENV_H_
#define STORAGE_LEVELDB_INCLUDE_ENV_H_

#include <stdarg.h>
#include <stdint.h>
#include <string>
#include <vector>
#include "leveldb/export.h"
#include "leveldb/status.h"

namespace leveldb {

class FileLock;
class Logger;
class RandomAccessFile;
class SequentialFile;
class Slice;
class WritableFile;

// Env 被 leveldb 用来访问操作系统相关的功能, 如文件系统等等. 
// 调用者如果想做细粒度的控制, 可以在打开一个数据库时提供一个定制的 Env 对象. 
//
// 全部 Env 实现必须是线程安全的. 
class LEVELDB_EXPORT Env {
 public:
  Env() = default;

  Env(const Env&) = delete;
  Env& operator=(const Env&) = delete;

  virtual ~Env();

  // Return a default environment suitable for the current operating
  // system.  Sophisticated users may wish to provide their own Env
  // implementation instead of relying on this default environment.
  //
  // The result of Default() belongs to leveldb and must never be deleted.
  //
  // 返回当前操作系统默认的环境对象. 
  // 老练的开发者也可以用自己实现的环境来替代该默认环境. 
  //
  // 该方法返回的结果属于 leveldb 并且不能被删除. 
  static Env* Default();

  // Create an object that sequentially reads the file with the specified name.
  // On success, stores a pointer to the new file in *result and returns OK.
  // On failure stores nullptr in *result and returns non-OK.  If the file does
  // not exist, returns a non-OK status.  Implementations should return a
  // NotFound status when the file does not exist.
  //
  // The returned file will only be accessed by one thread at a time.
  //
  // 创建一个顺序文件对象, 它用来读取指定名字的文件. 
  // 如果成功, 将指向一个新文件的指针存储到 *result 并返回 OK. 
  // 如果失败, 将 *result 置为 nullptr 并返回 non-OK. 
  // 如果文件不存在, 返回 NotFound. 
  //
  // 返回的文件一次只能只被一个线程访问. 
  virtual Status NewSequentialFile(const std::string& fname,
                                   SequentialFile** result) = 0;

  // Create an object supporting random-access reads from the file with the
  // specified name.  On success, stores a pointer to the new file in
  // *result and returns OK.  On failure stores nullptr in *result and
  // returns non-OK.  If the file does not exist, returns a non-OK
  // status.  Implementations should return a NotFound status when the file does
  // not exist.
  //
  // The returned file may be concurrently accessed by multiple threads.
  //
  // 创建一个支持随机读的文件对象, 它用来随机读取指定名字的文件. 
  // 如果成功, 将指向一个新文件的指针存储到 *result 并返回 OK. 
  // 如果失败, 将 *result 置为 nullptr 并返回 non-OK. 
  // 如果文件不存在, 返回 NotFound. 
  //
  // 返回的文件允许多个线程并发访问. 
  virtual Status NewRandomAccessFile(const std::string& fname,
                                     RandomAccessFile** result) = 0;

  // Create an object that writes to a new file with the specified
  // name.  Deletes any existing file with the same name and creates a
  // new file.  On success, stores a pointer to the new file in
  // *result and returns OK.  On failure stores nullptr in *result and
  // returns non-OK.
  //
  // The returned file will only be accessed by one thread at a time.
  //
  // 创建一个对象, 它用来写一个指定名字的新文件. 如果同名文件, 那先删除然后创建同名文件. 
  // 如果成功, 将指向一个新文件的指针存储到 *result 并返回 OK. 
  // 如果失败, 将 *result 置为 nullptr 并返回 non-OK. 
  //
  // 返回的文件一次只能只被一个线程访问. 
  virtual Status NewWritableFile(const std::string& fname,
                                 WritableFile** result) = 0;

  // Create an object that either appends to an existing file, or
  // writes to a new file (if the file does not exist to begin with).
  // On success, stores a pointer to the new file in *result and
  // returns OK.  On failure stores nullptr in *result and returns
  // non-OK.
  //
  // The returned file will only be accessed by one thread at a time.
  //
  // May return an IsNotSupportedError error if this Env does
  // not allow appending to an existing file.  Users of Env (including
  // the leveldb implementation) must be prepared to deal with
  // an Env that does not support appending.
  //
  // 创建一个对象, 它要么往一个已存在文件追加内容, 要么写到一个新文件(如果开始的时候这个文件不存在). 
  // 如果成功, 将指向一个新文件的指针存储到 *result 并返回 OK. 
  // 如果失败, 将 *result 置为 nullptr 并返回 non-OK. 
  //
  // 返回的文件一次只能只被一个线程访问. 
  //
  // 如果这个 Env 对象不允许向已存在文件追加, 可以返回一个 IsNotSupportedError, 
  // Env 用户(包括 leveldb 实现)必须能处理前述错误. 
  virtual Status NewAppendableFile(const std::string& fname,
                                   WritableFile** result);

  // Returns true iff the named file exists.
  //
  // 当且仅当指定文件不存在返回 true. 
  virtual bool FileExists(const std::string& fname) = 0;

  // Store in *result the names of the children of the specified directory.
  // The names are relative to "dir".
  // Original contents of *results are dropped.
  //
  // 将目录 dir 包含的全部目录项的名字存储到 *result, 这些名字是相对于指定的目录来说的. 
  // *result 原有内容会被丢弃, 请事先保存. 
  virtual Status GetChildren(const std::string& dir,
                             std::vector<std::string>* result) = 0;

  // Delete the named file.
  //
  // 删除指定名字文件
  virtual Status DeleteFile(const std::string& fname) = 0;

  // Create the specified directory.
  //
  // 创建指定名字目录
  virtual Status CreateDir(const std::string& dirname) = 0;

  // Delete the specified directory.
  //
  // 删除指定名字目录
  virtual Status DeleteDir(const std::string& dirname) = 0;

  // Store the size of fname in *file_size.
  //
  // 将指定名字文件的大小存储到 *file_size. 
  virtual Status GetFileSize(const std::string& fname, uint64_t* file_size) = 0;

  // Rename file src to target.
  //
  // 将原名字为 src 的文件重命名为 target. 
  virtual Status RenameFile(const std::string& src,
                            const std::string& target) = 0;

  // Lock the specified file.  Used to prevent concurrent access to
  // the same db by multiple processes.  On failure, stores nullptr in
  // *lock and returns non-OK.
  //
  // On success, stores a pointer to the object that represents the
  // acquired lock in *lock and returns OK.  The caller should call
  // UnlockFile(*lock) to release the lock.  If the process exits,
  // the lock will be automatically released.
  //
  // If somebody else already holds the lock, finishes immediately
  // with a failure.  I.e., this call does not wait for existing locks
  // to go away.
  //
  // May create the named file if it does not already exist.
  //
  // 锁定指定名字的文件. 用于防止多个进程并发访问同一个数据库. 
  // 如果失败, 将 nullptr 存储到 *lock 并返回 non-OK. 
  //
  // 如果成功, 将一个对象指针存储到 *lock, 这个对象表示已获取到的锁, 并返回 OK. 调用者应该
  // 调用 UnlockFile(*lock) 来释放锁. 如果进程退出, 锁会被自动释放. 
  //
  // 如果其它人已经获得了这把锁, 该方法会快速失败, 也就是说, 这个方法不会等到锁可用. 
  //
  // 如果文件不存在, 则创建该名字的文件. 
  virtual Status LockFile(const std::string& fname, FileLock** lock) = 0;

  // Release the lock acquired by a previous successful call to LockFile.
  // REQUIRES: lock was returned by a successful LockFile() call
  // REQUIRES: lock has not already been unlocked.
  //
  // 释放通过前一个方法成功调用后获得的锁. 
  // 前提: 必须已经通过 LockFile 方法获得了 lock; lock 还未被释放过. 
  virtual Status UnlockFile(FileLock* lock) = 0;

  // Arrange to run "(*function)(arg)" once in a background thread.
  //
  // "function" may run in an unspecified thread.  Multiple functions
  // added to the same Env may run concurrently in different threads.
  // I.e., the caller may not assume that background work items are
  // serialized.
  //
  // 安排在后台线程运行一次 (*function)(args), 该方法实际做的就是把参数中的内容加入任务队列中. 
  //
  // function 可以在非指定的线程运行. 被加入到同一个 Env 的多个 functions 可用并发的运行在不同的线程中. 
  // 也就是说, 调用者不用假设后台工作任务串行执行. 
  virtual void Schedule(
      void (*function)(void* arg),
      void* arg) = 0;

  // Start a new thread, invoking "function(arg)" within the new thread.
  // When "function(arg)" returns, the thread will be destroyed.
  //
  // 启动一个新线程, 并在这个线程中调用 function(arg). 
  // 当 function(arg) 返回的时候, 线程将会被销毁. 
  virtual void StartThread(void (*function)(void* arg), void* arg) = 0;

  // *path is set to a temporary directory that can be used for testing. It may
  // or many not have just been created. The directory may or may not differ
  // between runs of the same process, but subsequent calls will return the
  // same directory.
  //
  // *path 会被设置为一个临时目录, 从来做测试. 
  // 是否事先创建该目录都无所谓. 这个目录在同一个进程多次运行时可能不同, 但是后续的调用都返回同一个目录(? ). 
  virtual Status GetTestDirectory(std::string* path) = 0;

  // Create and return a log file for storing informational messages.
  //
  // 创建并返回一个 log 文件, 用来存储日志. 
  virtual Status NewLogger(const std::string& fname, Logger** result) = 0;

  // Returns the number of micro-seconds since some fixed point in time. Only
  // useful for computing deltas of time.
  //
  // 返回某个固定时间后的微秒数. 
  // 只有在计算时间差时候有用. 
  virtual uint64_t NowMicros() = 0;

  // Sleep/delay the thread for the prescribed number of micro-seconds.
  //
  // 令线程睡眠/延迟指定的微秒数. 
  virtual void SleepForMicroseconds(int micros) = 0;
};

// A file abstraction for reading sequentially through a file
//
// 该类是一个文件抽象, 用于顺序读取文件(这个类竟然没有 close 方法)
class LEVELDB_EXPORT SequentialFile {
 public:
  SequentialFile() = default;

  SequentialFile(const SequentialFile&) = delete;
  SequentialFile& operator=(const SequentialFile&) = delete;

  virtual ~SequentialFile();

  // Read up to "n" bytes from the file.  "scratch[0..n-1]" may be
  // written by this routine.  Sets "*result" to the data that was
  // read (including if fewer than "n" bytes were successfully read).
  // May set "*result" to point at data in "scratch[0..n-1]", so
  // "scratch[0..n-1]" must be live when "*result" is used.
  // If an error was encountered, returns a non-OK status.
  //
  // REQUIRES: External synchronization
  //
  // 从文件最多读取 n 字节, 读取内容存储到 scratch 指向空间, 然后把 scratch 封装在 result 里面, 
  // 所以这两者生命期要协调好. 如果出错, 返回 non-OK. 
  //
  // 前提: 调用该方法需要使用外部同步设施. 
  virtual Status Read(size_t n, Slice* result, char* scratch) = 0;

  // Skip "n" bytes from the file. This is guaranteed to be no
  // slower that reading the same data, but may be faster.
  //
  // If end of file is reached, skipping will stop at the end of the
  // file, and Skip will return OK.
  //
  // REQUIRES: External synchronization
  //
  // 跳过文件中 n 个字节, 这个可以用于保证读得更快. 
  //
  // 如果前进 n 超过了文件末尾, 那就会直接停在文件尾部, 同时返回 OK. 
  //
  // 前提: 调用该方法需要使用外部同步设施. 
  virtual Status Skip(uint64_t n) = 0;
};

// A file abstraction for randomly reading the contents of a file.
//
// 该类是一个文件抽象, 用于随机读取文件内容. 
class LEVELDB_EXPORT RandomAccessFile {
 public:
  RandomAccessFile() = default;

  RandomAccessFile(const RandomAccessFile&) = delete;
  RandomAccessFile& operator=(const RandomAccessFile&) = delete;

  virtual ~RandomAccessFile();

  // Read up to "n" bytes from the file starting at "offset".
  // "scratch[0..n-1]" may be written by this routine.  Sets "*result"
  // to the data that was read (including if fewer than "n" bytes were
  // successfully read).  May set "*result" to point at data in
  // "scratch[0..n-1]", so "scratch[0..n-1]" must be live when
  // "*result" is used.  If an error was encountered, returns a non-OK
  // status.
  //
  // Safe for concurrent use by multiple threads.
  //
  // 从 offset 开始读取最多 n 个字节, 读取内容存储到 scratch 指向空间, 然后把 scratch 封装在 result 里面, 
  // 所以这两者生命期要协调好. 如果出错, 返回 non-OK. 
  //
  // 该方法线程安全, 可以被多个线程并发调用. 
  virtual Status Read(uint64_t offset, size_t n, Slice* result,
                      char* scratch) const = 0;
};

// A file abstraction for sequential writing.  The implementation
// must provide buffering since callers may append small fragments
// at a time to the file.
//
// 针对顺序写入的文件抽象. 
// 具体实现需要提供缓存, 因为调用者可能会一次追加多个小片段到该文件. 
class LEVELDB_EXPORT WritableFile {
 public:
  WritableFile() = default;

  WritableFile(const WritableFile&) = delete;
  WritableFile& operator=(const WritableFile&) = delete;

  virtual ~WritableFile();

  virtual Status Append(const Slice& data) = 0;
  virtual Status Close() = 0;
  virtual Status Flush() = 0;
  // 同步文件和缓存内容到磁盘, 针对 manifest 文件需要额外处理. 
  virtual Status Sync() = 0;
};

// An interface for writing log messages.
//
// logger 接口, 具体实现见 PosixLogger
class LEVELDB_EXPORT Logger {
 public:
  Logger() = default;

  Logger(const Logger&) = delete;
  Logger& operator=(const Logger&) = delete;

  virtual ~Logger();

  // Write an entry to the log file with the specified format.
  virtual void Logv(const char* format, va_list ap) = 0;
};

// Identifies a locked file.
//
// 文件锁接口
class LEVELDB_EXPORT FileLock {
 public:
  FileLock() = default;

  FileLock(const FileLock&) = delete; // 锁不允许拷贝
  FileLock& operator=(const FileLock&) = delete; // 也不允许赋值

  virtual ~FileLock();
};

// Log the specified data to *info_log if info_log is non-null.
//
// 关于这里 __attribute__ 的使用, 请见 http://blog.51cto.com/afreez/7351
// __attribute__ format
// 该__attribute__属性可以给被声明的函数加上类似printf或者scanf的特征, 它可以使编译器检查函数声明和函数实际调用参数之间的格式化字符串是否匹配. 该功能十分有用, 尤其是处理一些很难发现的bug. 
// format的语法格式为: 
// format (archetype, string-index, first-to-check)
//    format属性告诉编译器, 按照printf, scanf, strftime或strfmon的参数表格式规则对该函数的参数进行检查. "archetype" 指定是哪种风格; "string-index" 指定传入函数的第几个参数是格式化字符串; "first-to-check" 指定从函数的第几个参数开始按上述规则进行检查
void Log(Logger* info_log, const char* format, ...)
#   if defined(__GNUC__) || defined(__clang__)
    __attribute__((__format__ (__printf__, 2, 3)))
#   endif
    ;

// A utility routine: write "data" to the named file.
LEVELDB_EXPORT Status WriteStringToFile(Env* env, const Slice& data,
                                        const std::string& fname);

// A utility routine: read contents of named file into *data
LEVELDB_EXPORT Status ReadFileToString(Env* env, const std::string& fname,
                                       std::string* data);

// An implementation of Env that forwards all calls to another Env.
// May be useful to clients who wish to override just part of the
// functionality of another Env.
//
// 一个 Env 的实现, 具体就是把全部调用转发给另一个 Env. 
// 如果客户端仅仅想覆盖部分方法, 这个类比较有用. 
class LEVELDB_EXPORT EnvWrapper : public Env {
 public:
  // Initialize an EnvWrapper that delegates all calls to *t.
  explicit EnvWrapper(Env* t) : target_(t) { }
  virtual ~EnvWrapper();

  // Return the target to which this Env forwards all calls.
  Env* target() const { return target_; }

  // The following text is boilerplate that forwards all methods to target().
  Status NewSequentialFile(const std::string& f, SequentialFile** r) override {
    return target_->NewSequentialFile(f, r);
  }
  Status NewRandomAccessFile(const std::string& f,
                             RandomAccessFile** r) override {
    return target_->NewRandomAccessFile(f, r);
  }
  Status NewWritableFile(const std::string& f, WritableFile** r) override {
    return target_->NewWritableFile(f, r);
  }
  Status NewAppendableFile(const std::string& f, WritableFile** r) override {
    return target_->NewAppendableFile(f, r);
  }
  bool FileExists(const std::string& f) override {
    return target_->FileExists(f);
  }
  Status GetChildren(const std::string& dir,
                     std::vector<std::string>* r) override {
    return target_->GetChildren(dir, r);
  }
  Status DeleteFile(const std::string& f) override {
    return target_->DeleteFile(f);
  }
  Status CreateDir(const std::string& d) override {
    return target_->CreateDir(d);
  }
  Status DeleteDir(const std::string& d) override {
    return target_->DeleteDir(d);
  }
  Status GetFileSize(const std::string& f, uint64_t* s) override {
    return target_->GetFileSize(f, s);
  }
  Status RenameFile(const std::string& s, const std::string& t) override {
    return target_->RenameFile(s, t);
  }
  Status LockFile(const std::string& f, FileLock** l) override {
    return target_->LockFile(f, l);
  }
  Status UnlockFile(FileLock* l) override { return target_->UnlockFile(l); }
  void Schedule(void (*f)(void*), void* a) override {
    return target_->Schedule(f, a);
  }
  void StartThread(void (*f)(void*), void* a) override {
    return target_->StartThread(f, a);
  }
  Status GetTestDirectory(std::string* path) override {
    return target_->GetTestDirectory(path);
  }
  Status NewLogger(const std::string& fname, Logger** result) override {
    return target_->NewLogger(fname, result);
  }
  uint64_t NowMicros() override {
    return target_->NowMicros();
  }
  void SleepForMicroseconds(int micros) override {
    target_->SleepForMicroseconds(micros);
  }

 private:
  Env* target_;
};

}  // namespace leveldb

#endif  // STORAGE_LEVELDB_INCLUDE_ENV_H_

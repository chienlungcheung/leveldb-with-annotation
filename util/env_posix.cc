// Copyright (c) 2011 The LevelDB Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file. See the AUTHORS file for names of contributors.

#include <dirent.h>
#include <fcntl.h>
#include <pthread.h>
#include <sys/mman.h>
#include <sys/resource.h>
#include <sys/stat.h>
#include <sys/time.h>
#include <sys/types.h>
#include <unistd.h>

#include <atomic>
#include <cerrno>
#include <cstddef>
#include <cstdint>
#include <cstdio>
#include <cstdlib>
#include <cstring>
#include <limits>
#include <queue>
#include <set>
#include <string>
#include <thread>
#include <type_traits>
#include <utility>

#include "leveldb/env.h"
#include "leveldb/slice.h"
#include "leveldb/status.h"
#include "port/port.h"
#include "port/thread_annotations.h"
#include "util/posix_logger.h"
#include "util/env_posix_test_helper.h"

// HAVE_FDATASYNC is defined in the auto-generated port_config.h, which is
// included by port_stdcxx.h.
#if !HAVE_FDATASYNC
// 前者与后者类似, 不过后者无差别将目标文件的元数据刷到磁盘上, 
// 但是前者只是将必要的(下次读数据时必须依赖的, 比如文件大小, 但但最后一次修改时间或者访问时间就不算)
// 元数据刷新到磁盘上, 所以针对那些不要求全部元数据被刷新到磁盘的应用, 这样减少了磁盘活动. 
#define fdatasync fsync
#endif  // !HAVE_FDATASYNC

namespace leveldb {

namespace {

// Set by EnvPosixTestHelper::SetReadOnlyMMapLimit() and MaxOpenFiles().
int g_open_read_only_file_limit = -1;

// Up to 1000 mmap regions for 64-bit binaries; none for 32-bit.
// 针对 64 位系统(此时指针大小最少为 8 字节)最多可以使用 1000 个 mmap regions; 32 位系统不使用 mmap. 
constexpr const int kDefaultMmapLimit = (sizeof(void*) >= 8) ? 1000 : 0;

// Can be set using EnvPosixTestHelper::SetReladOnlyMMapLimit.
int g_mmap_limit = kDefaultMmapLimit;

// 写文件缓存, 64 KB
constexpr const size_t kWritableFileBufferSize = 65536;

// 将 posix error 转换为 Status
Status PosixError(const std::string& context, int error_number) {
  if (error_number == ENOENT) {
    return Status::NotFound(context, std::strerror(error_number));
  } else {
    return Status::IOError(context, std::strerror(error_number));
  }
}

// Helper class to limit resource usage to avoid exhaustion.
// Currently used to limit read-only file descriptors and mmap file usage
// so that we do not run out of file descriptors or virtual memory, or run into
// kernel performance problems for very large databases.
//
// 一个帮助类(本质是一个原子计数器), 用于限制资源使用, 防止其耗尽. 
// 当前该类只用于限制只读文件描述符个数以及 mmap file 的使用, 所以我们不会耗尽文件描述符个数或者虚拟内存, 
// 也不会因为数据库超大导致内核性能问题. 
class Limiter {
 public:
  // Limit maximum number of resources to |max_acquires|.
  //
  // 将某资源的使用上限设置为 max_acquires. 
  // 允许将一个整形数隐式地转换为一个 Limiter 对象. 
  Limiter(int max_acquires) : acquires_allowed_(max_acquires) {}

  Limiter(const Limiter&) = delete;
  Limiter operator=(const Limiter&) = delete;

  // If another resource is available, acquire it and return true.
  // Else return false.
  //
  // 如果还有资源可用, 获取它并返回 true; 否则返回 false. 
  bool Acquire() {
    // 修改顺序中立即前趋此函数效果的值
    int old_acquires_allowed =
        acquires_allowed_.fetch_sub(1, std::memory_order_relaxed);

    if (old_acquires_allowed > 0)
      return true;

    acquires_allowed_.fetch_add(1, std::memory_order_relaxed);
    return false;
  }

  // Release a resource acquired by a previous call to Acquire() that returned
  // true.
  //
  // 归还之前通过 Acquire() 方法成功获取到的资源. 
  void Release() {
    acquires_allowed_.fetch_add(1, std::memory_order_relaxed);
  }

 private:
  // The number of available resources.
  //
  // This is a counter and is not tied to the invariants of any other class, so
  // it can be operated on safely using std::memory_order_relaxed.
  //
  // 下面成员变量定义了资源的可用数目. 
  // 由于这是一个计数器, 而且不和外部任何类构成不变式, 所以针对它的操作可安全地使用 std::memory_order_relaxed.
  //
  // 下面内容详见 https://zh.cppreference.com/w/cpp/atomic/memory_order
  //
  // std::memory_order 指定常规的非原子内存访问如何围绕原子操作排序. 
  //
  // 库中所有原子操作的默认行为提供序列一致顺序(见后述讨论). 该默认行为可能有损性能, 
  //   不过可以给予库的原子操作额外的 std::memory_order 参数, 以指定附加制约, 在原子性外(注意, 原子性和操作顺序是两件事), 
  //   编译器和处理器还必须强制该操作. 
  // - memory_order_relaxed	宽松操作：没有同步或顺序制约, 仅对此操作要求原子性. (宽松顺序见 https://zh.cppreference.com/w/cpp/atomic/memory_order#.E5.AE.BD.E6.9D.BE.E9.A1.BA.E5.BA.8F)
  // - memory_order_consume	有此内存顺序的加载操作, 在其影响的内存位置进行 consume 操作(相比 acquire 粒度更细, 只限制操作对象)：
  //                            当前线程中依赖于当前加载的该值的读或写不能被重排到此加载前. 
  //                        其他释放同一原子变量的线程的对数据依赖变量的写入, 为当前线程所可见. 
  //                        在大多数平台上, 这只影响到编译器优化. 
  // - memory_order_acquire	有此内存顺序的加载操作, 在其影响的内存位置进行 acquire 操作：
  //                            当前线程中在 acquire 后的任意读或写不能被重排到此加载前. 
  //                        其他释放同一原子变量的线程的所有写入, 能为当前线程所见. 
  // - memory_order_release	有此内存顺序的存储操作进行 release 操作：
  //                            当前线程中在 release 之前的任意读或写不能被重排到此存储后. 
  //                        当前线程的所有写入, 可见于获得该同一原子变量的其他线程释放获得顺序, 
  //                        并且对该原子变量的带依赖写入变得对于其他消费同一原子对象的线程可见. 
  // - memory_order_acq_rel	带此内存顺序的读修改写操作既是 acquire 操作又是 release 操作：
  //                            当前线程的任意读或写内存不能被重排到此存储前或后. 
  //                        所有释放同一原子变量的线程的写入可见于修改之前, 而且修改可见于其他获得同一原子变量的线程. 
  // - memory_order_seq_cst	有此内存顺序的加载操作进行 acquire 操作, 存储操作进行 release 操作, 而读改写操作进行 acquire 操作和 release 操作, 
  //                        再加上存在一个单独全序, 其中所有线程以同一顺序观测到所有修改.
  std::atomic<int> acquires_allowed_;
};

// Implements sequential read access in a file using read().
//
// Instances of this class are thread-friendly but not thread-safe, as required
// by the SequentialFile API.
//
// 通过系统调用 read() 实现顺序读. 
//
// 该类的实例是线程友好的(todo？)但不是线程安全的, 正如父类所要求的. 
class PosixSequentialFile final : public SequentialFile {
 public:
  PosixSequentialFile(std::string filename, int fd)
      : fd_(fd), filename_(filename) {}
  ~PosixSequentialFile() override { close(fd_); }

  Status Read(size_t n, Slice* result, char* scratch) override {
    Status status;
    while (true) {
      ::ssize_t read_size = ::read(fd_, scratch, n);
      if (read_size < 0) {  // Read error.
        if (errno == EINTR) {
          continue;  // Retry 被系统 signal 打断则重试
        }
        status = PosixError(filename_, errno);
        break;
      }
      *result = Slice(scratch, read_size);
      break;
    }
    return status;
  }

  Status Skip(uint64_t n) override {
    if (::lseek(fd_, n, SEEK_CUR) == static_cast<off_t>(-1)) {
      return PosixError(filename_, errno);
    }
    return Status::OK();
  }

 private:
  const int fd_;
  const std::string filename_;
};

// Implements random read access in a file using pread().
//
// Instances of this class are thread-safe, as required by the RandomAccessFile
// API. Instances are immutable and Read() only calls thread-safe library
// functions.
//
// 通过系统调用 pread() 实现了对文件的随机读取. 
//
// 该类的实例是线程安全的, 正如父类所要求的. 该类的实例是 immutable 的并且 Read() 方法只调用线程安全的库方法. 
class PosixRandomAccessFile final : public RandomAccessFile {
 public:
  // The new instance takes ownership of |fd|. |fd_limiter| must outlive this
  // instance, and will be used to determine if .
  //
  // 注意这里的参数 fd、fd_limiter 的生命期要长于该类实例. 
  PosixRandomAccessFile(std::string filename, int fd, Limiter* fd_limiter)
      : has_permanent_fd_(fd_limiter->Acquire()), // 如果描述符资源已经耗尽, 则该类实例不能持久地持有该 fd, 用前自己打开, 用完自己关闭. 
        fd_(has_permanent_fd_ ? fd : -1),
        fd_limiter_(fd_limiter),
        filename_(std::move(filename)) {
    if (!has_permanent_fd_) {
      assert(fd_ == -1);
      ::close(fd);  // The file will be opened on every read. todo 这里有关闭的必要吗？
    }
  }

  // 关闭文件描述符, 归还占用的描述符资源
  ~PosixRandomAccessFile() override {
    if (has_permanent_fd_) {
      assert(fd_ != -1);
      ::close(fd_);
      fd_limiter_->Release();
    }
  }

  Status Read(uint64_t offset, size_t n, Slice* result,
              char* scratch) const override {
    int fd = fd_;
    if (!has_permanent_fd_) {
      fd = ::open(filename_.c_str(), O_RDONLY);
      if (fd < 0) {
        return PosixError(filename_, errno);
      }
    }

    assert(fd != -1);

    Status status;
    // pread 允许多个线程同时读同一个文件, 互相之间的偏移量各自线程维持, 不会互相影响. 
    ssize_t read_size = ::pread(fd, scratch, n, static_cast<off_t>(offset));
    *result = Slice(scratch, (read_size < 0) ? 0 : read_size);
    if (read_size < 0) {
      // An error: return a non-ok status.
      status = PosixError(filename_, errno);
    }
    if (!has_permanent_fd_) {
      // Close the temporary file descriptor opened earlier.
      assert(fd != fd_);
      ::close(fd);
    }
    return status;
  }

 private:
  // 如果为 false, 则每次读都会打开一次文件. 
  const bool has_permanent_fd_;  // If false, the file is opened on every read.
  // 如果 hash_permanent_fd 为 false, 则该值为 -1. 
  const int fd_;  // -1 if has_permanent_fd_ is false.
  Limiter* const fd_limiter_;
  const std::string filename_;
};

// Implements random read access in a file using mmap().
//
// Instances of this class are thread-safe, as required by the RandomAccessFile
// API. Instances are immutable and Read() only calls thread-safe library
// functions.
//
// 使用系统调用 mmap() 实现了对文件的随机读取. 
//
// 该类的实例是线程安全的, 正如父类所要求的. 该类的实例是 immutable 的并且 Read() 方法只调用线程安全的库方法. 
class PosixMmapReadableFile final : public RandomAccessFile {
 public:
  // mmap_base[0, length-1] points to the memory-mapped contents of the file. It
  // must be the result of a successful call to mmap(). This instances takes
  // over the ownership of the region.
  //
  // |mmap_limiter| must outlive this instance. The caller must have already
  // aquired the right to use one mmap region, which will be released when this
  // instance is destroyed.
  //
  // mmap_base[0, length-1] 指向通过内存映射的文件的内容, 
  // 它必须是成功调用 mmap() 后的结果(生成该类实例之前要先调用 mmap 获取 mmap_base). 
  // 该实例将会持有 mmap region 的所有权. 
  //
  // mmap_limiter 的生命期要长于该类的实例. 调用者必须实现获取使用一个 mmap region 的权利, mmap region 在该类实例销毁时被释放. 
  PosixMmapReadableFile(std::string filename, char* mmap_base, size_t length,
                        Limiter* mmap_limiter)
      : mmap_base_(mmap_base), length_(length), mmap_limiter_(mmap_limiter),
        filename_(std::move(filename)) {}

  ~PosixMmapReadableFile() override {
    ::munmap(static_cast<void*>(mmap_base_), length_); // 进程退出也会自动进行 munmap
    mmap_limiter_->Release();
  }

  Status Read(uint64_t offset, size_t n, Slice* result,
              char* scratch) const override {
    if (offset + n > length_) {
      *result = Slice();
      return PosixError(filename_, EINVAL);
    }

    *result = Slice(mmap_base_ + offset, n);
    return Status::OK();
  }

 private:
  char* const mmap_base_;
  const size_t length_;
  Limiter* const mmap_limiter_;
  const std::string filename_;
};

// 顺序写入的文件抽象 posix 实现
class PosixWritableFile final : public WritableFile {
 public:
  PosixWritableFile(std::string filename, int fd)
      : pos_(0), fd_(fd), is_manifest_(IsManifest(filename)), // 生成实例的时候会顺便判断对应的写入文件是否为 MANIFEST
        filename_(std::move(filename)), dirname_(Dirname(filename_)) {}

  ~PosixWritableFile() override {
    if (fd_ >= 0) {
      // Ignoring any potential errors
      Close();
    }
  }

  // 将 data 包含内容追加到内部缓存或者文件
  Status Append(const Slice& data) override {
    size_t write_size = data.size();
    const char* write_data = data.data();

    // Fit as much as possible into buffer.
    size_t copy_size = std::min(write_size, kWritableFileBufferSize - pos_);
    std::memcpy(buf_ + pos_, write_data, copy_size); // 先写到缓存里
    write_data += copy_size;
    write_size -= copy_size;
    pos_ += copy_size;
    if (write_size == 0) { // 如果写完了则返回
      return Status::OK();
    }

    // Can't fit in buffer, so need to do at least one write.
    //
    // 要写的内容当前缓存剩余空间放不下, 需要先 flush 缓存到文件, 然后再继续写入剩余内容
    Status status = FlushBuffer();
    if (!status.ok()) {
      return status;
    }

    // Small writes go to buffer, large writes are written directly.、
    //
    // 如果剩余待写内容能被缓存容纳, 则写入缓存后返回; 
    // 否则, 剩余待写内容不写入缓存而是直接写入文件. 
    if (write_size < kWritableFileBufferSize) {
      std::memcpy(buf_, write_data, write_size);
      pos_ = write_size;
      return Status::OK();
    }
    return WriteUnbuffered(write_data, write_size);
  }

  // 关闭实例需要先 flush 缓存到文件, 然后关闭文件描述符(不管前一步是否 flush 成功). 
  // 如果关闭失败并且 flush 失败, 则构造失败状态返回给调用者; 否则只返回 flush 状态给调用者. 
  Status Close() override {
    Status status = FlushBuffer();
    const int close_result = ::close(fd_);
    if (close_result < 0 && status.ok()) {
      status = PosixError(filename_, errno);
    }
    fd_ = -1;
    return status;
  }

  // 将缓存内容 flush 到文件
  Status Flush() override {
    return FlushBuffer();
  }

  // 同步文件和缓存内容到磁盘, 针对 manifest 文件需要额外处理. 
  Status Sync() override {
    // Ensure new files referred to by the manifest are in the filesystem.
    //
    // This needs to happen before the manifest file is flushed to disk, to
    // avoid crashing in a state where the manifest refers to files that are not
    // yet on disk.
    //
    // 确保被 manifest 引用的新文件都在文件系统里. 为了做到这一点, 需要在 manifest 文件被刷新到磁盘之前将 manifest 引用的文件
    // 同步到操作系统, 这样才能避免系统崩溃时 manifest 文件存在但被它引用的文件却不存在的情况. 
    //
    // 如果当前文件是 manifest 文件, 则先同步 manifest 文件同目录的文件到磁盘. 
    Status status = SyncDirIfManifest();
    if (!status.ok()) {
      return status;
    }

    // 将缓存内容写入到磁盘. 
    status = FlushBuffer();
    if (status.ok() && ::fdatasync(fd_) != 0) { // 将必要的文件元数据(文件大小等)同步到磁盘, 相比 fsync
      status = PosixError(filename_, errno);
    }
    return status;
  }

 private:
  // 将缓存内容写到文件
  Status FlushBuffer() {
    Status status = WriteUnbuffered(buf_, pos_);
    pos_ = 0;
    return status;
  }

  // 将缓存内容写入文件(通过 write 系统调用实现)
  Status WriteUnbuffered(const char* data, size_t size) {
    while (size > 0) {
      ssize_t write_result = ::write(fd_, data, size);
      if (write_result < 0) {
        if (errno == EINTR) {
          continue;  // Retry 如果被 signal 打断则重试
        }
        return PosixError(filename_, errno); // 其它 IO 错误则停止写入
      }
      data += write_result;
      size -= write_result;
    }
    return Status::OK();
  }

  // 如果当前文件为 manifest 文件, 则同步其所属目录的改变(包括数据和元数据)到磁盘(通过 fsync 系统调用实现)
  Status SyncDirIfManifest() {
    Status status;
    if (!is_manifest_) {
      return status;
    }

    int fd = ::open(dirname_.c_str(), O_RDONLY); // 打开该 manifest 文件所在目录
    if (fd < 0) {
      status = PosixError(dirname_, errno);
    } else {
      if (::fsync(fd) < 0) { // 对该目录进行 sync 操作
        status = PosixError(dirname_, errno);
      }
      ::close(fd); // 不管是否 sync 成功, 都要关闭文件描述符
    }
    return status;
  }

  // Returns the directory name in a path pointing to a file.
  //
  // Returns "." if the path does not contain any directory separator.
  //
  // 从一个指向文件的 path 中抽取目录部分. 如果 path 不包含任何目录分隔符, 返回 ".", 即当前目录. 
  static std::string Dirname(const std::string& filename) {
    std::string::size_type separator_pos = filename.rfind('/'); // 返回最后一个 '/' 所对应的索引
    if (separator_pos == std::string::npos) {
      return std::string(".");
    }
    // The filename component should not contain a path separator. If it does,
    // the splitting was done incorrectly.
    assert(filename.find('/', separator_pos + 1) == std::string::npos);

    return filename.substr(0, separator_pos);
  }

  // Extracts the file name from a path pointing to a file.
  //
  // The returned Slice points to |filename|'s data buffer, so it is only valid
  // while |filename| is alive and unchanged.
  //
  // 从一个指向文件的 path 中抽取文件名部分. 
  //
  // 返回的 slice 指向 filename 的数据内存部分, 所以前者生命期和内容取决于后者. 
  static Slice Basename(const std::string& filename) {
    std::string::size_type separator_pos = filename.rfind('/');
    if (separator_pos == std::string::npos) {
      return Slice(filename);
    }
    // The filename component should not contain a path separator. If it does,
    // the splitting was done incorrectly.
    assert(filename.find('/', separator_pos + 1) == std::string::npos);

    return Slice(filename.data() + separator_pos + 1,
                 filename.length() - separator_pos - 1);
  }

  // True if the given file is a manifest file.
  //
  // 判断给定的文件是否是一个 manifest 文件. 
  static bool IsManifest(const std::string& filename) {
    return Basename(filename).starts_with("MANIFEST");
  }

  // buf_[0, pos_ - 1] contains data to be written to fd_.
  // 父类要求具体实现要提供缓存, 因为调用者可能会一次追加多个小片段到该文件. 
  // buf[0, pos - 1] 包含的数据就是要写入 fd 的. 
  char buf_[kWritableFileBufferSize];
  size_t pos_; // 指向 buf 当前位置
  int fd_; // 指向要写入的文件

  // 如果文件名字以 MANIFEST 开头则为 true
  const bool is_manifest_;  // True if the file's name starts with MANIFEST.
  const std::string filename_;
  // filename 所在的目录名
  const std::string dirname_;  // The directory of filename_.
};

// 根据 lock 取值决定锁定或者解锁 fd 指向的文件, 锁是写锁, 通过系统数据结构 flock 实现. 
// true 表示锁定, false 表示解锁. 
int LockOrUnlock(int fd, bool lock) {
  errno = 0;
  struct ::flock file_lock_info; // 用于指定要锁定的文件区域
  std::memset(&file_lock_info, 0, sizeof(file_lock_info));
  file_lock_info.l_type = (lock ? F_WRLCK : F_UNLCK); // 文件锁类型: F_RDLCK, F_WRLCK, or F_UNLCK.
  file_lock_info.l_whence = SEEK_SET; // 确定 l_start 的原点
  file_lock_info.l_start = 0;
  file_lock_info.l_len = 0;  // Lock/unlock entire file. 取值为 0 表示直至文件尾部. 
  return ::fcntl(fd, F_SETLK, &file_lock_info);
}

// Instances are thread-safe because they are immutable.
//
// 该类的实例都是线程安全的, 因为是 immutable 的. 
class PosixFileLock : public FileLock {
 public:
  PosixFileLock(int fd, std::string filename)
      : fd_(fd), filename_(std::move(filename)) {}

  int fd() const { return fd_; }
  const std::string& filename() const { return filename_; }

 private:
  const int fd_;
  const std::string filename_;
};

// Tracks the files locked by PosixEnv::LockFile().
//
// We maintain a separate set instead of relying on fcntrl(F_SETLK) because
// fcntl(F_SETLK) does not provide any protection against multiple uses from the
// same process.
//
// Instances are thread-safe because all member data is guarded by a mutex.
//
// 跟踪被 PosixEnv::LockFile 锁定的文件. 
//
// 我们使用一个独立的集合, 而不是依赖 fcntl(F_SETLK), 后者无法让同一个进程重复锁定一个文件, 也就是说后者是不可重入的, 而前者可以模拟实现. 
//
// 该类实例都是线程安全的, 因为全部的数据成员都由 mutex 保护. 
//
// 该类用在了 PosixEnv 的 lockFile 方法中. 
class PosixLockTable {
 public:
  bool Insert(const std::string& fname) LOCKS_EXCLUDED(mu_) {
    mu_.Lock();
    bool succeeded = locked_files_.insert(fname).second; // 只有 insert 执行了, send 才会为 true; 所以重复插入返回 false. 
    mu_.Unlock();
    return succeeded;
  }
  void Remove(const std::string& fname) LOCKS_EXCLUDED(mu_) {
    mu_.Lock();
    locked_files_.erase(fname);
    mu_.Unlock();
  }

 private:
  port::Mutex mu_;
  std::set<std::string> locked_files_ GUARDED_BY(mu_); // 对该 set 的操作均由 mu 负责保护
};

class PosixEnv : public Env {
 public:
  PosixEnv();
  ~PosixEnv() override {
    static char msg[] = "PosixEnv singleton destroyed. Unsupported behavior!\n";
    std::fwrite(msg, 1, sizeof(msg), stderr); // Env 不允许销毁todo
    std::abort();
  }

  Status NewSequentialFile(const std::string& filename,
                           SequentialFile** result) override {
    int fd = ::open(filename.c_str(), O_RDONLY);
    if (fd < 0) {
      *result = nullptr;
      return PosixError(filename, errno);
    }

    *result = new PosixSequentialFile(filename, fd);
    return Status::OK();
  }

  Status NewRandomAccessFile(const std::string& filename,
                             RandomAccessFile** result) override {
    *result = nullptr;
    int fd = ::open(filename.c_str(), O_RDONLY); // 根据指定的文件路径打开一个文件, 只读. 
    if (fd < 0) {
      return PosixError(filename, errno);
    }

    if (!mmap_limiter_.Acquire()) { // 申请一个 mmap 资源
      *result = new PosixRandomAccessFile(filename, fd, &fd_limiter_);
      return Status::OK();
    }

    uint64_t file_size;
    Status status = GetFileSize(filename, &file_size);
    if (status.ok()) {
      // 使用上面打开的文件描述符和申请的 mmap 资源资格, 获取 mmap 内存
      //
      // 如果 addr 为 nullptr, 则由操作系统负责选择 mapping 的内存地址; 
      // 如果用户指定, 也是用来作参考, linux 选择的起始地址总是靠近页面边界. 
      //
      // 最后那个 offset 参数如果要指定, 必须为页面大小的整数倍. 
      void* mmap_base = ::mmap(/*addr=*/nullptr, file_size, PROT_READ,
                               MAP_SHARED, fd, 0);
      if (mmap_base != MAP_FAILED) {
        *result = new PosixMmapReadableFile( // 使用 mmap 内存构造一个随机读文件对象
            filename, reinterpret_cast<char*>(mmap_base), file_size,
            &mmap_limiter_);
      } else {
        status = PosixError(filename, errno);
      }
    }
    ::close(fd); // 通过 mmap 做完映射后, 对应的文件可以关闭了; 注意关闭文件描述符并不会导致 munmap
    if (!status.ok()) {
      mmap_limiter_.Release();
    }
    return status;
  }

  Status NewWritableFile(const std::string& filename,
                         WritableFile** result) override {
    int fd = ::open(filename.c_str(), O_TRUNC | O_WRONLY | O_CREAT, 0644);
    if (fd < 0) {
      *result = nullptr;
      return PosixError(filename, errno);
    }

    *result = new PosixWritableFile(filename, fd);
    return Status::OK();
  }

  Status NewAppendableFile(const std::string& filename,
                           WritableFile** result) override {
    int fd = ::open(filename.c_str(), O_APPEND | O_WRONLY | O_CREAT, 0644); // 注意这个跟上面区别是 O_APPEND
    if (fd < 0) {
      *result = nullptr;
      return PosixError(filename, errno);
    }

    *result = new PosixWritableFile(filename, fd);
    return Status::OK();
  }

  bool FileExists(const std::string& filename) override {
    // 这个系统调用本来是用来检查用户是否有权限打开该文件, 此处使用的是它的副作用, 
    // 毕竟文件存在才能检查权限. 
    return ::access(filename.c_str(), F_OK) == 0;
  }

  Status GetChildren(const std::string& directory_path,
                     std::vector<std::string>* result) override {
    result->clear();
    ::DIR* dir = ::opendir(directory_path.c_str());
    if (dir == nullptr) {
      return PosixError(directory_path, errno);
    }
    struct ::dirent* entry;
    // 取出目录中每一项并将其名称存到 vector 中
    while ((entry = ::readdir(dir)) != nullptr) {
      result->emplace_back(entry->d_name); // 直接在 vector 管理的内存里构造, 而非先通过拷贝构造一个临时对象然后再拷贝到 vector 里面
    }
    ::closedir(dir);
    return Status::OK();
  }

  Status DeleteFile(const std::string& filename) override {
    // 解除某个文件的一个引用, 如果引用数目变为 0, 则删除文件. 
    if (::unlink(filename.c_str()) != 0) {
      return PosixError(filename, errno);
    }
    return Status::OK();
  }

  Status CreateDir(const std::string& dirname) override {
    if (::mkdir(dirname.c_str(), 0755) != 0) {
      return PosixError(dirname, errno);
    }
    return Status::OK();
  }

  Status DeleteDir(const std::string& dirname) override {
    if (::rmdir(dirname.c_str()) != 0) {
      return PosixError(dirname, errno);
    }
    return Status::OK();
  }

  Status GetFileSize(const std::string& filename, uint64_t* size) override {
    struct ::stat file_stat;
    if (::stat(filename.c_str(), &file_stat) != 0) { // 读取文件元数据
      *size = 0;
      return PosixError(filename, errno);
    }
    *size = file_stat.st_size;
    return Status::OK();
  }

  Status RenameFile(const std::string& from, const std::string& to) override {
    if (std::rename(from.c_str(), to.c_str()) != 0) {
      return PosixError(from, errno);
    }
    return Status::OK();
  }

  Status LockFile(const std::string& filename, FileLock** lock) override {
    *lock = nullptr;

    int fd = ::open(filename.c_str(), O_RDWR | O_CREAT, 0644);
    if (fd < 0) {
      return PosixError(filename, errno);
    }

    // 尝试锁定. 插入失败, 表示集合中已经存在相同文件名, 进程已经锁定过该文件了. 
    if (!locks_.Insert(filename)) {
      ::close(fd); // 一个文件可以对应多次打开, 每次打开对应一个文件描述符. 
      return Status::IOError("lock " + filename, "already held by process");
    }

    // 这里是真正地对文件上锁
    if (LockOrUnlock(fd, true) == -1) {
      int lock_errno = errno;
      ::close(fd);
      locks_.Remove(filename); // 锁定失败, 要将该文件从尝试锁定文件列表删除
      return PosixError("lock " + filename, lock_errno);
    }

    *lock = new PosixFileLock(fd, filename); // 锁定成功, 生成一个对应的 PosixFileLock 实例
    return Status::OK();
  }

  Status UnlockFile(FileLock* lock) override {
    PosixFileLock* posix_file_lock = static_cast<PosixFileLock*>(lock);
    if (LockOrUnlock(posix_file_lock->fd(), false) == -1) {
      return PosixError("unlock " + posix_file_lock->filename(), errno);
    }
    locks_.Remove(posix_file_lock->filename());
    ::close(posix_file_lock->fd());
    delete posix_file_lock;
    return Status::OK();
  }

  void Schedule(void (*background_work_function)(void* background_work_arg),
                void* background_work_arg) override;

  void StartThread(void (*thread_main)(void* thread_main_arg),
                   void* thread_main_arg) override;

  Status GetTestDirectory(std::string* result) override {
    const char* env = std::getenv("TEST_TMPDIR"); // 返回给定名字的全局变量, 不存在返回 NULL
    if (env && env[0] != '\0') {
      *result = env;
    } else {
      char buf[100];
      std::snprintf(buf, sizeof(buf), "/tmp/leveldbtest-%d",
                    static_cast<int>(::geteuid()));
      *result = buf;
    }

    // The CreateDir status is ignored because the directory may already exist.
    CreateDir(*result);

    return Status::OK();
  }

  Status NewLogger(const std::string& filename, Logger** result) override {
    std::FILE* fp = std::fopen(filename.c_str(), "w");
    if (fp == nullptr) {
      *result = nullptr;
      return PosixError(filename, errno);
    } else {
      *result = new PosixLogger(fp);
      return Status::OK();
    }
  }

  uint64_t NowMicros() override {
    // 一秒对应的微秒数
    static constexpr uint64_t kUsecondsPerSecond = 1000000;
    struct ::timeval tv;
    // 返回当前时间, 微妙精度. 
    ::gettimeofday(&tv, nullptr);
    return static_cast<uint64_t>(tv.tv_sec) * kUsecondsPerSecond + tv.tv_usec;
  }

  void SleepForMicroseconds(int micros) override {
    ::usleep(micros); // sleep 到指定时间或者来了一个没有拦截或者不能忽略的信号
  }

 private:
  void BackgroundThreadMain(); // 后台线程主逻辑, 不停地从任务队列取出任务去执行

  // 后台线程入口函数, 注意它是 static 的
  static void BackgroundThreadEntryPoint(PosixEnv* env) {
    env->BackgroundThreadMain();
  }

  // Stores the work item data in a Schedule() call.
  //
  // Instances are constructed on the thread calling Schedule() and used on the
  // background thread.
  //
  // This structure is thread-safe beacuse it is immutable.
  //
  // 在 Schedule 方法中执行的工作任务. 
  // 该类的实例都是在调用 Schedule 时候生成的, 然后在后台线程执行. 
  // 该类为线程安全的, 因为它是 immutable 的. 
  struct BackgroundWorkItem {
    explicit BackgroundWorkItem(void (*function)(void* arg), void* arg)
        : function(function), arg(arg) {}

    void (* const function)(void*); // 函数指针是常量, 不可更改
    void* const arg; // 函数参数也是常量, 不可更改
  };


  port::Mutex background_work_mutex_;
  port::CondVar background_work_cv_ GUARDED_BY(background_work_mutex_);
  bool started_background_thread_ GUARDED_BY(background_work_mutex_);

  std::queue<BackgroundWorkItem> background_work_queue_
      GUARDED_BY(background_work_mutex_);

  PosixLockTable locks_;  // Thread-safe.
  Limiter mmap_limiter_;  // Thread-safe.
  Limiter fd_limiter_;  // Thread-safe.
};

// Return the maximum number of concurrent mmaps.
int MaxMmaps() {
  return g_mmap_limit;
}

// Return the maximum number of read-only files to keep open.
int MaxOpenFiles() {
  if (g_open_read_only_file_limit >= 0) {
    return g_open_read_only_file_limit;
  }
  struct ::rlimit rlim;
  if (::getrlimit(RLIMIT_NOFILE, &rlim)) {
    // getrlimit failed, fallback to hard-coded default.
    g_open_read_only_file_limit = 50; // 如果获取资源数失败, 则硬编码为 50, 即最多只能打开 50 个只读文件
  } else if (rlim.rlim_cur == RLIM_INFINITY) {
    g_open_read_only_file_limit = std::numeric_limits<int>::max();
  } else {
    // Allow use of 20% of available file descriptors for read-only files.
    g_open_read_only_file_limit = rlim.rlim_cur / 5; // 只把 20% 的文件描述符用于只读文件
  }
  return g_open_read_only_file_limit;
}

}  // namespace

PosixEnv::PosixEnv()
    : background_work_cv_(&background_work_mutex_),
      started_background_thread_(false),
      mmap_limiter_(MaxMmaps()),
      fd_limiter_(MaxOpenFiles()) {
}

void PosixEnv::Schedule(
    void (*background_work_function)(void* background_work_arg),
    void* background_work_arg) {
  background_work_mutex_.Lock(); // 后面涉及几个状态需要锁保护

  // Start the background thread, if we haven't done so already.
  // 如果之前从未启动过后台线程, 则生成一个并让其独立运行
  if (!started_background_thread_) {
    started_background_thread_ = true;
    std::thread background_thread(PosixEnv::BackgroundThreadEntryPoint, this);
    // 调用 detach 后, 线程执行部分与线程对象分离, 独立去运行, 线程对象不再拥有对执行部分的所有权. 
    // 线程执行完毕会自动释放全部分配的资源. 
    background_thread.detach();
  }

  // If the queue is empty, the background thread may be waiting for work.
  // 如果队列为空, 后台线程可能在等待, 激活之
  if (background_work_queue_.empty()) {
    background_work_cv_.Signal();
  }

  // 将本次调度的任务加到任务队列里, 通过 emplace 直接构造避免了中间临时变量生成和拷贝. 
  background_work_queue_.emplace(background_work_function, background_work_arg);
  background_work_mutex_.Unlock();
}

// 循环地从工作队列中取出任务执行, 如果队列为空则等待. 
void PosixEnv::BackgroundThreadMain() {
  while (true) {
    background_work_mutex_.Lock();

    // Wait until there is work to be done.
    while (background_work_queue_.empty()) {
      background_work_cv_.Wait(); // 任务队列为空则等待
    }

    assert(!background_work_queue_.empty());
    auto background_work_function =
        background_work_queue_.front().function;
    void* background_work_arg = background_work_queue_.front().arg;
    background_work_queue_.pop();

    background_work_mutex_.Unlock();
    background_work_function(background_work_arg); // 执行一个工作任务
  }
}

namespace {

// Wraps an Env instance whose destructor is never created.
//
// 封装了一个 Env 具体实现, 该实现的析构函数永远不会被调用. 具体用法如下：
// Intended usage:
//   using PlatformSingletonEnv = SingletonEnv<PlatformEnv>;
//   void ConfigurePosixEnv(int param) {
//     PlatformSingletonEnv::AssertEnvNotInitialized();
//     // set global configuration flags.
//   }
//   Env* Env::Default() {
//     static PlatformSingletonEnv default_env;
//     return default_env.env();
//   }
template<typename EnvType>
class SingletonEnv {
 public:
  SingletonEnv() {
#if !defined(NDEBUG)
    env_initialized_.store(true, std::memory_order::memory_order_relaxed); // 只保证原子性, 不保证线程内执行顺序以及多线程可见性. 
#endif  // !defined(NDEBUG)
    static_assert(sizeof(env_storage_) >= sizeof(EnvType),
                  "env_storage_ will not fit the Env");
    static_assert(alignof(decltype(env_storage_)) >= alignof(EnvType),
                  "env_storage_ does not meet the Env's alignment needs");
    new (&env_storage_) EnvType(); // 定位 new, 在 env_storage 处构造 EnvType 对象. 
  }
  ~SingletonEnv() = default;

  SingletonEnv(const SingletonEnv&) = delete;
  SingletonEnv& operator=(const SingletonEnv&) = delete;

  Env* env() { return reinterpret_cast<Env*>(&env_storage_); }

  static void AssertEnvNotInitialized() {
#if !defined(NDEBUG)
    assert(!env_initialized_.load(std::memory_order::memory_order_relaxed));
#endif  // !defined(NDEBUG)
  }

 private:
  typename std::aligned_storage<sizeof(EnvType), alignof(EnvType)>::type
      env_storage_;
#if !defined(NDEBUG)
  static std::atomic<bool> env_initialized_;
#endif  // !defined(NDEBUG)
};

#if !defined(NDEBUG)
template<typename EnvType>
std::atomic<bool> SingletonEnv<EnvType>::env_initialized_;
#endif  // !defined(NDEBUG)

using PosixDefaultEnv = SingletonEnv<PosixEnv>;

}  // namespace

void PosixEnv::StartThread(void (*thread_main)(void* thread_main_arg),
                           void* thread_main_arg) {
  std::thread new_thread(thread_main, thread_main_arg);
  new_thread.detach();
}

void EnvPosixTestHelper::SetReadOnlyFDLimit(int limit) {
  PosixDefaultEnv::AssertEnvNotInitialized();
  g_open_read_only_file_limit = limit;
}

void EnvPosixTestHelper::SetReadOnlyMMapLimit(int limit) {
  PosixDefaultEnv::AssertEnvNotInitialized();
  g_mmap_limit = limit;
}

Env* Env::Default() {
  static PosixDefaultEnv env_container;
  return env_container.env();
}

}  // namespace leveldb

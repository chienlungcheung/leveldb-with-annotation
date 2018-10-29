// Copyright (c) 2011 The LevelDB Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file. See the AUTHORS file for names of contributors.

// AtomicPointer provides storage for a lock-free pointer.
// Platform-dependent implementation of AtomicPointer:
// - If the platform provides a cheap barrier, we use it with raw pointers
// - If <atomic> is present (on newer versions of gcc, it is), we use
//   a <atomic>-based AtomicPointer.  However we prefer the memory
//   barrier based version, because at least on a gcc 4.4 32-bit build
//   on linux, we have encountered a buggy <atomic> implementation.
//   Also, some <atomic> implementations are much slower than a memory-barrier
//   based implementation (~16ns for <atomic> based acquire-load vs. ~1ns for
//   a barrier based acquire-load).
// This code is based on atomicops-internals-* in Google's perftools:
// http://code.google.com/p/google-perftools/source/browse/#svn%2Ftrunk%2Fsrc%2Fbase

/**
 * AtomicPointer 用来实现无锁指针。
 *
 * 依赖于具体平台的 AtomicPointer 实现如下：
 *
 * - 如果平台提供了方便的内存屏障，我们就直接使用原始指针
 * - 如果 <atomic> 头文件存在（在新版的 gcc 中就有），我们使用基于 <atomic> 的 AtomicPointer。但
 *   是我们更倾向于使用基于内存屏障的版本，因为至少在 gcc 4.4 32-bit linux 版本里面我们发现 <atomic> 有 bug。
 *   同时，我们也发现一些 <atomic> 实现在性能上不如基于内存屏障的版本（比如基于 <atomic> 的 acquire-load 实现
 *   耗时大约 16 纳秒，而基于内存屏障的 acquire-load 实现耗时只需要大约 1 纳秒）。
 */
#ifndef PORT_ATOMIC_POINTER_H_
#define PORT_ATOMIC_POINTER_H_

#include <stdint.h>

#include <atomic>

#ifdef OS_WIN
#include <windows.h>
#endif

#if defined(_M_X64) || defined(__x86_64__)
#define ARCH_CPU_X86_FAMILY 1
#elif defined(_M_IX86) || defined(__i386__) || defined(__i386)
#define ARCH_CPU_X86_FAMILY 1
#elif defined(__ARMEL__)
#define ARCH_CPU_ARM_FAMILY 1
#elif defined(__aarch64__)
#define ARCH_CPU_ARM64_FAMILY 1
#elif defined(__ppc__) || defined(__powerpc__) || defined(__powerpc64__)
#define ARCH_CPU_PPC_FAMILY 1
#elif defined(__mips__)
#define ARCH_CPU_MIPS_FAMILY 1
#endif

namespace leveldb {
namespace port {
/**
 * 注意这两个宏定义 ARCH_CPU_X86_FAMILY 与 __GNUC__，在 x86-64 架构且有 GCC，则会使用平台提供的内存屏障，
 * 即使有 <atomic>。如果前述两个宏被定义，则会定义 LEVELDB_HAVE_MEMORY_BARRIER 宏，则最后的  AtomicPointer 实现
 * 就是基于平台内存屏障而非 C++11 提供的 <atomic>。
 *
 * 具体实现为 __asm__ __volatile__("" : : : "memory")，就这一行代码。
 *
 * 具体原理请见 http://gcc.gnu.org/ml/gcc/2003-04/msg01180.html 以及 http://en.wikipedia.org/wiki/Memory_ordering
 *
 * 内存屏障的意义就是阻止编译器对内存屏障前后的代码进行重排，确保屏障前的 stores 肯定能被屏障后的 loads 看到。
 */
// Define MemoryBarrier() if available
// Windows on x86
#if defined(OS_WIN) && defined(COMPILER_MSVC) && defined(ARCH_CPU_X86_FAMILY)
// windows.h already provides a MemoryBarrier(void) macro
// http://msdn.microsoft.com/en-us/library/ms684208(v=vs.85).aspx
#define LEVELDB_HAVE_MEMORY_BARRIER

// Mac OS
#elif defined(__APPLE__)
inline void MemoryBarrier() {
  std::atomic_thread_fence(std::memory_order_seq_cst);
}
#define LEVELDB_HAVE_MEMORY_BARRIER

// Gcc on x86
#elif defined(ARCH_CPU_X86_FAMILY) && defined(__GNUC__)
inline void MemoryBarrier() {
  // See http://gcc.gnu.org/ml/gcc/2003-04/msg01180.html for a discussion on
  // this idiom. Also see http://en.wikipedia.org/wiki/Memory_ordering.
  __asm__ __volatile__("" : : : "memory");
}
#define LEVELDB_HAVE_MEMORY_BARRIER

// Sun Studio
#elif defined(ARCH_CPU_X86_FAMILY) && defined(__SUNPRO_CC)
inline void MemoryBarrier() {
  // See http://gcc.gnu.org/ml/gcc/2003-04/msg01180.html for a discussion on
  // this idiom. Also see http://en.wikipedia.org/wiki/Memory_ordering.
  asm volatile("" : : : "memory");
}
#define LEVELDB_HAVE_MEMORY_BARRIER

// ARM Linux
#elif defined(ARCH_CPU_ARM_FAMILY) && defined(__linux__)
typedef void (*LinuxKernelMemoryBarrierFunc)(void);
// The Linux ARM kernel provides a highly optimized device-specific memory
// barrier function at a fixed memory address that is mapped in every
// user-level process.
//
// This beats using CPU-specific instructions which are, on single-core
// devices, un-necessary and very costly (e.g. ARMv7-A "dmb" takes more
// than 180ns on a Cortex-A8 like the one on a Nexus One). Benchmarking
// shows that the extra function call cost is completely negligible on
// multi-core devices.
//
inline void MemoryBarrier() {
  (*(LinuxKernelMemoryBarrierFunc)0xffff0fa0)();
}
#define LEVELDB_HAVE_MEMORY_BARRIER

// ARM64
#elif defined(ARCH_CPU_ARM64_FAMILY)
inline void MemoryBarrier() {
  asm volatile("dmb sy" : : : "memory");
}
#define LEVELDB_HAVE_MEMORY_BARRIER

// PPC
#elif defined(ARCH_CPU_PPC_FAMILY) && defined(__GNUC__)
inline void MemoryBarrier() {
  // TODO for some powerpc expert: is there a cheaper suitable variant?
  // Perhaps by having separate barriers for acquire and release ops.
  asm volatile("sync" : : : "memory");
}
#define LEVELDB_HAVE_MEMORY_BARRIER

// MIPS
#elif defined(ARCH_CPU_MIPS_FAMILY) && defined(__GNUC__)
inline void MemoryBarrier() {
  __asm__ __volatile__("sync" : : : "memory");
}
#define LEVELDB_HAVE_MEMORY_BARRIER

#endif

// AtomicPointer built using platform-specific MemoryBarrier().
#if defined(LEVELDB_HAVE_MEMORY_BARRIER)
/**
 * 此处实现使用的是平台相关的内存屏障。
 * 注意，__asm__ __volatile__("" : : : "memory"); 除了作为内存屏障阻止编译器将屏障前后操作重排序，
 * 它还有一个副作用，就是将当前 cpu/core 上寄存器内容全部失效，具体为：
 * - 它会强制代码生成器将屏障插入位置之前在全部 cpu/core 上发生的全部 stores 操作从寄存器刷新到内存中（这可以确保其它 cpu/core 对内存的修改都生效）；
 * - 另外还有一个副作用就是令编译器假设内存已经发生改变，当前 cpu/core 寄存器里的内容都失效了（这可以强制屏障后用到的变量必须去内存读取最新值）。
 * （It forces the code generator to emit all the stores for data that's currently in registers
 *   but needs to go to variables. But it also (as documented) causes the compiler to assume
 *   that memory has changed since then, i.e., anything it loaded before is no longer valid.）
 */
class AtomicPointer {
 private:
  void* rep_; // 虽然看起来是个指针，但实际上它就是要互斥访问的值。
 public:
  AtomicPointer() { }
  explicit AtomicPointer(void* p) : rep_(p) {}
  inline void* NoBarrier_Load() const { return rep_; }
  inline void NoBarrier_Store(void* v) { rep_ = v; }
  inline void* Acquire_Load() const {
    void* result = rep_;
    // 确保对 result 的 store （即 result = rep_ 分为 load rep_->mov rep_, result ->store result 三个阶段）
    // 发生在 result 的 load 之前（即 return result 分为两个阶段 result load->赋值给外面变量），
    // 这样可以确保 Acquire_Load() 调用后返回的是内存中最新的值，实现了当前 cpu/core 看到其它 cpu/core 对该值的修改，
    // 从而确保了各个 cpu/core 缓存一致性。
    MemoryBarrier();
    return result;
  }
  inline void Release_Store(void* v) {
    // 确保对 v 的 load （传参有赋值行为，需要 load）发生在 rep_ 的 store 之前，
    // 这样可以确保把最新的 v 存储到 rep_ 所在的内存位置，
    // 实现了修改可以被其它 cpu/core 看到,
    // 从而确保了各个 cpu/core 缓存一致性。
    MemoryBarrier();
    rep_ = v;
  }
};

// AtomicPointer based on C++11 <atomic>.
#else
/**
 * 此处实现基于 C++11 <atomic>.
 */
class AtomicPointer {
 private:
  std::atomic<void*> rep_;
 public:
  AtomicPointer() { }
  explicit AtomicPointer(void* v) : rep_(v) { }
  inline void* Acquire_Load() const {
    return rep_.load(std::memory_order_acquire); // 确保操作的原子性和对其它 cpu/core 的可见性
  }
  inline void Release_Store(void* v) {
    rep_.store(v, std::memory_order_release); // 确保操作的原子性和对其它 cpu/core 的可见性
  }
  inline void* NoBarrier_Load() const {
    return rep_.load(std::memory_order_relaxed); // 确保操作的原子性，但不确保对其它 cpu/core 的可见性
  }
  inline void NoBarrier_Store(void* v) {
    rep_.store(v, std::memory_order_relaxed); // 确保操作的原子性，但不确保对其它 cpu/core 的可见性
  }
};

#endif

#undef LEVELDB_HAVE_MEMORY_BARRIER
#undef ARCH_CPU_X86_FAMILY
#undef ARCH_CPU_ARM_FAMILY
#undef ARCH_CPU_ARM64_FAMILY
#undef ARCH_CPU_PPC_FAMILY

}  // namespace port
}  // namespace leveldb

#endif  // PORT_ATOMIC_POINTER_H_

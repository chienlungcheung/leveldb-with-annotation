// Copyright (c) 2018 The LevelDB Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file. See the AUTHORS file for names of contributors.

#ifndef STORAGE_LEVELDB_UTIL_NO_DESTRUCTOR_H_
#define STORAGE_LEVELDB_UTIL_NO_DESTRUCTOR_H_

#include <type_traits>
#include <utility>

namespace leveldb {

// Wraps an instance whose destructor is never called.
//
// This is intended for use with function-level static variables.
//
// 该类用来包装一个实例，被包装的实例的析构函数永远都不会被调用。
// 这个类适用于函数级别的静态变量。
template<typename InstanceType>
class NoDestructor {
 public:
  // 构造函数使用了定位 new，即在指定的内存位置（此处是成员 instance_storage_ 的地址）构造 InstanceType 类型的对象
  template <typename... ConstructorArgTypes>
  explicit NoDestructor(ConstructorArgTypes&&... constructor_args) {
    static_assert(sizeof(instance_storage_) >= sizeof(InstanceType),
                  "instance_storage_ is not large enough to hold the instance");
    static_assert(
        alignof(decltype(instance_storage_)) >= alignof(InstanceType),
        "instance_storage_ does not meet the instance's alignment requirement");
    // std::forward 实现参数的完美转发
    new (&instance_storage_) InstanceType(
        std::forward<ConstructorArgTypes>(constructor_args)...);
  }

  ~NoDestructor() = default;

  NoDestructor(const NoDestructor&) = delete;
  NoDestructor& operator=(const NoDestructor&) = delete;

  InstanceType* get() {
    return reinterpret_cast<InstanceType*>(&instance_storage_);
  }

 private:
  // 每个对象类型都具有被称为对齐要求 (alignment requirement) 的性质，它是一个整数（类型为 std::size_t，总为 2 的幂），
  // 表示这个类型的对象所能分配的相邻地址之间的字节数。
  //
  // 可以使用 alignof 或 std::alignment_of 获得类型的对齐要求。
  //
  // 可以使用指针对齐函数 std::align 来获取某个缓冲区中经过适当对齐的指针，还可以使用 std::aligned_storage 来获取经过适当对齐的存储区。
  //
  // 对象类型会强制该类型的所有对象实行这个类型的对齐要求；可以使用 alignas 来要求更严格的对齐（更大的对齐要求）。
  // 为了使类中的所有非静态成员都符合对齐要求，会在一些成员后面插入一些填充。
  //
  // 关于对齐的描述具体见 https://en.cppreference.com/w/cpp/language/object#Alignment
  //
  // 内存对齐的主要作用是（http://www.cppblog.com/snailcong/archive/2009/03/16/76705.html）：
  // 1、  平台原因(移植原因)：不是所有的硬件平台都能访问任意地址上的任意数据的；某些硬件平台只能在某些地址处取某些特定类型的数据，否则抛出硬件异常。
  // 2、  性能原因：经过内存对齐后，CPU的内存访问速度大大提升。
  //
  // 所以下面的 instance_storage_ 变量的存储空间满足：字节个数最小为 sizeof(InstanceType)，并且按照 alignof(InstanceType) 进行对齐。
  typename
      std::aligned_storage<sizeof(InstanceType), alignof(InstanceType)>::type
      instance_storage_;
};

}  // namespace leveldb

#endif  // STORAGE_LEVELDB_UTIL_NO_DESTRUCTOR_H_

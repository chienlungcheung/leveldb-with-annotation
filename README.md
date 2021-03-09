**LevelDB is a fast key-value storage library written at Google that provides an ordered mapping from string keys to string values.**

**LevelDB 是一个快速的键值存储程序库, 提供了字符串形式的 keys 到字符串形式的 values 的有序映射.**

[![Build Status](https://travis-ci.org/google/leveldb.svg?branch=master)](https://travis-ci.org/google/leveldb)

Authors: Sanjay Ghemawat (sanjay@google.com) and Jeff Dean (jeff@google.com)

# Features

  * Keys and values are arbitrary byte arrays.
  * Data is stored sorted by key.
  * Callers can provide a custom comparison function to override the sort order.
  * The basic operations are `Put(key,value)`, `Get(key)`, `Delete(key)`.
  * Multiple changes can be made in one atomic batch.
  * Users can create a transient snapshot to get a consistent view of data.
  * Forward and backward iteration is supported over the data.
  * Data is automatically compressed using the [Snappy compression library](http://google.github.io/snappy/).
  * External activity (file system operations etc.) is relayed through a virtual interface so users can customize the operating system interactions.

# 特性

  * keys 和 values 都可以是随机的字节数组. 
  * 数据被按照 key 的顺序进行存储. 
  * 调用者可以提供一个定制的比较函数来覆盖默认的比较器. 
  * 基础操作有 `Put(key,value)`, `Get(key)`, `Delete(key)`. 
  * 多个更改可以在一个原子批处理中一起生效. 
  * 用户可以创建一个瞬时快照来获取数据的一致性视图. 
  * 支持针对数据的前向和后向遍历. 
  * 数据通过 Snappy 压缩程序库自动压缩. 
  * 外部行为(文件系统操作等)通过一个虚拟接口被中转, 所以用户可以定制文件系统交互行为. 

# Documentation

  [LevelDB library documentation](https://github.com/google/leveldb/blob/master/doc/index.md) is online and bundled with the source code.

# Limitations

  * This is not a SQL database.  It does not have a relational data model, it does not support SQL queries, and it has no support for indexes.
  * Only a single process (possibly multi-threaded) can access a particular database at a time.
  * There is no client-server support builtin to the library.  An application that needs such support will have to wrap their own server around the library.

# 局限性

  * LevelDB 不是 SQL 数据库. 它没有关系数据模型, 不支持 SQL 查询, 也不支持索引. 
  * 同时只能有一个进程(可能是多线程的进程)访问一个特定的数据库. 
  * 该程序库没有内置的客户端-服务端支持. 需要该支持的用户需要自己围绕该程序库进行封装. 

# Building

This project supports [CMake](https://cmake.org/) out of the box.

Quick start:

```bash
mkdir -p build && cd build
cmake -DCMAKE_BUILD_TYPE=Release .. && cmake --build .
sudo make install
```

Please see the CMake documentation and `CMakeLists.txt` for more advanced usage.

```bash
$ sudo make install/local
Installing only the local directory...
-- Install configuration: ""
-- Installing: /usr/local/lib/libleveldb.a
-- Installing: /usr/local/include/leveldb/c.h
-- Installing: /usr/local/include/leveldb/cache.h
-- Installing: /usr/local/include/leveldb/comparator.h
-- Installing: /usr/local/include/leveldb/db.h
-- Installing: /usr/local/include/leveldb/dumpfile.h
-- Installing: /usr/local/include/leveldb/env.h
-- Installing: /usr/local/include/leveldb/export.h
-- Installing: /usr/local/include/leveldb/filter_policy.h
-- Installing: /usr/local/include/leveldb/iterator.h
-- Installing: /usr/local/include/leveldb/options.h
-- Installing: /usr/local/include/leveldb/slice.h
-- Installing: /usr/local/include/leveldb/status.h
-- Installing: /usr/local/include/leveldb/table_builder.h
-- Installing: /usr/local/include/leveldb/table.h
-- Installing: /usr/local/include/leveldb/write_batch.h
-- Installing: /usr/local/lib/cmake/leveldb/leveldbTargets.cmake
-- Installing: /usr/local/lib/cmake/leveldb/leveldbTargets-noconfig.cmake
-- Installing: /usr/local/lib/cmake/leveldb/leveldbConfig.cmake
-- Installing: /usr/local/lib/cmake/leveldb/leveldbConfigVersion.cmake
```

```bash
$ sudo make install
Install the project...
-- Install configuration: ""
-- Installing: /usr/local/lib/libleveldb.a
-- Installing: /usr/local/include/leveldb/c.h
-- Installing: /usr/local/include/leveldb/cache.h
-- Installing: /usr/local/include/leveldb/comparator.h
-- Installing: /usr/local/include/leveldb/db.h
-- Installing: /usr/local/include/leveldb/dumpfile.h
-- Installing: /usr/local/include/leveldb/env.h
-- Installing: /usr/local/include/leveldb/export.h
-- Installing: /usr/local/include/leveldb/filter_policy.h
-- Installing: /usr/local/include/leveldb/iterator.h
-- Installing: /usr/local/include/leveldb/options.h
-- Installing: /usr/local/include/leveldb/slice.h
-- Installing: /usr/local/include/leveldb/status.h
-- Installing: /usr/local/include/leveldb/table_builder.h
-- Installing: /usr/local/include/leveldb/table.h
-- Installing: /usr/local/include/leveldb/write_batch.h
-- Installing: /usr/local/lib/cmake/leveldb/leveldbTargets.cmake
-- Installing: /usr/local/lib/cmake/leveldb/leveldbTargets-noconfig.cmake
-- Installing: /usr/local/lib/cmake/leveldb/leveldbConfig.cmake
-- Installing: /usr/local/lib/cmake/leveldb/leveldbConfigVersion.cmake
```
# 构建

该工程开箱即支持 CMake. 

快速开始: 
```bash
mkdir -p build && cd build
cmake -DCMAKE_BUILD_TYPE=Release .. && cmake --build .
sudo make install
```

更多高级用法请请参照 CMake 文档和 `CMakeLists.txt`. 

# Contributing to the leveldb Project

The leveldb project welcomes contributions. leveldb's primary goal is to be
a reliable and fast key/value store. Changes that are in line with the
features/limitations outlined above, and meet the requirements below,
will be considered.

Contribution requirements:

1. **POSIX only**. We _generally_ will only accept changes that are both
   compiled, and tested on a POSIX platform - usually Linux. Very small
   changes will sometimes be accepted, but consider that more of an
   exception than the rule.

2. **Stable API**. We strive very hard to maintain a stable API. Changes that
   require changes for projects using leveldb _might_ be rejected without
   sufficient benefit to the project.

3. **Tests**: All changes must be accompanied by a new (or changed) test, or
   a sufficient explanation as to why a new (or changed) test is not required.

## Submitting a Pull Request

Before any pull request will be accepted the author must first sign a
Contributor License Agreement (CLA) at https://cla.developers.google.com/.

In order to keep the commit timeline linear
[squash](https://git-scm.com/book/en/v2/Git-Tools-Rewriting-History#Squashing-Commits)
your changes down to a single commit and [rebase](https://git-scm.com/docs/git-rebase)
on google/leveldb/master. This keeps the commit timeline linear and more easily sync'ed
with the internal repository at Google. More information at GitHub's
[About Git rebase](https://help.github.com/articles/about-git-rebase/) page.

# Performance

Here is a performance report (with explanations) from the run of the
included db_bench program.  The results are somewhat noisy, but should
be enough to get a ballpark performance estimate.

# 性能

下面是通过运行 db_bench 程序得出的性能测试报告. 我们使用的是一个包含一百万数据项的数据库, 
其中 key 是 16 字节, value 是 100 字节, value 压缩后大约是原来的一半. 

## Setup

We use a database with a million entries.  Each entry has a 16 byte
key, and a 100 byte value.  Values used by the benchmark compress to
about half their original size.

    LevelDB:    version 1.1
    Date:       Sun May  1 12:11:26 2011
    CPU:        4 x Intel(R) Core(TM)2 Quad CPU    Q6600  @ 2.40GHz
    CPUCache:   4096 KB
    Keys:       16 bytes each
    Values:     100 bytes each (50 bytes after compression)
    Entries:    1000000
    Raw Size:   110.6 MB (estimated)
    File Size:  62.9 MB (estimated)

## Write performance

The "fill" benchmarks create a brand new database, in either
sequential, or random order.  The "fillsync" benchmark flushes data
from the operating system to the disk after every operation; the other
write operations leave the data sitting in the operating system buffer
cache for a while.  The "overwrite" benchmark does random writes that
update existing keys in the database.

    fillseq      :       1.765 micros/op;   62.7 MB/s
    fillsync     :     268.409 micros/op;    0.4 MB/s (10000 ops)
    fillrandom   :       2.460 micros/op;   45.0 MB/s
    overwrite    :       2.380 micros/op;   46.5 MB/s

Each "op" above corresponds to a write of a single key/value pair.
I.e., a random write benchmark goes at approximately 400,000 writes per second.

Each "fillsync" operation costs much less (0.3 millisecond)
than a disk seek (typically 10 milliseconds).  We suspect that this is
because the hard disk itself is buffering the update in its memory and
responding before the data has been written to the platter.  This may
or may not be safe based on whether or not the hard disk has enough
power to save its memory in the event of a power failure.

## 写性能

"fill" 基准测试创建了一个全新的数据库, 以顺序(下面 seq 结尾者)或者随机(下面 random 结尾者)方式. 
"fillsync" 基准测试每次写操作都将数据从操作系统冲刷到磁盘; 其它的操作会将数据保存在系统中一段时间. 
"overwrite" 基准测试做随机写, 这些操作会更新数据库中已有的键. 

    fillseq      :       1.765 micros/op;   62.7 MB/s
    fillsync     :     268.409 micros/op;    0.4 MB/s (10000 ops)
    fillrandom   :       2.460 micros/op;   45.0 MB/s
    overwrite    :       2.380 micros/op;   46.5 MB/s

上述每个 "op" 对应一个 key/value 对的写操作. 也就是说, 一个随机写基准测试每秒大约进行四十万次写操作(1,000,000/2.46). 

每个 "fillsync" 操作时间消耗(大约 0.3 毫秒)少于一次磁盘寻道(大约 10 毫秒). 我们怀疑这是因为磁盘本身将更新操作缓存到了内存, 
并且在数据真正落盘前返回响应. 该方式是否安全取决于断电后磁盘是否有备用电力将数据落盘. 

## Read performance

We list the performance of reading sequentially in both the forward
and reverse direction, and also the performance of a random lookup.
Note that the database created by the benchmark is quite small.
Therefore the report characterizes the performance of leveldb when the
working set fits in memory.  The cost of reading a piece of data that
is not present in the operating system buffer cache will be dominated
by the one or two disk seeks needed to fetch the data from disk.
Write performance will be mostly unaffected by whether or not the
working set fits in memory.

    readrandom  : 16.677 micros/op;  (approximately 60,000 reads per second)
    readseq     :  0.476 micros/op;  232.3 MB/s
    readreverse :  0.724 micros/op;  152.9 MB/s

LevelDB compacts its underlying storage data in the background to
improve read performance.  The results listed above were done
immediately after a lot of random writes.  The results after
compactions (which are usually triggered automatically) are better.

    readrandom  : 11.602 micros/op;  (approximately 85,000 reads per second)
    readseq     :  0.423 micros/op;  261.8 MB/s
    readreverse :  0.663 micros/op;  166.9 MB/s

Some of the high cost of reads comes from repeated decompression of blocks
read from disk.  If we supply enough cache to the leveldb so it can hold the
uncompressed blocks in memory, the read performance improves again:

    readrandom  : 9.775 micros/op;  (approximately 100,000 reads per second before compaction)
    readrandom  : 5.215 micros/op;  (approximately 190,000 reads per second after compaction)

## 读性能

我们分别给出正向顺序读、反向顺序读的性能以及随机查询的性能指标. 注意, 基准测试创建的数据库很小. 
因此该性能报告描述的是 leveldb 的全部数据集能放入到内存的场景. 如果数据不在操作系统缓存中, 
读取一点数据的性能消耗主要在于一到两次的磁盘寻道. 写性能基本不会受数据集是否能放入内存的影响. 

    readrandom  : 16.677 micros/op;  (approximately 60,000 reads per second)
    readseq     :  0.476 micros/op;  232.3 MB/s
    readreverse :  0.724 micros/op;  152.9 MB/s

LevelDB 会在后台压实底层的数据来改善读性能. 上面列出的结果是在经过一系列随机写操作后得出的. 如果
经过压实(通常是自动触发), 那么上述指标会更好. 

    readrandom  : 11.602 micros/op;  (approximately 85,000 reads per second)
    readseq     :  0.423 micros/op;  261.8 MB/s
    readreverse :  0.663 micros/op;  166.9 MB/s
    
读操作消耗高的地方有一些来自重复解压从磁盘读取的数据块. 如果我们能提供足够的缓存给 leveldb 来将
解压后的数据保存在内存中, 读性能会进一步改善: 
   
    readrandom  : 9.775 micros/op;  (approximately 100,000 reads per second before compaction)
    readrandom  : 5.215 micros/op;  (approximately 190,000 reads per second after compaction) 

## Repository contents

See [doc/index.md](doc/index.md) for more explanation. See
[doc/impl.md](doc/impl.md) for a brief overview of the implementation.

更详细介绍请参见 [doc/index.md](doc/index.md), 如果想了解 leveldb 的实现请参见 [doc/impl.md](doc/impl.md). 
 
The public interface is in include/*.h.  Callers should not include or
rely on the details of any other header files in this package.  Those
internal APIs may be changed without warning.

LevelDB 对外的接口都包含在 include/*.h 中. 除了该目录下的文件, 用户不应该依赖其它目录下任何文件. 

Guide to header files:

头文件介绍: 

* **include/db.h**: Main interface to the DB: Start here 主要的接口在这, 使用 leveldb 从这里开始. 

* **include/options.h**: Control over the behavior of an entire database,
and also control over the behavior of individual reads and writes. 使用 leveldb 过程中与读写有关的控制参数. 

* **include/comparator.h**: Abstraction for user-specified comparison function.
If you want just bytewise comparison of keys, you can use the default
comparator, but clients can write their own comparator implementations if they
want custom ordering (e.g. to handle different character encodings, etc.) 比较函数的抽象, 如果你想用逐字节比较 keys 那么可以直接使用默认的比较器. 如果你想定制排序逻辑可以定制自己的比较函数. 

* **include/iterator.h**: Interface for iterating over data. You can get
an iterator from a DB object. 迭代数据的接口. 你可以从一个 DB 对象获取到一个迭代器. 

* **include/write_batch.h**: Interface for atomically applying multiple
updates to a database. 原子地应用多个更新到一个数据库. 

* **include/slice.h**: A simple module for maintaining a pointer and a
length into some other byte array. 类似 string, 维护着指向字节数组的一个指针和响应长度. 

* **include/status.h**: Status is returned from many of the public interfaces
and is used to report success and various kinds of errors. 许多公共接口都会返回 Status, 用于报告成功以及多种错误. 

* **include/env.h**:
Abstraction of the OS environment.  A posix implementation of this interface is
in util/env_posix.cc 操作系统环境的抽象. 在 util/env_posix.cc 中有一个该接口的 posix 实现. 

* **include/table.h, include/table_builder.h**: Lower-level modules that most
clients probably won't use directly 底层的模块, 大多数客户端可能不会直接用到. 

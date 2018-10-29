leveldb
=======

_Jeff Dean, Sanjay Ghemawat_

The leveldb library provides a persistent key value store. Keys and values are
arbitrary byte arrays.  The keys are ordered within the key value store
according to a user-specified comparator function.

leveldb 提供了一个持久性的 key/value 存储。keys 和 values 都是随机字节数组，并且根据用户指定的 coparator function 对 keys 进行排序。

## Opening A Database

A leveldb database has a name which corresponds to a file system directory. All
of the contents of database are stored in this directory. The following example
shows how to open a database, creating it if necessary:

leveldb 数据库都有一个名字，该名字对应了文件系统上一个目录，而且该数据库内容全都存在该目录下。下面的例子显示了如何打开一个数据库以及在必要情况下创建之。

```c++
#include <cassert>
#include "leveldb/db.h"

leveldb::DB* db;
leveldb::Options options;
options.create_if_missing = true;
leveldb::Status status = leveldb::DB::Open(options, "/tmp/testdb", &db);
assert(status.ok());
...
```

If you want to raise an error if the database already exists, add the following
line before the `leveldb::DB::Open` call:

如果你想在数据库已存在的时候引发一个异常，将下面这行加到 `leveldb::DB::Open` 调用之前：

```c++
options.error_if_exists = true;
```

## Status

You may have noticed the `leveldb::Status` type above. Values of this type are
returned by most functions in leveldb that may encounter an error. You can check
if such a result is ok, and also print an associated error message:

你可能注意到上面的 `leveldb::Status` 类型了。该类型的值会被 leveldb 中大部分函数在遭遇错误的时候返回。你可以检查它释放为 ok，然后打印关联的错误信息即可：

```c++
leveldb::Status s = ...;
if (!s.ok()) cerr << s.ToString() << endl;
```

## Closing A Database

When you are done with a database, just delete the database object. Example:

当数据库不再使用的时候，像下面这样直接删除数据库对象就可以了：

```c++
... open the db as described above ...
... do something with db ...
delete db;
```

## Reads And Writes

The database provides Put, Delete, and Get methods to modify/query the database.
For example, the following code moves the value stored under key1 to key2.

数据库提供了 Put、Delete 以及 Get 方法来修改、查询数据库。下面的代码展示了将 key1 对应的 value 移动（先拷贝后删除）到 key2 下。

```c++
std::string value;
leveldb::Status s = db->Get(leveldb::ReadOptions(), key1, &value);
if (s.ok()) s = db->Put(leveldb::WriteOptions(), key2, value);
if (s.ok()) s = db->Delete(leveldb::WriteOptions(), key1);
```

## Atomic Updates

Note that if the process dies after the Put of key2 but before the delete of
key1, the same value may be left stored under multiple keys. Such problems can
be avoided by using the `WriteBatch` class to atomically apply a set of updates:

注意，上一小节中如果进程在 Put 了 key2 之后但是删除 key1 之前挂了，那么同样的 value 就出现在了多个 keys 之下。该问题可以通过使用 `WriteBatch` 类原子地应用一组操作来避免。

```c++
#include "leveldb/write_batch.h"
...
std::string value;
leveldb::Status s = db->Get(leveldb::ReadOptions(), key1, &value);
if (s.ok()) {
  leveldb::WriteBatch batch;
  batch.Delete(key1);
  batch.Put(key2, value);
  s = db->Write(leveldb::WriteOptions(), &batch);
}
```

The `WriteBatch` holds a sequence of edits to be made to the database, and these
edits within the batch are applied in order. Note that we called Delete before
Put so that if key1 is identical to key2, we do not end up erroneously dropping
the value entirely.

Apart from its atomicity benefits, `WriteBatch` may also be used to speed up
bulk updates by placing lots of individual mutations into the same batch.

`WriteBatch` 保存着一系列将被应用到数据库的编辑操作，这些操作会按照添加的顺序依次被执行。注意，我们先执行 Delete 后执行 Put，这样如果 key1 和 key2 一样的情况下我们也不会错误地丢失数据。

除了原子性，`WriteBatch` 也能加速更新过程，因为可以把一大批独立的操作添加到同一个 batch 中然后一次性执行。

## Synchronous Writes

By default, each write to leveldb is asynchronous: it returns after pushing the
write from the process into the operating system. The transfer from operating
system memory to the underlying persistent storage happens asynchronously. The
sync flag can be turned on for a particular write to make the write operation
not return until the data being written has been pushed all the way to
persistent storage. (On Posix systems, this is implemented by calling either
`fsync(...)` or `fdatasync(...)` or `msync(..., MS_SYNC)` before the write
operation returns.)

默认 leveldb 每个写操作都是异步的：进程把要写的内容 push 给操作系统后立马返回。从操作系统内存到底层持久性存储的迁移异步地发生。当然，也可以把某个写操作的 sync 标识打开，以等到数据真正被记录到持久化存储再让写操作返回。（在 Posix 系统上，这是通过在写操作返回前调用 `fsync(...)` 或 `fdatasync(...)` 或 `msync(..., MS_SYNC)` 来实现的。）

```c++
leveldb::WriteOptions write_options;
write_options.sync = true;
db->Put(write_options, ...);
```

Asynchronous writes are often more than a thousand times as fast as synchronous
writes. The downside of asynchronous writes is that a crash of the machine may
cause the last few updates to be lost. Note that a crash of just the writing
process (i.e., not a reboot) will not cause any loss since even when sync is
false, an update is pushed from the process memory into the operating system
before it is considered done.

异步写通常比同步写快一千倍。异步写的缺点是，一旦机器崩溃可能会导致最后几个更新操作丢失。注意，仅仅是写进程崩溃（而非机器重启）将不会引起任何更新操作丢失，因为哪怕 sync 标识为 false，写数据也已经从进程内存 push 到了操作系统。

Asynchronous writes can often be used safely. For example, when loading a large
amount of data into the database you can handle lost updates by restarting the
bulk load after a crash. A hybrid scheme is also possible where every Nth write
is synchronous, and in the event of a crash, the bulk load is restarted just
after the last synchronous write finished by the previous run. (The synchronous
write can update a marker that describes where to restart on a crash.)

异步写总是可以安全使用。比如你要将大量的数据写入数据库，如果丢失了最后几个更新操作，你可以重启整个写过程。如果数据量非常大，一个优化点是，每进行 N 个异步写操作则进行一次同步地写操作，如果期间发生了崩溃，重启自从上一个成功的同步写操作以来的更新操作即可。（同步的写操作能够更新一个标识，该标识描述了应该从哪个地方开始重启更新操作。）

`WriteBatch` provides an alternative to asynchronous writes. Multiple updates
may be placed in the same WriteBatch and applied together using a synchronous
write (i.e., `write_options.sync` is set to true). The extra cost of the
synchronous write will be amortized across all of the writes in the batch.

`WriteBatch` 可以作为异步写操作的替代品。多个更新操作可以放到同一个 WriteBatch 中然后通过一次同步写（即 `write_options.sync` 置为 false）一起应用。

## Concurrency

A database may only be opened by one process at a time. The leveldb
implementation acquires a lock from the operating system to prevent misuse.
Within a single process, the same `leveldb::DB` object may be safely shared by
multiple concurrent threads. I.e., different threads may write into or fetch
iterators or call Get on the same database without any external synchronization
(the leveldb implementation will automatically do the required synchronization).
However other objects (like Iterator and `WriteBatch`) may require external
synchronization. If two threads share such an object, they must protect access
to it using their own locking protocol. More details are available in the public
header files.

## Iteration

The following example demonstrates how to print all key,value pairs in a
database.

```c++
leveldb::Iterator* it = db->NewIterator(leveldb::ReadOptions());
for (it->SeekToFirst(); it->Valid(); it->Next()) {
  cout << it->key().ToString() << ": "  << it->value().ToString() << endl;
}
assert(it->status().ok());  // Check for any errors found during the scan
delete it;
```

The following variation shows how to process just the keys in the range
[start,limit):

```c++
for (it->Seek(start);
   it->Valid() && it->key().ToString() < limit;
   it->Next()) {
  ...
}
```

You can also process entries in reverse order. (Caveat: reverse iteration may be
somewhat slower than forward iteration.)

```c++
for (it->SeekToLast(); it->Valid(); it->Prev()) {
  ...
}
```

## Snapshots

Snapshots provide consistent read-only views over the entire state of the
key-value store.  `ReadOptions::snapshot` may be non-NULL to indicate that a
read should operate on a particular version of the DB state. If
`ReadOptions::snapshot` is NULL, the read will operate on an implicit snapshot
of the current state.

Snapshots are created by the `DB::GetSnapshot()` method:

```c++
leveldb::ReadOptions options;
options.snapshot = db->GetSnapshot();
... apply some updates to db ...
leveldb::Iterator* iter = db->NewIterator(options);
... read using iter to view the state when the snapshot was created ...
delete iter;
db->ReleaseSnapshot(options.snapshot);
```

Note that when a snapshot is no longer needed, it should be released using the
`DB::ReleaseSnapshot` interface. This allows the implementation to get rid of
state that was being maintained just to support reading as of that snapshot.

## Slice

The return value of the `it->key()` and `it->value()` calls above are instances
of the `leveldb::Slice` type. Slice is a simple structure that contains a length
and a pointer to an external byte array. Returning a Slice is a cheaper
alternative to returning a `std::string` since we do not need to copy
potentially large keys and values. In addition, leveldb methods do not return
null-terminated C-style strings since leveldb keys and values are allowed to
contain `'\0'` bytes.

C++ strings and null-terminated C-style strings can be easily converted to a
Slice:

```c++
leveldb::Slice s1 = "hello";

std::string str("world");
leveldb::Slice s2 = str;
```

A Slice can be easily converted back to a C++ string:

```c++
std::string str = s1.ToString();
assert(str == std::string("hello"));
```

Be careful when using Slices since it is up to the caller to ensure that the
external byte array into which the Slice points remains live while the Slice is
in use. For example, the following is buggy:

```c++
leveldb::Slice slice;
if (...) {
  std::string str = ...;
  slice = str;
}
Use(slice);
```

When the if statement goes out of scope, str will be destroyed and the backing
storage for slice will disappear.

## Comparators

The preceding examples used the default ordering function for key, which orders
bytes lexicographically. You can however supply a custom comparator when opening
a database.  For example, suppose each database key consists of two numbers and
we should sort by the first number, breaking ties by the second number. First,
define a proper subclass of `leveldb::Comparator` that expresses these rules:

```c++
class TwoPartComparator : public leveldb::Comparator {
 public:
  // Three-way comparison function:
  //   if a < b: negative result
  //   if a > b: positive result
  //   else: zero result
  int Compare(const leveldb::Slice& a, const leveldb::Slice& b) const {
    int a1, a2, b1, b2;
    ParseKey(a, &a1, &a2);
    ParseKey(b, &b1, &b2);
    if (a1 < b1) return -1;
    if (a1 > b1) return +1;
    if (a2 < b2) return -1;
    if (a2 > b2) return +1;
    return 0;
  }

  // Ignore the following methods for now:
  const char* Name() const { return "TwoPartComparator"; }
  void FindShortestSeparator(std::string*, const leveldb::Slice&) const {}
  void FindShortSuccessor(std::string*) const {}
};
```

Now create a database using this custom comparator:

```c++
TwoPartComparator cmp;
leveldb::DB* db;
leveldb::Options options;
options.create_if_missing = true;
options.comparator = &cmp;
leveldb::Status status = leveldb::DB::Open(options, "/tmp/testdb", &db);
...
```

### Backwards compatibility

The result of the comparator's Name method is attached to the database when it
is created, and is checked on every subsequent database open. If the name
changes, the `leveldb::DB::Open` call will fail. Therefore, change the name if
and only if the new key format and comparison function are incompatible with
existing databases, and it is ok to discard the contents of all existing
databases.

You can however still gradually evolve your key format over time with a little
bit of pre-planning. For example, you could store a version number at the end of
each key (one byte should suffice for most uses). When you wish to switch to a
new key format (e.g., adding an optional third part to the keys processed by
`TwoPartComparator`), (a) keep the same comparator name (b) increment the
version number for new keys (c) change the comparator function so it uses the
version numbers found in the keys to decide how to interpret them.

## Performance

Performance can be tuned by changing the default values of the types defined in
`include/leveldb/options.h`.

### Block size

leveldb groups adjacent keys together into the same block and such a block is
the unit of transfer to and from persistent storage. The default block size is
approximately 4096 uncompressed bytes.  Applications that mostly do bulk scans
over the contents of the database may wish to increase this size. Applications
that do a lot of point reads of small values may wish to switch to a smaller
block size if performance measurements indicate an improvement. There isn't much
benefit in using blocks smaller than one kilobyte, or larger than a few
megabytes. Also note that compression will be more effective with larger block
sizes.

leveldb 把相邻的 keys 组织在同一个 block 中，而且 block 是 leveldb 把数据从内存到转移到持久化存储和从持久化存储转移到内存的基本单位。默认的 block 大约为 4KB，压缩前。一次性处理大块数据的应用可能希望把整个值调大，每次随机读取某个数据的应用可能希望这个值小一点，这样可能性能会更高一些。但是，没有证据表名该值小于 1KB 或者大于几个 MB 的时候性能会表现更好。同时要注意，针对大的 block size，数据压缩后效率更高。

### Compression

Each block is individually compressed before being written to persistent
storage. Compression is on by default since the default compression method is
very fast, and is automatically disabled for uncompressible data. In rare cases,
applications may want to disable compression entirely, but should only do so if
benchmarks show a performance improvement:

```c++
leveldb::Options options;
options.compression = leveldb::kNoCompression;
... leveldb::DB::Open(options, name, ...) ....
```

### Cache

The contents of the database are stored in a set of files in the filesystem and
each file stores a sequence of compressed blocks. If options.block_cache is
non-NULL, it is used to cache frequently used uncompressed block contents.

数据库的内容存储在文件系统的一组文件里，每个文件保存着一系列压缩后的 blocks。如果 options.block_cache 不为空，它就会被用于缓存频繁被使用的 block 内容（已解压缩）。

```c++
#include "leveldb/cache.h"

leveldb::Options options;
options.block_cache = leveldb::NewLRUCache(100 * 1048576);  // 100MB cache
leveldb::DB* db;
leveldb::DB::Open(options, name, &db);
... use the db ...
delete db
delete options.block_cache;
```

Note that the cache holds uncompressed data, and therefore it should be sized
according to application level data sizes, without any reduction from
compression. (Caching of compressed blocks is left to the operating system
buffer cache, or any custom Env implementation provided by the client.)

When performing a bulk read, the application may wish to disable caching so that
the data processed by the bulk read does not end up displacing most of the
cached contents. A per-iterator option can be used to achieve this:

注意 cache 保存的是未压缩的数据，因此应该根据应用程序所需的数据大小来设置这个值。（缓存压缩数据的活儿交给操作系统的 buffer cache 或者用户提供的定制的 Env 实现去干。）

当执行一个大块数据读操作时，应用程序可能想要取消缓存功能，这样通过大块读进来数据就不会替换 cache 中当前大部分数据。我们可以为它提供一个单独的 iterator 来达到目的：

```c++
leveldb::ReadOptions options;
options.fill_cache = false;
leveldb::Iterator* it = db->NewIterator(options);
for (it->SeekToFirst(); it->Valid(); it->Next()) {
  ...
}
```

### Key Layout

Note that the unit of disk transfer and caching is a block. Adjacent keys
(according to the database sort order) will usually be placed in the same block.
Therefore the application can improve its performance by placing keys that are
accessed together near each other and placing infrequently used keys in a
separate region of the key space.

For example, suppose we are implementing a simple file system on top of leveldb.
The types of entries we might wish to store are:

    filename -> permission-bits, length, list of file_block_ids
    file_block_id -> data

We might want to prefix filename keys with one letter (say '/') and the
`file_block_id` keys with a different letter (say '0') so that scans over just
the metadata do not force us to fetch and cache bulky file contents.

注意，磁盘传输的单位以及磁盘缓存的单位时一个 block。相邻的 keys（已排序）将总是在同一个 block 中。因此应用程序可以通过把总是差不多需要一起访问的 keys 和经常使用的 keys 放到一个独立的键空间区域来提升性能。

举个例子，假设我们正基于 leveldb 实现一个简单的文件系统。我们打算存储到这个文件系统的数据项类型如下：

    filename -> permission-bits, length, list of file_block_ids
    file_block_id -> data

我们可以把上面表示 filename 的 keys 增加一个字符前缀，比如 '/'，然后 `file_block_id` keys 增加一个不同的前缀，比如 '0'，这样这些 keys 就具有了各自独立的键空间区域，扫描元数据的时候我们就不用读取和缓存大块文件数据了。

### Filters

Because of the way leveldb data is organized on disk, a single `Get()` call may
involve multiple reads from disk. The optional FilterPolicy mechanism can be
used to reduce the number of disk reads substantially.

鉴于 leveldb 数据的组织形式，一次 `Get()` 调用可能设计多次磁盘读操作。可选的 FilterPolicy 机制可以大幅减少磁盘读次数。

```c++
leveldb::Options options;
options.filter_policy = NewBloomFilterPolicy(10);
leveldb::DB* db;
leveldb::DB::Open(options, "/tmp/testdb", &db);
... use the database ...
delete db;
delete options.filter_policy;
```

The preceding code associates a Bloom filter based filtering policy with the
database.  Bloom filter based filtering relies on keeping some number of bits of
data in memory per key (in this case 10 bits per key since that is the argument
we passed to `NewBloomFilterPolicy`). This filter will reduce the number of
unnecessary disk reads needed for Get() calls by a factor of approximately
a 100. Increasing the bits per key will lead to a larger reduction at the cost
of more memory usage. We recommend that applications whose working set does not
fit in memory and that do a lot of random reads set a filter policy.

If you are using a custom comparator, you should ensure that the filter policy
you are using is compatible with your comparator. For example, consider a
comparator that ignores trailing spaces when comparing keys.
`NewBloomFilterPolicy` must not be used with such a comparator. Instead, the
application should provide a custom filter policy that also ignores trailing
spaces. For example:

```c++
class CustomFilterPolicy : public leveldb::FilterPolicy {
 private:
  FilterPolicy* builtin_policy_;

 public:
  CustomFilterPolicy() : builtin_policy_(NewBloomFilterPolicy(10)) {}
  ~CustomFilterPolicy() { delete builtin_policy_; }

  const char* Name() const { return "IgnoreTrailingSpacesFilter"; }

  void CreateFilter(const Slice* keys, int n, std::string* dst) const {
    // Use builtin bloom filter code after removing trailing spaces
    std::vector<Slice> trimmed(n);
    for (int i = 0; i < n; i++) {
      trimmed[i] = RemoveTrailingSpaces(keys[i]);
    }
    return builtin_policy_->CreateFilter(&trimmed[i], n, dst);
  }
};
```

Advanced applications may provide a filter policy that does not use a bloom
filter but uses some other mechanism for summarizing a set of keys. See
`leveldb/filter_policy.h` for detail.

## Checksums

leveldb associates checksums with all data it stores in the file system. There
are two separate controls provided over how aggressively these checksums are
verified:

`ReadOptions::verify_checksums` may be set to true to force checksum
verification of all data that is read from the file system on behalf of a
particular read.  By default, no such verification is done.

`Options::paranoid_checks` may be set to true before opening a database to make
the database implementation raise an error as soon as it detects an internal
corruption. Depending on which portion of the database has been corrupted, the
error may be raised when the database is opened, or later by another database
operation. By default, paranoid checking is off so that the database can be used
even if parts of its persistent storage have been corrupted.

If a database is corrupted (perhaps it cannot be opened when paranoid checking
is turned on), the `leveldb::RepairDB` function may be used to recover as much
of the data as possible

## Approximate Sizes

The `GetApproximateSizes` method can used to get the approximate number of bytes
of file system space used by one or more key ranges.

```c++
leveldb::Range ranges[2];
ranges[0] = leveldb::Range("a", "c");
ranges[1] = leveldb::Range("x", "z");
uint64_t sizes[2];
leveldb::Status s = db->GetApproximateSizes(ranges, 2, sizes);
```

The preceding call will set `sizes[0]` to the approximate number of bytes of
file system space used by the key range `[a..c)` and `sizes[1]` to the
approximate number of bytes used by the key range `[x..z)`.

## Environment

All file operations (and other operating system calls) issued by the leveldb
implementation are routed through a `leveldb::Env` object. Sophisticated clients
may wish to provide their own Env implementation to get better control.
For example, an application may introduce artificial delays in the file IO
paths to limit the impact of leveldb on other activities in the system.

由 leveldb 发起的全部文件操作以及其它的操作系统调用最后都会被路由给一个 `leveldb::Env` 对象。 用户也可以提供自己的 Env 实现以实现更好的控制。比如，如果应用程序想要针对 leveldb 的文件 IO 引入一个人工延迟以限制 leveldb 对同一个系统中其它应用的影响。

```c++
class SlowEnv : public leveldb::Env {
  ... implementation of the Env interface ...
};

SlowEnv env;
leveldb::Options options;
options.env = &env;
Status s = leveldb::DB::Open(options, ...);
```

## Porting

leveldb may be ported to a new platform by providing platform specific
implementations of the types/methods/functions exported by
`leveldb/port/port.h`.  See `leveldb/port/port_example.h` for more details.

In addition, the new platform may need a new default `leveldb::Env`
implementation.  See `leveldb/util/env_posix.h` for an example.

## Other Information

Details about the leveldb implementation may be found in the following
documents:

1. [Implementation notes](impl.md)
2. [Format of an immutable Table file](table_format.md)
3. [Format of a log file](log_format.md)

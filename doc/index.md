leveldb
=======

leveldb 提供了一个持久性的 key/value 存储. keys 和 values 都是随机字节数组, 并且根据用户指定的 coparator function 对 keys 进行排序. 

向两位致敬: _Jeff Dean, Sanjay Ghemawat_

## Opening A Database

leveldb 数据库都有一个名字, 该名字对应了文件系统上一个目录, 而且该数据库内容全都存在该目录下. 下面的例子显示了如何打开一个数据库以及在必要情况下创建之. 

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

如果你想在数据库已存在的时候引发一个异常, 将下面这行加到 `leveldb::DB::Open` 调用之前: 

```c++
options.error_if_exists = true;
```

## Status


你可能注意到上面的 `leveldb::Status` 类型了. 该类型的值会被 leveldb 中大部分函数在遭遇错误的时候返回. 你可以检查它释放为 ok, 然后打印关联的错误信息即可: 

```c++
leveldb::Status s = ...;
if (!s.ok()) cerr << s.ToString() << endl;
```

## Closing A Database

当数据库不再使用的时候, 像下面这样直接删除数据库对象就可以了: 

```c++
... open the db as described above ...
... do something with db ...
delete db;
```

## Reads And Writes

数据库提供了 Put、Delete 以及 Get 方法来修改、查询数据库. 下面的代码展示了将 key1 对应的 value 移动(先拷贝后删除)到 key2 下. 

```c++
std::string value;
leveldb::Status s = db->Get(leveldb::ReadOptions(), key1, &value);
if (s.ok()) s = db->Put(leveldb::WriteOptions(), key2, value);
if (s.ok()) s = db->Delete(leveldb::WriteOptions(), key1);
```

## Atomic Updates


注意, 上一小节中如果进程在 Put 了 key2 之后但是删除 key1 之前挂了, 那么同样的 value 就出现在了多个 keys 之下. 该问题可以通过使用 `WriteBatch` 类原子地应用一组操作来避免. 

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

`WriteBatch` 保存着一系列将被应用到数据库的编辑操作, 这些操作会按照添加的顺序依次被执行. 注意, 我们先执行 Delete 后执行 Put, 这样如果 key1 和 key2 一样的情况下我们也不会错误地丢失数据. 

除了原子性, `WriteBatch` 也能加速更新过程, 因为可以把一大批独立的操作添加到同一个 batch 中然后一次性执行. 

## Synchronous Writes

默认 leveldb 每个写操作都是异步的: 进程把要写的内容 push 给操作系统后立马返回. 从操作系统内存到底层持久性存储的迁移异步地发生. 当然, 也可以把某个写操作的 sync 标识打开, 以等到数据真正被记录到持久化存储再让写操作返回. (在 Posix 系统上, 这是通过在写操作返回前调用 `fsync(...)` 或 `fdatasync(...)` 或 `msync(..., MS_SYNC)` 来实现的. )

```c++
leveldb::WriteOptions write_options;
write_options.sync = true;
db->Put(write_options, ...);
```


异步写通常比同步写快一千倍. 异步写的缺点是, 一旦机器崩溃可能会导致最后几个更新操作丢失. 注意, 仅仅是写进程崩溃(而非机器重启)将不会引起任何更新操作丢失, 因为哪怕 sync 标识为 false, 写数据也已经从进程内存 push 到了操作系统. 


异步写总是可以安全使用. 比如你要将大量的数据写入数据库, 如果丢失了最后几个更新操作, 你可以重启整个写过程. 如果数据量非常大, 一个优化点是, 每进行 N 个异步写操作则进行一次同步地写操作, 如果期间发生了崩溃, 重启自从上一个成功的同步写操作以来的更新操作即可. (同步的写操作能够更新一个标识, 该标识描述了应该从哪个地方开始重启更新操作. )

`WriteBatch` 可以作为异步写操作的替代品. 多个更新操作可以放到同一个 WriteBatch 中然后通过一次同步写(即 `write_options.sync` 置为 true)一起应用. 

## Concurrency


一个数据库一次只能被一个进程打开. LevelDB 的实现从操作系统获取一把锁来阻止误用. 在单个进程中, 同一个 `leveldb::DB` 对象可以被多个多个并发的线程安全地使用, 也就是说, 不同的线程可以写入或者获取 iterators, 或者针对同一个数据库调用 Get, 签署全部操作均不需要借助外部同步设施(leveldb 实现会自动地确保必要的同步). 但是其它对象, 比如 Iterator 或者 WriteBatch 需要外部的同步设施. 如果两个线程共享此类对象, 必须安全地对其访问, 具体见对应的头文件. 

## Iteration


下面的用例展示了如何打印数据库中全部的 key,value 对. 
```c++
leveldb::Iterator* it = db->NewIterator(leveldb::ReadOptions());
for (it->SeekToFirst(); it->Valid(); it->Next()) {
  cout << it->key().ToString() << ": "  << it->value().ToString() << endl;
}
assert(it->status().ok());  // Check for any errors found during the scan
delete it;
```

下面的用例展示了如何打印 `[start, limit)` 范围内数据:

```c++
for (it->Seek(start);
   it->Valid() && it->key().ToString() < limit;
   it->Next()) {
  ...
}
```

当然你也可以反向遍历(注意, 反向遍历可能比正向遍历要慢一些, 具体见 README.md 的读性能基准测试). 

```c++
for (it->SeekToLast(); it->Valid(); it->Prev()) {
  ...
}
```

## Snapshots

快照提供了针对整个 key-value 存储的一致性的只读视图. `ReadOptions::snapshot` 不为空表示读操作应该作用在 DB 的某个特定版本上; 若为空, 则读操作将会作用在当前版本的一个隐式的快照上.  


快照通过调用 `ReadOptions::snapshot` 方法创建:  
```c++
leveldb::ReadOptions options;
// 更新之前创建一个快照
options.snapshot = db->GetSnapshot();
// 更新数据库
... apply some updates to db ...
// 获取之前创建快照的迭代器
leveldb::Iterator* iter = db->NewIterator(options);
// 使用该迭代器查看之前快照的状态
... read using iter to view the state when the snapshot was created ...
delete iter;
db->ReleaseSnapshot(options.snapshot);
```

注意, 当一个快照不再使用的时候, 应该通过 `DB::ReleaseSnapshot` 接口进行释放. 

## Slice

`it->key()` 和 `it->value()` 调用返回的值是 `leveldb::Slice` 类型的实例. 切片是一个简单的数据结构, 包含一个长度和一个只想外部字节数组的指针. 返回一个切片比返回 `std::string` 更加高效, 因为不需要隐式地拷贝大量的 keys 和 values. 另外, leveldb 方法不返回空字符结尾的 C 风格地字符串, 因为 leveldb 的 keys 和 values 允许包含 `\0` 字节. 

C++ strings and null-terminated C-style strings can be easily converted to a
Slice:

C++ 风格的 string 和 C 风格的空字符结尾的字符串很容易转换为一个切片: 

```c++
leveldb::Slice s1 = "hello";

std::string str("world");
leveldb::Slice s2 = str;
```

A Slice can be easily converted back to a C++ string:

一个切片也很容易转换回 C++ 风格的字符串: 

```c++
std::string str = s1.ToString();
assert(str == std::string("hello"));
```

注意, 当使用切片时, 调用者要确保它内部指针指向的外部字节数组保持存活. 比如, 下面的代码就有问题: 

```c++
leveldb::Slice slice;
if (...) {
  std::string str = ...;
  slice = str;
}
Use(slice);
```


当 if 语句结束的时候, str 将会被销毁, 切片的底层存储也随之消失了, 就出问题了. 

## Comparators


前面的例子中用的都是默认的比较函数, 即逐字节字典序比较函数. 你可以定制自己的比较函数, 然后在打开数据库的时候传入. 只需继承 `leveldb::Comparator` 然后定义相关逻辑即可, 下面是一个例子: 

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

然后使用上面定义的比较器打开数据库: 

```c++
// 实例化比较器
TwoPartComparator cmp;
leveldb::DB* db;
leveldb::Options options;
options.create_if_missing = true;
// 将比较器赋值给 options.comparator
options.comparator = &cmp;
// 打开数据库
leveldb::Status status = leveldb::DB::Open(options, "/tmp/testdb", &db);
...
```

### Backwards compatibility

比较器 Name 方法返回的结果在创建数据库时绑定到数据库上, 后续每次打开都会进行检查. 如果名称改了, 对 `leveldb::DB::Open` 的调用就会失败. 因此, 当且仅当在新的 key 格式和比较函数与已有的数据库不兼容而且已有数据不再被需要的时候再修改比较器名称. 



你可以根据预先规划一点一点的演进你的 key 格式. 比如, 你可以存储一个版本号在每个 key 的结尾(大多数场景, 一个字节足够了). 当你想要切换到新的 key 格式的时候(比如新增一个第三部分到上面例子 `TwoPartComparator` 处理的 keys 中), (a)保持比较器名称不变(b)递增新 keys 的版本号(c)修改比较器函数以让其使用版本号来决定如何进行排序. 

## Performance


通过修改 `include/leveldb/options.h` 中定义的类型的默认值来对 leveldb 的性能进行调优. 

### Block size


leveldb 把相邻的 keys 组织在同一个 block 中(具体见 [sstable 文件格式](table_format.md)), 而且 block 是 leveldb 把数据从内存到转移到持久化存储和从持久化存储转移到内存的基本单位. 默认的 block 大约为 4KB, 压缩前. 经常处理大块数据的应用可能希望把这个值调大, 而针对数据做"点读" 的应用可能希望这个值小一点, 这样性能可能会更高一些. 但是, 没有证据表明该值小于 1KB 或者大于几个 MB 的时候性能会表现更好. 同时要注意, 针对大的 block size, 进行压缩效率会更高. 

### Compression



每个 block 在写入持久存储之前都会被单独压缩. 压缩默认是开启的, 因为默认的压缩算法非常快, 而且对于不能压缩的数据会自动关闭压缩. 极少的场景会让用户想要完全关闭压缩功能, 除非基准测试显示关闭压缩会显著改善性能. 按照下面方式做就关闭了压缩功能: 

```c++
leveldb::Options options;
options.compression = leveldb::kNoCompression;
... leveldb::DB::Open(options, name, ...) ....
```

### Cache


数据库的内容存储在文件系统的一组文件里, 每个文件保存着一系列压缩后的 blocks. 如果 options.block_cache 不为空, 它就会被用于缓存频繁被使用的 block 内容(已解压缩). 

```c++
#include "leveldb/cache.h"

leveldb::Options options;
// 打开数据库之前分配一个 100MB 的 LRU Cache 用于缓存解压的 blocks
options.block_cache = leveldb::NewLRUCache(100 * 1048576);  // 100MB cache
leveldb::DB* db;
// 打开数据库
leveldb::DB::Open(options, name, &db);
... use the db ...
delete db
delete options.block_cache;
```


注意 cache 保存的是未压缩的数据, 因此应该根据应用程序所需的数据大小来设置它的大小. (已经压缩的数据的缓存工作交给操作系统的 buffer cache 或者用户提供的定制的 Env 实现去干. )

当执行一个大块数据读操作时, 应用程序可能想要取消缓存功能, 这样通过大块读进来数据就不会导致 cache 中当前大部分数据被置换出去. 我们可以为它提供一个单独的 iterator 来达到目的: 

```c++
leveldb::ReadOptions options;
// 缓存设置为关闭
options.fill_cache = false;
// 用该设置去创建一个新的迭代器
leveldb::Iterator* it = db->NewIterator(options);
// 用该迭代器去处理大块数据
for (it->SeekToFirst(); it->Valid(); it->Next()) {
  ...
}
```

### Key Layout


注意, 磁盘传输的单位以及磁盘缓存的单位都是一个 block. 相邻的 keys(已排序)总是在同一个 block 中. 因此应用程序可以通过把需要一起访问的 keys 放在一起, 同时把不经常使用的 keys 放到一个独立的键空间区域来提升性能. 

举个例子, 假设我们正基于 leveldb 实现一个简单的文件系统. 我们打算存储到这个文件系统的数据项类型如下: 

    filename -> permission-bits, length, list of file_block_ids
    file_block_id -> data

我们可能想要给上面表示 filename 的键增加一个字符前缀, 比如 '/', 然后给表示 `file_block_id` 的键增加另一个不同的前缀, 比如 '0', 这样这些不同用途的键就具有了各自独立的键空间区域, 扫描元数据的时候我们就不用读取和缓存大块文件内容数据了. 

### Filters


鉴于 leveldb 数据在磁盘上的组织形式, 一次 `Get()` 调用可能涉及多次磁盘读操作. 可选的 FilterPolicy 机制可以用来大幅减少磁盘读次数. 

```c++
leveldb::Options options;
// 设置启用基于布隆过滤器的过滤策略
options.filter_policy = NewBloomFilterPolicy(10);
leveldb::DB* db;
// 用该设置打开数据库
leveldb::DB::Open(options, "/tmp/testdb", &db);
... use the database ...
delete db;
delete options.filter_policy;
```



上述代码将一个基于布隆过滤器的过滤策略与数据库进行了关联. 基于布隆过滤器的过滤方式依赖于如下事实, 在内存中保存每个 key 的部分位(在上面例子中是 10 位, 因为我们传给 `NewBloomFilterPolicy` 的参数是 10). 这个过滤器将会使得 Get() 调用中非必须的磁盘读操作大约减少 100 倍. 增加每个 key 用于过滤器的位数将会进一步减少读磁盘次数, 当然也会占用更多内存空间. 我们推荐数据集无法全部放入内存同时又存在大量随机读的应用设置一个过滤器策略. 



如果你在使用定制的比较器, 你应该确保你在用的过滤器策略与你的比较器兼容. 举个例子, 如果一个比较器在比较键的时候忽略结尾的空格, 那么`NewBloomFilterPolicy` 一定不能与此比较器共存. 相反, 应用应该提供一个定制的过滤器策略, 而且它也应该忽略键的尾部空格. 示例如下: 

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
    // 将尾部空格移除后再使用内置的布隆过滤器
    std::vector<Slice> trimmed(n);
    for (int i = 0; i < n; i++) {
      trimmed[i] = RemoveTrailingSpaces(keys[i]);
    }
    return builtin_policy_->CreateFilter(&trimmed[i], n, dst);
  }
};
```



高级应用可以自己提供不使用布隆过滤器的过滤器策略, 具体见 `leveldb/filter_policy.h`. 

## Checksums

leveldb 将一个校验和与它存储在文件系统中的全部数据进行关联. 根据激进程度有两种方式控制校验和的核对: 


`ReadOptions::verify_checksums` 可以设置为 true 来强制核对从文件系统读取的全部数据的进行校验和检查. 默认为 false. 


`Options::paranoid_checks` 在数据库打开之前设置为 true 可以使得数据库一旦检测到数据损毁即报错. 取决于数据库损坏部位, 报错时机可能是打开数据库后的时候, 也可能是在后续执行某个操作的时候. 偏执检查默认是关闭状态, 这样即使持久性存储部分虽坏数据库也能继续使用. 



如果数据库损坏了(当开启偏执检查的时候可能就打不开了), `leveldb::RepairDB` 函数可以用于对尽可能多的数据进行修复. 

## Approximate Sizes


`GetApproximateSizes` 方法用于后去一个或多个键区间占据的文件系统近似大小(单位, 字节). 

```c++
leveldb::Range ranges[2];
ranges[0] = leveldb::Range("a", "c");
ranges[1] = leveldb::Range("x", "z");
uint64_t sizes[2];
leveldb::Status s = db->GetApproximateSizes(ranges, 2, sizes);
```


上述代码结果是, `size[0]` 保存 `[a..c)` 键区间对应的文件系统字节数, `size[1]` 保存 `[x..z)` 键区间对应的文件系统字节数. 

## Environment


由 leveldb 发起的全部文件操作以及其它的操作系统调用最后都会被路由给一个 `leveldb::Env` 对象.  用户也可以提供自己的 Env 实现以实现更好的控制. 比如, 如果应用程序想要针对 leveldb 的文件 IO 引入一个人工延迟以限制 leveldb 对同一个系统中其它应用的影响. 

```c++
// 定制自己的 Env 
class SlowEnv : public leveldb::Env {
  ... implementation of the Env interface ...
};

SlowEnv env;
leveldb::Options options;
options.env = &env;
// 用定制的 Env 打开数据库
Status s = leveldb::DB::Open(options, ...);
```

## Porting


如果针对特定平台提供 `leveldb/port/port.h` 导出的类型/方法/函数实现, 那么 leveldb 可以被移植到该平台上, 更多细节见 `leveldb/port/port_example.h`. 

另外, 新平台可能还需要一个新的默认的 `leveldb::Env` 实现. 具体可参考 `leveldb/util/env_posix.h` 实现. 

## Other Information


关于 leveldb 实现的更多细节请见下面的文档: 

1. [Implementation notes](impl.md)
2. [Format of an immutable Table file](table_format.md)
3. [Format of a log file](log_format.md)

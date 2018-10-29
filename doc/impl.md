[toc]
## Files（leveldb 中的各种文件）

The implementation of leveldb is similar in spirit to the representation of a
single [Bigtable tablet (section 5.3)](http://research.google.com/archive/bigtable.html).
However the organization of the files that make up the representation is
somewhat different and is explained below.

Each database is represented by a set of files stored in a directory. There are
several different types of files as documented below:

每个 leveldb 的实现类似于一个单独的 [Bigtable tablet (section 5.3)](http://research.google.com/archive/bigtable.html)，但是它的文件组织有点不同。

每个 leveldb database 就是存储到某个目录下的一组文件。这些文件分为如下几种：

### Log files （日志文件）

A log file (*.log) stores a sequence of recent updates. Each update is appended
to the current log file. When the log file reaches a pre-determined size
(approximately 4MB by default), it is converted to a sorted table (see below)
and a new log file is created for future updates.

A copy of the current log file is kept in an in-memory structure (the
`memtable`). This copy is consulted on every read so that read operations
reflect all logged updates.

一个 log 文件（*.log）保存着最近一系列更新操作。每个更新操作都被追加到当前的 log 文件中。
当 log 文件大小达到一个预定义的大小时（默认大约 4MB），这个 log 文件就会被转换为一个有序 table（见下文）然后一个新的 log 文件就会被创建以保存未来的更新操作。

当前 log 文件的一个拷贝被保存在一个内存数据结构中（即 `memtable`)。这份拷贝会在每次执行读操作时被查询以让每个读操作都能反映全部已被记录到 log 文件的更新。

## Sorted tables

A sorted table (*.ldb) stores a sequence of entries sorted by key. Each entry is
either a value for the key, or a deletion marker for the key. (Deletion markers
are kept around to hide obsolete values present in older sorted tables).

The set of sorted tables are organized into a sequence of levels. The sorted
table generated from a log file is placed in a special **young** level (also
called level-0). When the number of young files exceeds a certain threshold
(currently four), all of the young files are merged together with all of the
overlapping level-1 files to produce a sequence of new level-1 files (we create
a new level-1 file for every 2MB of data.)

Files in the young level may contain overlapping keys. However files in other
levels have distinct non-overlapping key ranges. Consider level number L where
L >= 1. When the combined size of files in level-L exceeds (10^L) MB (i.e., 10MB
for level-1, 100MB for level-2, ...), one file in level-L, and all of the
overlapping files in level-(L+1) are merged to form a set of new files for
level-(L+1). These merges have the effect of gradually migrating new updates
from the young level to the largest level using only bulk reads and writes
(i.e., minimizing expensive seeks).



一个有序 table（*.ldb）保存着一系列数据项，这些数据项已被按照 key 排过序了。每个数据项要么是一个与某个 key 对应的 value，要么是某个 key 的删除标记。（删除标记用来隐藏保存在更老的有序 tables 中的已经过期的数据）。

这些有序的 tables 被组织成一系列 levels。从一个 log 文件生成的有序 table 被放到一个特殊的 **young** level（也被叫做 level-0）。当 young 文件数目超过某个阈值（当前是 4），这些全部的 young 文件就会和 level-1 的全部文件进行合并，消除两个 level 文件之间的键区间重叠（overlapping）同时生成一系列新的 level-1 文件（我们为每 2MB 数据生成一个新的 level-1 文件）。

注意，young level 的文件之间可能存在键区间的重叠情况，但是其它每层 level 内部文件之间是不存在重叠情况的。我们下面来说下 level-1 及其以上的 level 的文件如何合并。当 level-L （L >= 1）的文件总大小超过了 10^L MB（即 level-1 超过了 10MB，level-2 超过了 100MB， ... ...），此时 level-L 的一个文件就会和 level-(L+1) 中与自己键区间重叠的文件进行合并，然后为 level-(L+1) 生成一组新的文件。这些合并过程可以实现将 young level 的新的更新操作一点一点搬到最高的那层 level，这个迁移过程使用的都是块读写（最小化了昂贵的定位 （seeks）消耗）。

### Manifest

A MANIFEST file lists the set of sorted tables that make up each level, the
corresponding key ranges, and other important metadata. A new MANIFEST file
(with a new number embedded in the file name) is created whenever the database
is reopened. The MANIFEST file is formatted as a log, and changes made to the
serving state (as files are added or removed) are appended to this log.

MANIFEST 文件列出了构成每一个 level 的全部有序 tables，每个 table 的键区间，以及其它重要的元数据。每当重新打开某个数据库的时候，就会创建一个新的 MANIFEST 文件（文件名中有一个新的数字）。MANIFEST 文件的内容就是一条条 log，每当某 level 发生新增文件或者删除文件，就会有一条 log 被追加到 MANIFEST。

### Current

CURRENT is a simple text file that contains the name of the latest MANIFEST
file.

CURRENT 文件是一个简单的文本文件，包含了最新的 MANIFEST 文件的名称。

### Info logs

Informational messages are printed to files named LOG and LOG.old.

info 级别的消息会被输出到名为 LOG 或者 LOG.old 的文件中。

### Others

Other files used for miscellaneous purposes may also be present (LOCK, *.dbtmp).

其它文件被用于比较零碎的目的，现有的其它文件有 LOCK、*.dbtmp。

## Level 0

When the log file grows above a certain size (4MB by default):
Create a brand new memtable and log file and direct future updates here.

In the background:

1. Write the contents of the previous memtable to an sstable.
2. Discard the memtable.
3. Delete the old log file and the old memtable.
4. Add the new sstable to the young (level-0) level.

当 level-0 的 log 文件大小超过某个值（默认 4MB）：

就会创建一个全新的 memtable 和 log 文件，新的更新操作就会写到这里。

同时在后台：

1. 将前一个 memtable 内容写到一个 sstable。
2. 丢弃这个 memtable。
3. 删除老的 log 文件和老的 memtable。
4. 新增一个 sstable 到 leve-0。

## Compactions 压实

When the size of level L exceeds its limit, we compact it in a background
thread. The compaction picks a file from level L and all overlapping files from
the next level L+1. Note that if a level-L file overlaps only part of a
level-(L+1) file, the entire file at level-(L+1) is used as an input to the
compaction and will be discarded after the compaction.  Aside: because level-0
is special (files in it may overlap each other), we treat compactions from
level-0 to level-1 specially: a level-0 compaction may pick more than one
level-0 file in case some of these files overlap each other.

A compaction merges the contents of the picked files to produce a sequence of
level-(L+1) files. We switch to producing a new level-(L+1) file after the
current output file has reached the target file size (2MB). We also switch to a
new output file when the key range of the current output file has grown enough
to overlap more than ten level-(L+2) files.  This last rule ensures that a later
compaction of a level-(L+1) file will not pick up too much data from
level-(L+2).

The old files are discarded and the new files are added to the serving state.

Compactions for a particular level rotate through the key space. In more detail,
for each level L, we remember the ending key of the last compaction at level L.
The next compaction for level L will pick the first file that starts after this
key (wrapping around to the beginning of the key space if there is no such
file).

Compactions drop overwritten values. They also drop deletion markers if there
are no higher numbered levels that contain a file whose range overlaps the
current key.

当 level-L 大小超过了上限，我们就在后台线程中将其压实。压实过程会从 level-L 挑一个文件，然后将 level-(L+1) 中与该文件键区间重叠的文件都找出来。注意，如果一个 level-L 文件键区间仅是 level-(L+1) 某个文件的一部分，那么 level-(L+1) 的这个文件就会整个作为压实过程的输入，等压实结束后该文件就会被丢弃。另外，因为 level-0 比较特殊（该层的文件之间可能相互重叠），我们会把 level-0 到 level-1 的压实过程做特殊处理：我们会从 level-0 挑选多个文件进行压实，这么做是防止这些文件互相重叠。

一次压实会合并多个被挑选文件的内容从而生成一系列新的 level-(L+1) 文件，生成一个新文件的条件有两个：当前文件达到了 2MB 大小或者当前文件的键区间与超过 10 个 level-(L+2) 文件发生了重叠。第二个条件的目的在于避免后续对 level-(L+1) 文件进行压实时需要从 level-(L+2) 读取过多的数据。

压实后，旧的文件会被丢弃，新生成的文件开始生效。

针对某一个 level 的压实会循环整个键空间。具体来讲，针对 level L，我们会记住 level L 上次压实的最后一个 key。针对 level L 的下次压实将会挑选从这个 key 之后开始的第一个文件进行。（如果不存在这样的文件，那么就会遍历回键空间起始 key）。

压实会丢弃某个 key 对应的被覆盖过的 values（只保留最新的那个 value），也会在没有更高的 level 包含某 key 的时候丢弃这个 key 的删除标记。

### Timing 压实时间消耗

Level-0 compactions will read up to four 1MB files from level-0, and at worst
all the level-1 files (10MB). I.e., we will read 14MB and write 14MB.

Other than the special level-0 compactions, we will pick one 2MB file from level
L. In the worst case, this will overlap ~ 12 files from level L+1 (10 because
level-(L+1) is ten times the size of level-L, and another two at the boundaries
since the file ranges at level-L will usually not be aligned with the file
ranges at level-L+1). The compaction will therefore read 26MB and write 26MB.
Assuming a disk IO rate of 100MB/s (ballpark range for modern drives), the worst
compaction cost will be approximately 0.5 second.

If we throttle the background writing to something small, say 10% of the full
100MB/s speed, a compaction may take up to 5 seconds. If the user is writing at
10MB/s, we might build up lots of level-0 files (~50 to hold the 5*10MB). This
may significantly increase the cost of reads due to the overhead of merging more
files together on every read.

Solution 1: To reduce this problem, we might want to increase the log switching
threshold when the number of level-0 files is large. Though the downside is that
the larger this threshold, the more memory we will need to hold the
corresponding memtable.

Solution 2: We might want to decrease write rate artificially when the number of
level-0 files goes up.

Solution 3: We work on reducing the cost of very wide merges. Perhaps most of
the level-0 files will have their blocks sitting uncompressed in the cache and
we will only need to worry about the O(N) complexity in the merging iterator.

Level-0 压实将会从 level-0 读取高达 4 个 1MB 文件，最坏情况下同时会把 level-1 全部 10MB 文件都读进来。也就是说，这种情况下我们会读取 14MB 写入 14MB。

除了特殊的 level-0 压实过程，我们会从 level L 选取一个 2MB 大小的文件。最坏情况下，这会与 level L+1 层大约 12 个文件发生重叠（其中 10 个是因为 level-(L+1) 大小是 level-L 的十倍，另外 2 个（作为前面提到的 10 个文件前后的边界文件）是因为 level-L 的文件区间通常不与 level-(L+1) 对齐）。因此压实会读取 26MB 写入 26MB。假设磁盘 IO 速度为 100MB/s（现代的磁盘驱动大约就这速度），最坏情况下的压实将会消耗大约 0.5 秒（读写共 52MB，读或者写都需要寻道，两个操作是串行的）。

假如我们把后台写入速度限制到一个比较小的值，比如全速 100MB/s 的 10%，一次压实大约消耗 5 秒。如果用户正在以 100MB/s 的速度写磁盘，我们可能需要构建大量的 level-0 文件（大约 50 个文件来保存 5*10MB 数据）。这会显著增加读操作时间消耗，因为每次读都需要合并更多的文件。（？）

解决方案 1：为了减轻这个问题的影响，我们可能会想要在 level-0 文件个数太大的时候增加 log 文件的切换阈值。但这么做的缺点是，这个阈值越大，我们需要更多的内存来维持对应的 memtable。

解决方案 2：我们可能想要在 level-0 文件个数蹿升时人工减小写入速度。

解决方案 3：我们设法减少大范围合并的消耗。可能大多数 level-0 文件对应的块在缓存中处于未压缩状态，我们只需操心合并时的 O(N) 复杂度的遍历。

### Number of files 文件个数的影响

Instead of always making 2MB files, we could make larger files for larger levels
to reduce the total file count, though at the expense of more bursty
compactions.  Alternatively, we could shard the set of files into multiple
directories.

An experiment on an ext3 filesystem on Feb 04, 2011 shows the following timings
to do 100K file opens in directories with varying number of files:


| Files in directory | Microseconds to open a file |
|-------------------:|----------------------------:|
|               1000 |                           9 |
|              10000 |                          10 |
|             100000 |                          16 |

So maybe even the sharding is not necessary on modern filesystems?

不再总是构造大小为 2MB 大小的文件，我们可以为更高的 level 构造更大的文件以减少总的文件个数，虽然这样会导致更高的压实成本。或者，我们可以将同一组文件分片到多个目录中。

2011 年 2 月 4 号，我们在一个 ext3 文件系统上做了针对不同数目的文件打开十万次的时间消耗测试：

|目录中的文件数         | 打开一个文件的微妙数          |
|-------------------:|----------------------------:|
|               1000 |                           9 |
|              10000 |                          10 |
|             100000 |                          16 |

测试结果显示，在现代文件系统上，分片可能不是必须的。

## Recovery

* Read CURRENT to find name of the latest committed MANIFEST
* Read the named MANIFEST file
* Clean up stale files
* We could open all sstables here, but it is probably better to be lazy...
* Convert log chunk to a new level-0 sstable
* Start directing new writes to a new log file with recovered sequence#

- 读取 CURRENT 文件找到最新提交的 MANIFEST 文件的名称
- 读取该 MANIFEST 文件内容
- 删除过期的文件
- 这一步我们可以打开全部 sstables，但最好延迟一会
- 将 log 文件转换为一个新的 level-0 sstable
- 将接下来的写操作将会写入这个待恢复序列号的新 log 文件

## Garbage collection of files

`DeleteObsoleteFiles()` is called at the end of every compaction and at the end
of recovery. It finds the names of all files in the database. It deletes all log
files that are not the current log file. It deletes all table files that are not
referenced from some level and are not the output of an active compaction.

每次压实结束或者恢复结束 `DeleteObsoleteFiles()` 方法就会被调用。该方法会找到数据库中的全部文件的名称。它会删除全部的非当前 log 文件，也会删除全部 table 文件（这些文件不再被任何 level 引用且不是某个正在进行的压实过程的输出文件）。
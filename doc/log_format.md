leveldb Log format
==================
The log file contents are a sequence of 32KB blocks.  The only exception is that
the tail of the file may contain a partial block.

Each block consists of a sequence of records:

    block := record* trailer?
    record :=
      checksum: uint32     // crc32c of type and data[] ; little-endian
      length: uint16       // little-endian
      type: uint8          // One of FULL, FIRST, MIDDLE, LAST
      data: uint8[length]

A record never starts within the last six bytes of a block (since it won't fit).
Any leftover bytes here form the trailer, which must consist entirely of zero
bytes and must be skipped by readers.

Aside: if exactly seven bytes are left in the current block, and a new non-zero
length record is added, the writer must emit a FIRST record (which contains zero
bytes of user data) to fill up the trailing seven bytes of the block and then
emit all of the user data in subsequent blocks.

More types may be added in the future.  Some Readers may skip record types they
do not understand, others may report that some data was skipped.

    FULL == 1
    FIRST == 2
    MIDDLE == 3
    LAST == 4

The FULL record contains the contents of an entire user record.

FIRST, MIDDLE, LAST are types used for user records that have been split into
multiple fragments (typically because of block boundaries).  FIRST is the type
of the first fragment of a user record, LAST is the type of the last fragment of
a user record, and MIDDLE is the type of all interior fragments of a user
record.

Example: consider a sequence of user records:

    A: length 1000
    B: length 97270
    C: length 8000

**A** will be stored as a FULL record in the first block.

**B** will be split into three fragments: first fragment occupies the rest of
the first block, second fragment occupies the entirety of the second block, and
the third fragment occupies a prefix of the third block.  This will leave six
bytes free in the third block, which will be left empty as the trailer.

**C** will be stored as a FULL record in the fourth block.


log 文件内容是一系列 blocks，每个 block 大小为 32KB。唯一的例外就是，log 文件末尾可能包含一个不完整的 block。

每个 block 由一系列 records 构成：

    block := record* trailer? // 即 0 或多个 records，0 或 1 个 trailer，总大小为 4 + 2 + 1 + length + trailer 大小
    record :=
      // 下面的 type 和 data[] 的 crc32c 校验和，小端字节序
      checksum: uint32     // crc32c of type and data[] ; little-endian
      // 下面的 data[] 的长度，小端字节序
      length: uint16       // little-endian
      // 类型，FULL、FIRST、MIDDLE、LAST 取值之一
      type: uint8          // One of FULL, FIRST, MIDDLE, LAST
      // 数据
      data: uint8[length]

如果一个 block 剩余字节不超过 6 个，则不会在这个剩余空间构造任何 record，因为大小不合适。这些剩余空间构成 trailer，应该被 reader 略过。

此外，如果当前 block 恰好剩余 7 个字节（正好可以容纳 record 中的 checksum + length + type），并且一个新的非 0 长度的 record 要被写入，那么 writer 必须写入一个 FIRST 类型的 record（确切讲应该是 fragment），而且 length 值为 0（此时该 block 已满，所以用户数据 data 部分需要写入下个 block，而且下个 block 起始还是要写入一个 header 不过该 header 类型为 middle）来填满该 block 尾部的 7 个字节，然后在接下来的 blocks 中写入全部用户数据。

未来可能加入更多的 record 类型。Readers 可以跳过它们不理解的 record 类型，也可以在跳过时进行报告。

    FULL == 1
    FIRST == 2
    MIDDLE == 3
    LAST == 4

FULL 类型的 record 包含了一个完整的用户 record 的内容。

FIRST、MIDDLE、LAST 这三个类型用于被分割成多个 fragments（典型地理由是某个 record 跨越了多个 block 边界） 的用户 record。FIRST 表示某个用户 record 的第一个 fragment，LAST 表示某个用户 record 的最后一个 fragment，MIDDLE 表示某个用户 record 的中间 fragments。

举例：考虑下面一系列用户 records：

    A：长度 1000
    B：长度 97270
    C：长度 8000 

**A** 会被作为 FULL 类型的 record 存储到第一个 block；

**B** 会被分割为 3 个 fragments：第一个 fragment 占据第一个 block 剩余空间，第二个 fragment 占据第二个 block 的全部空间，第三个 fragment 占据第三个 block 的起始空间。最后在第三个 block 剩下 6 个字节，这几个字节会被留空作为 trailer。

**C** 将会被作为 FULL 类型的 record 存储到第四个 block 中。

----

## Some benefits over the recordio format:

1. We do not need any heuristics for resyncing - just go to next block boundary
   and scan.  If there is a corruption, skip to the next block.  As a
   side-benefit, we do not get confused when part of the contents of one log
   file are embedded as a record inside another log file.

2. Splitting at approximate boundaries (e.g., for mapreduce) is simple: find the
   next block boundary and skip records until we hit a FULL or FIRST record.

3. We do not need extra buffering for large records.

上述 log 内容格式的好处是：

1. 不必进行任何启发式地 resyncing —— 直接跳到下个 block 边界进行扫描即可。如果数据有损坏，直接跳到下个 block。这么做的附带好处是，当某个 log 文件的部分内容作为一个 record 嵌入到另一个 log 文件时，我们不会分不清楚。
2. 在估计出来的边界处（比如 mapreduce）做分割变得简单了：找到下个 block 的边界然后跳过多个记录直到我们找到一个 FULL 或者 FIRST record 为止。

## Some downsides compared to recordio format:

1. No packing of tiny records.  This could be fixed by adding a new record type,
   so it is a shortcoming of the current implementation, not necessarily the
   format.

2. No compression.  Again, this could be fixed by adding new record types.

上述 log 内容格式的缺点：

1. 没有打包小的 records。通过增加一个新的 record 类型可以解决这个问题，所以这个问题是当前实现的不足而不是 log 格式的缺陷。
2. 没有压缩。再说一遍，这个可以通过增加一个新的 record 类型来解决。
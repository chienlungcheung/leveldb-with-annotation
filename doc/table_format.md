leveldb File format
===================

    <beginning_of_file>
    [data block 1]
    [data block 2]
    ...
    [data block N]
    [meta block 1]
    ...
    [meta block K]
    [metaindex block]
    [index block]
    [Footer]        (fixed size; starts at file_size - sizeof(Footer))
    <end_of_file>

The file contains internal pointers.  Each such pointer is called
a BlockHandle and contains the following information:

    offset:   varint64
    size:     varint64

See [varints](https://developers.google.com/protocol-buffers/docs/encoding#varints)
for an explanation of varint64 format.

1.  The sequence of key/value pairs in the file are stored in sorted
order and partitioned into a sequence of data blocks.  These blocks
come one after another at the beginning of the file.  Each data block
is formatted according to the code in `block_builder.cc`, and then
optionally compressed.

2. After the data blocks we store a bunch of meta blocks.  The
supported meta block types are described below.  More meta block types
may be added in the future.  Each meta block is again formatted using
`block_builder.cc` and then optionally compressed.

3. A "metaindex" block.  It contains one entry for every other meta
block where the key is the name of the meta block and the value is a
BlockHandle pointing to that meta block.

4. An "index" block.  This block contains one entry per data block,
where the key is a string >= last key in that data block and before
the first key in the successive data block.  The value is the
BlockHandle for the data block.

5. At the very end of the file is a fixed length footer that contains
the BlockHandle of the metaindex and index blocks as well as a magic number.

        metaindex_handle: char[p];     // Block handle for metaindex
        index_handle:     char[q];     // Block handle for index
        padding:          char[40-p-q];// zeroed bytes to make fixed length
                                       // (40==2*BlockHandle::kMaxEncodedLength)
        magic:            fixed64;     // == 0xdb4775248b80fb57 (little-endian)

leveldb sstable 文件的格式如下：

    <beginning_of_file>
    [data block 1]
    [data block 2]
    ...
    [data block N]
    [meta block 1]
    ...
    [meta block K]
    [metaindex block]
    [index block]
    [Footer]        (fixed size; starts at file_size - sizeof(Footer))
    <end_of_file>

该文件包含了内部指针。每个指针被叫做 BlockHandle，包含着下述信息：

    offset: varint64 对应 block 起始位置在文件中的偏移量
    size:   varint64 对应 block 的大小

下面详细解释下上面提到的文件格式：

1. 文件里存的是一系列 key/value 对，而且按照 key 排过序了，同时被划分到了多个 blocks 中。这些 blocks 从文件起始位置开始一个接一个。每个 data block 组织形式见 `block_builder.cc` 定义，用户可以选择对 data block 进行压缩。

2. 全部 data blocks 之后是一组 meta blocks。已经支持的 meta block 类型见下面描述，将来可能会加入更多的类型。每个 meta block 组织形式见 `block_builder.cc` 定义，同样地，用户可以选择对其进行压缩。

3. 全部 meta blocks 后是一个 metaindex block。每个 meta block 都有一个对应的 entry 保存在该 block 中，其中 key 就是某个 meta block 的名字，value 是一个指向该 meta block 的 BlockHandle。

4. 紧随 metaindex block 之后是一个 index block。针对每个 data block 都有一个对应的 entry 包含在 index block 中，其中 key 为大于等于对应 data block 最后（也是最大的，因为排序过了）一个 key 同时小于接下来的 data block 第一个 key 的字符串；value 是指向一个对应 data block 的 BlockHandle。

5. 在每个文件的末尾是一个固定长度的 footer，它包含了一个指向 metaindex block
   的 BlockHandle 和一个指向 index block 的 BlockHandle 以及一个 magic number。
   
           metaindex_handle: char[p];     // 指向 metaindex 的 BlockHandle
           index_handle:     char[q];     // 指向 index 的 BlockHandle
           padding:          char[40-p-q];// 用于维持固定长度的 padding 0
                                          // (40==2*BlockHandle::kMaxEncodedLength)
           magic:            fixed64;     // == 0xdb4775248b80fb57 (little-endian)

## "filter" Meta Block

If a `FilterPolicy` was specified when the database was opened, a
filter block is stored in each table.  The "metaindex" block contains
an entry that maps from `filter.<N>` to the BlockHandle for the filter
block where `<N>` is the string returned by the filter policy's
`Name()` method.

The filter block stores a sequence of filters, where filter i contains
the output of `FilterPolicy::CreateFilter()` on all keys that are stored
in a block whose file offset falls within the range

    [ i*base ... (i+1)*base-1 ]

Currently, "base" is 2KB.  So for example, if blocks X and Y start in
the range `[ 0KB .. 2KB-1 ]`, all of the keys in X and Y will be
converted to a filter by calling `FilterPolicy::CreateFilter()`, and the
resulting filter will be stored as the first filter in the filter
block.

The filter block is formatted as follows:

    [filter 0]
    [filter 1]
    [filter 2]
    ...
    [filter N-1]

    [offset of filter 0]                  : 4 bytes
    [offset of filter 1]                  : 4 bytes
    [offset of filter 2]                  : 4 bytes
    ...
    [offset of filter N-1]                : 4 bytes

    [offset of beginning of offset array] : 4 bytes
    lg(base)                              : 1 byte

The offset array at the end of the filter block allows efficient
mapping from a data block offset to the corresponding filter.

如果打开数据库的时候指定了一个 `FilterPolicy`，那么一个 filter block 就会被存储到每个 sstable 中。metaindex block 包含了一个 entry，它是从 `filter.<N>` 到 filter block 的 BlockHandle 的映射。其中，`<N>` 是一个由 filter policy 的 `Name()`方法返回的字符串。

filter block 保存着一系列 filters，其中 filter i 包含了 `FilterPolicy::CreateFilter()` 针对入参 keys 的输出，这些 keys 保存在一个 block 中，该 block 对应的文件偏移量落在下面的范围里：

    [ i*base ... (i+1)*base-1 ]

当前，上面的 base 是 2KB。比如，如果 block X 和 block Y 起始地址落在 `[ 0KB .. 2KB ]`  范围内，X 和 Y 中的全部 keys 将会在调用 `FilterPolicy::CreateFilter()` 时被转换为一个 filter，然后这个 filter 会作为第一个（为啥是第一个，因为 X、Y 起始地址落在第一个地址空间里） filter 被保存在 filter block 中。（用大白话再说一遍，每个 FilterPolicy 都有一个唯一的名字，在 metaindex block 通过这个名字就能找到对应的 filter block 了。而 filter block 存的就是用这个 FilterPolicy 构造的一系列 filters，为啥是一系列呢？因为 data blocks 太多了，所以分了区间，每几个 data blocks 对应一个 filter，具体几个根据上面那个带 base 的公式来算。再说说 filter 是怎么回事。data block 保存的不是键值对构成的 records 嘛，根据前面说的键区间限制，把每几个 blocks 的全部键根据某个 FilterPolicy 算一下就得到了一个 filter，然后把这个 filter 保存到了 filter block 的第 i 个位置。）

filter block 格式如下：

    [filter 0]
    [filter 1]
    [filter 2]
    ...
    [filter N-1]

    [offset of filter 0]                  : 4 bytes
    [offset of filter 1]                  : 4 bytes
    [offset of filter 2]                  : 4 bytes
    ...
    [offset of filter N-1]                : 4 bytes

    [offset of beginning of offset array] : 4 bytes
    lg(base)                              : 1 byte

其中，位于 filter block 尾部的 offset 数组可以使得我们快速定位到某个 filter。

## "stats" Meta Block

This meta block contains a bunch of stats.  The key is the name
of the statistic.  The value contains the statistic.

TODO(postrelease): record following stats.

    data size
    index size
    key size (uncompressed)
    value size (uncompressed)
    number of entries
    number of data blocks

下面的 meta block 保存着一组统计信息。key 是统计量的名称，value 是具体的统计值。

    data size
    index size
    key size (uncompressed)
    value size (uncompressed)
    number of entries
    number of data blocks
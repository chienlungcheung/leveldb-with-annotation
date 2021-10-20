leveldb Log format
==================

log 文件内容是一系列 blocks, 每个 block 大小为 32KB. 唯一的例外就是, log 文件末尾可能包含一个不完整的 block. 

每个 block 由一系列 records 构成: 

    block := record* trailer? // 即 0 或多个 records, 0 或 1 个 trailer, 总大小为 4 + 2 + 1 + length + trailer 大小
    record :=
      // 下面的 type 和 data[] 的 crc32c 校验和, 小端字节序
      checksum: uint32     // crc32c of type and data[] ; little-endian
      // 下面的 data[] 的长度, 小端字节序
      length: uint16       // little-endian
      // 类型, FULL、FIRST、MIDDLE、LAST 取值之一
      type: uint8          // One of FULL, FIRST, MIDDLE, LAST
      // 数据
      data: uint8[length]

如果一个 block 剩余字节不超过 6 个(checksum 字段长度 + length 字段长度 + type 字段长度 = 7), 则不会再构造任何 record, 如前括号解释因为大小不合适. 这些剩余空间会被用于构造一个 trailer, reader 读取该文件时候会忽略之. 

此外, 如果当前 block 恰好剩余 7 个字节(正好可以容纳 record 中的 checksum + length + type), 并且一个新的非 0 长度的 record 要被写入, 那么 writer 必须在此处写入一个 FIRST 类型的 record(但是 length 字段值为 0, data 字段为空. 用户数据 data 部分需要写入下个 block, 而且下个 block 起始还是要写入一个 header 不过其 type 为 middle)来填满该 block 尾部的 7 个字节, 然后在接下来的 blocks 中写入全部用户数据.

未来可能加入更多的 record 类型. Readers 可以跳过它们不理解的 record 类型, 也可以在跳过时进行报告. 

    FULL == 1
    FIRST == 2
    MIDDLE == 3
    LAST == 4

FULL 类型的 record 包含了一个完整的用户 record 的内容. 

FIRST、MIDDLE、LAST 这三个类型用于被分割成多个 fragments(典型的理由是某个 record 跨越了多个 block 边界) 的用户 record. FIRST 表示某个用户 record 的第一个 fragment, LAST 表示某个用户 record 的最后一个 fragment, MIDDLE 表示某个用户 record 的中间 fragments. 

举例: 考虑下面一系列用户 records: 

    A: 长度 1000
    B: 长度 97270
    C: 长度 8000 

**A** 会被作为 FULL 类型的 record 存储到第一个 block, 第一个 block 剩余空间为 32768 - 7 - 1000 = 31761; 

**B** 会被分割为 3 个 fragments: 第一个 fragment 占据第一个 block 剩余空间, 共存入 31761 - 7 = 31754, 剩余 65516; 第二个 fragment 占据第二个 block 的全部空间, 存入 32768 - 7 = 32761, 剩余 65516 - 32761 = 32755; 第三个 fragment 占据第三个 block 的起始空间共 7 + 32755 = 32762. 所以最后在第三个 block 剩下 32768 - 32762 = 6 个字节, 这几个字节会被填充 0 作为 trailer. 

**C** 将会被作为 FULL 类型的 record 存储到第四个 block 中. 

----

## Some benefits over the recordio format:


log 文件格式的好处是(总结一句话就是容易划分边界): 

1. 不必进行任何启发式地 resyncing(可以理解为寻找一个 block 的边界) —— 直接跳到下个 block 边界进行扫描即可, 因为每个 block 大小是固定的(32768 个字节, 除非文件尾部的 block 未写满). 如果数据有损坏, 直接跳到下个 block. 这个文件格式的附带好处是, 当一个 log 文件的部分内容作为一个 record 嵌入到另一个 log 文件时(即当一个逻辑 record 分为多个物理 records, 一部分 records 位于前一个 log 文件, 剩下 records 位于下个 log 文件), 我们不会分不清楚. 
2. 在估计出来的边界处做分割(比如为 mapreduce 应用)变得简单了: 找到下个 block 的边界, 如果起始是 MIDDLE 或者 LAST 类型的 record, 则跳过直到我们找到一个 FULL 或者 FIRST record 为止, 就可以在此处做分割, 一部分投递到一个计算任务, 另一部分(直到分界处)投递到另一个计算任务.

## Some downsides compared to recordio format:


log 文件格式的缺点: 

1. 没有打包小的 records. 通过增加一个新的 record 类型可以解决这个问题, 所以这个问题是当前实现的不足而不是 log 格式的缺陷. 
2. 没有压缩. 再说一遍, 这个可以通过增加一个新的 record 类型来解决. 

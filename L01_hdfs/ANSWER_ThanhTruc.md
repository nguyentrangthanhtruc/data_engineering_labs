# BÀI LÀM LAB 01: HDFS
Họ và tên: Nguyễn Trang Thanh Trúc

--------------------------------------
## Progress note
## 1. IP Mapping
- Name: 172.18.0.5:9866 (l01_hdfs-datanode2-1.l01_hdfs_default)
  Hostname: datanode2
- Name: 172.18.0.6:9866 (l01_hdfs-datanode1-1.l01_hdfs_default)
  Hostname: datanode1
- Name: 172.18.0.7:9866 (l01_hdfs-datanode3-1.l01_hdfs_default)
  Hostname: datanode3

## 2. http://localhost:9870 Overview
- Capicity: 2.95TB
- Live nodes: 3
- Status safe mode: Off

## Todo 2.1
root@namenode:/opt/hadoop# hdfs dfs -stat $'Name: %n \nBlockSize: %o \nReplication: %r \nSize: %b bytes' /data/taxi/taxi_zone_lookup.csv
Name: taxi_zone_lookup.csv
BlockSize: 134217728
Replication: 3
Size: 12331 bytes

## 3. Blocks: default vs. small block size
**Small block size**
Replicated Blocks:
 Total size:    49961641 B
 Total files:   1
 Total blocks (validated):      3 (avg. block size 16653880 B)
 Minimally replicated blocks:   3 (100.0 %)

**Default block size**
Replicated Blocks:
 Total size:    49961641 B
 Total files:   1
 Total blocks (validated):      1 (avg. block size 49961641 B)
 Minimally replicated blocks:   1 (100.0 %)


## 4. Replication factor
- Live_repl=2: Do đã giảm từ 3 bản rep xuống thành 2
- root@namenode:/opt/hadoop# hdfs dfs -du -h /data/taxi
	+ 47.6 M  142.9 M  /data/taxi/default_blocks
	+ 47.6 M  95.3 M   /data/taxi/small_blocks
	+ Do giảm số rep, size cũng giảm từ (47.6*3=142.9MB) xuống (47.6*2=95.3MB)

## Todo 4.1
root@namenode:/opt/hadoop# hdfs dfs -D dfs.replication=1 -put /datasets/data/taxi/taxi_zone_lookup.csv /tmp/one_replica.csv
root@namenode:/opt/hadoop# hdfs dfs -ls /tmp/one_replica.csv
-rw-r--r--   1 root supergroup      12331 2026-10-30 14:31 /tmp/one_replica.csv


## 5. Kill a DataNode → under-replication → re-replication
- default & taxi_zone: Under-replicated do yêu cầu 3 bản rep trong khi chỉ còn 2 node còn sống
- small yêu cầu 2 rep nên sau quá trình re-replication cho trạng thái OK


## 6. Safe mode
- Trong bài giảng: docker compose exec namenode hdfs dfsadmin -safemode get     # "Safe mode is ON" for a few seconds
- Thực tế chạy: Safe mode is off. Vì chạy riêng lẻ từng câu lệnh nên namenode có đủ thời gian để tự động tắt safe mode
---------------------------------------------------

## Checkpoint
### 1
- Default block: 128mb
- Default replica: 3
=> 300/128 = 3 blocks (128 + 128 + 44)
=> 3 replica * 3 blocks = 9 blocks replica	
=> 300mb * 3 replica = 900mb raw disk

### 2
 49961641 − 2 * 16777216 = 16407209 bytes < 16MB. nhưng không gây lãng phí vì nó lưu trữ đúng dung lượng của nó chứ không cấp phát sẵn 16gb để trống

### 3
- list of blocks location is saved on nameNode
- when nameNode restarts, all dataNode will make a report that each blocks contain which blocks

### 4
The replication 3-file is not healed because it should have had 3 replica on 3 dataNode. While replica-2 can easily makes a copy and stores at 2 alive dateNode, replica-3 just cant.

### 5
- Status is HEALTHY
- Under replica is just a warning that it needs actions, its not a error when no data is missing or unreadable

### 6
- a node is declared dead after 2 * 5 min + 10 × 3s = 10.5 min
- long timeout can avoid fake death (keep copying data from declared dead node)

### 7
to wait for the block reports correctly

### 8
costs many small blocks causing OOM

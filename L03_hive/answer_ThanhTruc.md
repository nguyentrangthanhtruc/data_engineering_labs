# L03 — Hive: SQL on HDFS, partitioning, file formats, bucketing
## Họ và tên: Nguyễn Trang Thanh Trúc
----------------------------------------
## 1. Start the stack
- Session ID:
  +  hiveserver2-1  | Hive Session ID = 20c20a76-1338-48ed-b30c-33e5e9b82153
  +  hiveserver2-1  | Hive Session ID = 661f9145-203e-4c75-aa39-82c6389d3d90
- Show database:
```
+----------------+
| database_name  |
+----------------+
| default        |
+----------------+
```

## 3. External tables (sql/01_external_tables.sql)
### Todo 1
-- TODO 1: complete the table definition:
--   * use the SerDe 'org.apache.hadoop.hive.serde2.OpenCSVSerde'  (ROW FORMAT SERDE '...')
--   * text file stored in HDFS directory /data/zones
--   * skip the header line  (TBLPROPERTIES ('skip.header.line.count' = '1'))
```
ROW FORMAT SERDE 'org.apache.hadoop.hive.serde2.OpenCSVSerde'
WITH SERDEPROPERTIES (
  "separatorChar" = ",",
  "quoteChar"     = "\""
)
STORED AS TEXTFILE
LOCATION '/data/zones'
TBLPROPERTIES ('skip.header.line.count' = '1');
```
- 3 sample trips
```
+---------------------+---------------------------------+----------------------------------+----------------------------+--------------------------+-----------------------+-------------------------------+-------------------------+-------------------------+-------------------------+------------------------+------------------+--------------------+-----------------------+-------------------------+----------------------------------+-------------------------+---------------------------------+------------------------+
| trips_csv.vendorid  | trips_csv.tpep_pickup_datetime  | trips_csv.tpep_dropoff_datetime  | trips_csv.passenger_count  | trips_csv.trip_distance  | trips_csv.ratecodeid  | trips_csv.store_and_fwd_flag  | trips_csv.pulocationid  | trips_csv.dolocationid  | trips_csv.payment_type  | trips_csv.fare_amount  | trips_csv.extra  | trips_csv.mta_tax  | trips_csv.tip_amount  | trips_csv.tolls_amount  | trips_csv.improvement_surcharge  | trips_csv.total_amount  | trips_csv.congestion_surcharge  | trips_csv.airport_fee  |
+---------------------+---------------------------------+----------------------------------+----------------------------+--------------------------+-----------------------+-------------------------------+-------------------------+-------------------------+-------------------------+------------------------+------------------+--------------------+-----------------------+-------------------------+----------------------------------+-------------------------+---------------------------------+------------------------+
| 2                   | 2024-01-17 16:10:20.0           | 2024-01-17 16:15:01.0            | 1                          | 0.56                     | 1                     | N                             | 237                     | 236                     | 1                       | 6.5                    | 2.5              | 0.5                | 1.3                   | 0.0                     | 1.0                              | 14.3                    | 2.5                             | 0.0                    |
| 2                   | 2024-01-17 16:17:05.0           | 2024-01-17 16:45:58.0            | 2                          | 3.37                     | 1                     | N                             | 236                     | 68                      | 1                       | 26.1                   | 2.5              | 0.5                | 2.0                   | 0.0                     | 1.0                              | 34.6                    | 2.5                             | 0.0                    |
| 2                   | 2024-01-17 16:17:53.0           | 2024-01-17 16:28:00.0            | 1                          | 1.69                     | 1                     | N                             | 143                     | 100                     | 1                       | 12.1                   | 2.5              | 0.5                | 2.0                   | 0.0                     | 1.0                              | 20.6                    | 2.5                             | 0.0                    |
+---------------------+---------------------------------+----------------------------------+----------------------------+--------------------------+-----------------------+-------------------------------+-------------------------+-------------------------+-------------------------+------------------------+------------------+--------------------+-----------------------+-------------------------+----------------------------------+-------------------------+---------------------------------+------------------------+
```
- 3 zones
```
+-------------------+----------------+--------------------------+---------------------+
| zones.locationid  | zones.borough  |        zones.zone        | zones.service_zone  |
+-------------------+----------------+--------------------------+---------------------+
| 1                 | EWR            | Newark Airport           | EWR                 |
| 2                 | Queens         | Jamaica Bay              | Boro Zone           |
| 3                 | Bronx          | Allerton/Pelham Gardens  | Boro Zone           |
+-------------------+----------------+--------------------------+---------------------+
```

- Count of rows
```
+-----------+
| csv_rows  |
+-----------+
| 2964624   |
+-----------+
```

## 4. Partitioned ORC and Parquet tables (sql/02_partitioned_tables.sql)
### To do 2
TODO 2: dynamic partitioning is "strict" by default (at least one static partition value required). Set hive.exec.dynamic.partition.mode so ALL partition values can be dynamic.

    ```SET hive.exec.dynamic.partition.mode = nonstrict;```

### To do 3
 --NULL /* TODO 3: replace NULL by an expression that turns tpep_pickup_datetime into 'yyyy-MM' */
 
       ```date_format(tpep_pickup_datetime, 'yyyy-MM') AS pickup_month```

### Todo 4
-- TODO 4: create trips_parquet: same columns and partitioning as trips_orc, STORED AS PARQUET
--         (TBLPROPERTIES ('parquet.compression' = 'SNAPPY')), and fill it the same way.
```
DROP TABLE IF EXISTS trips_parquet;
CREATE TABLE trips_parquet (
  vendorid INT, tpep_pickup_datetime TIMESTAMP, tpep_dropoff_datetime TIMESTAMP,
  passenger_count BIGINT, trip_distance DOUBLE, ratecodeid BIGINT, store_and_fwd_flag STRING,
  pulocationid INT, dolocationid INT, payment_type BIGINT, fare_amount DOUBLE, extra DOUBLE,
  mta_tax DOUBLE, tip_amount DOUBLE, tolls_amount DOUBLE, improvement_surcharge DOUBLE,
  total_amount DOUBLE, congestion_surcharge DOUBLE, airport_fee DOUBLE
)
PARTITIONED BY (pickup_month STRING)
STORED AS PARQUET
TBLPROPERTIES ('parquet.compression' = 'SNAPPY');

-- Dynamic partitioning: the LAST column of the SELECT feeds the partition column.
INSERT OVERWRITE TABLE trips_parquet PARTITION (pickup_month)
SELECT t.*,
        date_format(tpep_pickup_datetime, 'yyyy-MM') AS pickup_month
FROM trips_csv t;
```
- Result:
```
+---------------+----------+
| pickup_month  |  trips   |
+---------------+----------+
| 2002-12       | 2        |
| 2009-01       | 3        |
| 2023-12       | 10       |
| 2024-01       | 2964606  |
| 2024-02       | 3        |
+---------------+----------+
```
- Compare storage:
| Format | Size (1 M rows) | Size (full month) |
| :--- | :--- | :--- |
| CSV (text) | (88.5 M) | 261.5 M |
| ORC + ZLIB | (17.1 M) | 50.2 M |
| Parquet + Snappy | (20.9 M) | 62.3 M |


## 5. CSV vs ORC vs Parquet (sql/03_compare_formats.sql)
| Query | CSV | ORC | Parquet |
| :--- | :--- | :--- | :--- |
| **Q1: Top pickup zones Jan 2024** | 27.42 s | 14.356 s | 17.811 s |
Q2 (ORC): 11.646s
Q1 top row: zone 132 (JFK) — 145240 trips in the whole sample, avg fare ≈ 59.4. Q2 shows trips per borough (Manhattan ≈ 89.3 %).

## 6. Partition pruning (sql/04_partition_pruning.sql)
### Todo 5
-- TODO 5: write the same count for January 2024, but filter on tpep_pickup_datetime
--         instead of pickup_month, prefixed with EXPLAIN DEPENDENCY. How many partitions are read?

```
EXPLAIN DEPENDENCY
SELECT count(*) FROM trips_orc WHERE tpep_pickup_datetime >= '2024-01-01' AND tpep_pickup_datetime < '2024-02-01';
```

- a. "input_partitions":[{"partitionName":"taxi@trips_orc@pickup_month=2024-01"}]
- b. "input_partitions":[
  {"partitionName":"taxi@trips_orc@pickup_month=2002-12"},
  {"partitionName":"taxi@trips_orc@pickup_month=2009-01"},
  {"partitionName":"taxi@trips_orc@pickup_month=2023-12"},
  {"partitionName":"taxi@trips_orc@pickup_month=2024-01"},
  {"partitionName":"taxi@trips_orc@pickup_month=2024-02"}
]  => Total 5 khác 4 như git của thầy (1M rows vs ~3M rows)
 => Khi lọc trên cột dữ liệu thường (tpep_pickup_datetime), Hive không thể dùng Partition Pruning nên bắt buộc phải mở toàn bộ tất cả các partition hiện có trên đĩa.
- d: hdfs://namenode:8020/user/hive/warehouse/taxi.db/trips_orc/pickup_month=2024-01



-- TODO 6: bucket by pulocationid into 4 buckets, sorted by pulocationid
    CLUSTERED BY (pulocationid) SORTED BY (pulocationid ASC) INTO 4 BUCKETS
----------------------------------------------------------
## Checkpoint
### 1
- Metastore: thông tin cấu trúc của bảng bao gồm: tên bảng, tên/kiểu dữ liệu của các cột, danh sách phân vùng (partition), thuộc tính bảng (TBLPROPERTIES) và đường dẫn dẫn tới thư mục lưu dữ liệu trên HDFS 
- HDFS: Real data file
- DROP TABLE trips_csv: Just delete on Metastore, the original data file still consists on HDFS
- DROP TABLE trips_orc: Drop on both Metastore & HDFS

### 2
- zones need OpenCSVSerde while trips_csv works with ROW FORMAT DELIMITED:
  + zones.csv chứa các chuỗi văn bản nằm trong dấu ngoặc kép / dấu phẩy bên trong giá trị cột
  + row format delimited will cause errors
- Downside:
  + data are forced to string type
  + runs slows and costs more CPU

### 3.
Columnar Layout & Compression & Binary Coding

### 4
- Column Pruning: Chỉ truy vấn 3 cột và bỏ qua các cột còn lại
- Partition Pruning: Chỉ chạy diều kiện pickup_month và bỏ qua phân vùng khác
- Đối chiếu min max để lọc diều kiện và skip

### 5.
- Static Partitioning: tự gõ thủ công
- Dynamic Partitioning: Hive tự động đọc giá trị của cột được chọn và tự khởi tạo các thư mục phân vùng tương ứng trên HDFS
- strict mode: bắt buộc truy vấn phải có ít nhất 1 phân vùng tĩnh => ngăn chặn việc người dùng vô tình chạy một câu lệnh SQL chưa kiểm soát => quá tải cho NameNode

### 6.
- pulocationid + ngày: 265 zones * 365 days =  96,000+ thư mục phân vùng.
- tpep_pickup_datetime: Milions 
=> OOM

### 7
- Partitioning: Dùng cho cột có ít giá trị duy nhất (Low-cardinality), thường xuất hiện trong WHERE
- Bucketing: Dùng cho cột có nhiều giá trị duy nhất (High-cardinality), thường dùng làm khóa JOIN hoặc TABLESAMPLE
- Based on hash func: Bucket ID = hash(column_value) mod N

### 8
- Reason: clock drift/error
- Solve: 
  + Filtering before input to Data Warehouse
  + Marks on another table check and solve


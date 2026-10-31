# L04 — Ingestion from an OLTP database: Sqoop (legacy) → Spark JDBC
## Họ và tên: Nguyễn Trang Thanh Trúc
-----------------------------------------
# Part 1 — Sqoop (legacy) 
## TODO 1.2
- Import
      sqoop import $CONN --table customers \
        --split-by customer_zip_code_prefix -m 4 \
        --null-string '\\N' --null-non-string '\\N' \
        --target-dir /data/olist/sqoop/customers_full --delete-target-dir
- Count rows
      for f in $(hdfs dfs -ls -C /data/olist/sqoop/customers_full/part-m-*); do echo "$f $(hdfs dfs -cat $f | wc -l)"; done
- Result
  + /data/olist/sqoop/customers_full/part-m-00000 2450
  + /data/olist/sqoop/customers_full/part-m-00001 1146
  + /data/olist/sqoop/customers_full/part-m-00002 427
  + /data/olist/sqoop/customers_full/part-m-00003 977
- Explain 
  + Sqoop chia khoảng bằng cách lấy (MAX - MIN) / 4 dựa trên giá trị của cột customer_zip_code_prefix.
  +  Vì mã bưu điện (zip_code) phân bố không đồng đều trong thực tế, nên số lượng dòng rơi vào từng khoảng bị lệch nhau => output không đều dòng 

##  1.5 Incremental lastmodified + merge (new AND changed rows)
- Timeline of first baseline import: 
   --last-value 2026-10-31 11:24:10.654841
- Sau khi second run: Total rows = 5006
    root@sqoop:/work# hdfs dfs -cat /data/olist/sqoop/orders_lastmod/part-r-00000 | wc -l
   >5006
----------------------------------------------------
# Part 2 — Spark JDBC (modern)
## Todo 1
```
orders = jdbc(
    spark,
    dbtable="orders",
    partitionColumn="order_seq",
    lowerBound=str(b.lo),
    upperBound=str(b.hi),
    numPartitions=4,
    fetchsize="1000",
)```
```
## Todo 2
```
by_ts = jdbc(
    spark,
    dbtable="orders",
    partitionColumn="order_purchase_timestamp",
    lowerBound="2017-01-01 00:00:00",
    upperBound="2018-09-01 00:00:00",
    numPartitions=4,
)
```

## Todo 3
```
out = orders.withColumn("purchase_month", F.date_format("order_purchase_timestamp", "yyyy-MM"))
out.write.mode("overwrite").partitionBy("purchase_month").parquet(f"{BASE}/orders")
```
## Todo 4
```
subquery = f"(SELECT * FROM orders WHERE updated_at > '{wm_old}' AND updated_at <= '{wm_new}') AS changed_orders"
changes = jdbc(spark, dbtable=subquery).withColumn("_ingested_at", F.lit(wm_new).cast("timestamp"))
```

## Todo 5
```
window_spec = Window.partitionBy("order_id").orderBy(F.col("updated_at").desc(), F.col("_ingested_at").desc())
current = log.withColumn("rn", F.row_number().over(window_spec)).filter("rn = 1").drop("rn")
```
----------------------------------------------------
# Part 3 — Compare
| Criteria | Sqoop 1.4.7 | Spark JDBC |
| :--- | :--- | :--- |
| **Parallelism knob** | `-m` + `--split-by` | `numPartitions` + `partitionColumn` |
| **How ranges are computed** | `SELECT MIN, MAX` (BoundingValsQuery) | you pass `lowerBound` / `upperBound` (stride only) |
| **Incremental** | `--incremental append/lastmodified`, saved job stores last value | your code: watermark query + state file |
| **Handling updates** | `--merge-key` (extra MR job) | window/ `row_number` dedup, or MERGE into a table format (L11) |
| **Output formats** | text, SequenceFile, Avro (Parquet via Kite is broken on Hadoop 3) | anything Spark writes (Parquet, ORC, Delta/Iceberg) |
| **Transformations during load** | none (SQL `--query` only) | full DataFrame API |
| **Time for full `orders` import (your run)** | 3.30 s | 2.03 s |
| **Status** | Apache Attic (retired 2021) | actively developed |
------------------------------------------------
# Checkpoint
### 1
     SELECT MIN(split_col), MAX(split_col) FROM table
- Calculate Min, max. Then stride = (max-min)/4
- Based on stride to split out

### 2
- Vì mã bưu chính (customer_zip_code_prefix) phân bố không đều trong thực tế
- Nếu customer_id là random: Lỗi, vì Sqoop không biết cách chia khoảng toán học trên chuỗi không phải dạng số/ngày tháng trừ khi cấu hình đặc biệt.
- Nếu customer_id là số increased: Dữ liệu sẽ được chia rất đều giữa các partitions.

### 3
- có đọc, boudary để tính stride chứ không liên quan dến giá trị dữ liệu
- ở partition cuối

### 4.
- chốt now() trước khi đọc => Tránh mất dữ liệu (Data Loss) do race condition
- chỉ lưu watermark khi ghi thành công => Đảm bảo tính At-least-once processing. Nếu tiến trình ghi ra HDFS bị lỗi giữa chừng mà đã lưu watermark mới, lần chạy sau Spark sẽ không đọc lại khoảng dữ liệu đó nữa làm mất dữ liệu.

### 5.
- Captures update: lastmodified
- Capture delete: None of them

### 6.
- Not capture delete
- Miss data from long transactions
- Meanwhile, CDC
  + Note insert, update & **delete**
  + Note based on real commit sequences of transactions

### 7.
- Gây quá tải server
- Connection Exhaustion
- Connection Exhaustion

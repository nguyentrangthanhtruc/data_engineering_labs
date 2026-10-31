# L05 — PySpark basics: DataFrames, Spark SQL and the Spark UI
### Họ và tên: Nguyễn Trang Thanh Trúc
-------------------------------------
## 2. Transformations vs actions

```
from pyspark.sql.functions import col

# Định nghĩa các transformation
filtered_trips = trips.filter(col("trip_distance") > 10) \
                      .withColumn("total_per_mile", col("total_amount") / col("trip_distance")) \
                      .select("tpep_pickup_datetime", "trip_distance", "total_amount", "total_per_mile")

# mở http://localhost:4040

filtered_trips.count()
```
> 228254 rows

## Todo 1

```
null_counts = trips.select([F.count(F.when(F.col(c).isNull(), c)).alias(c) for c in trips.columns])
```
## Todo 2

```
"negative total_amount": F.col("total_amount") < 0,
"trip_distance <= 0 or > 100": (F.col("trip_distance") <= 0) | (F.col("trip_distance") > 100),
"duration_min <= 0 or > 360": (F.col("duration_min") <= 0) | (F.col("duration_min") > 360),
```

## To do 3
```
pu_zones = zones.select(
        F.col("LocationID").alias("PULocationID"),
        F.col("Borough").alias("pu_borough"),
        F.col("Zone").alias("pu_zone")
    )
do_zones = zones.select(
        F.col("LocationID").alias("DOLocationID"),
        F.col("Borough").alias("do_borough"),
        F.col("Zone").alias("do_zone")
    )
```

## To do 4

```
by_borough = enriched.groupBy("pu_borough").agg(
        F.count("*").alias("trips"),
        F.round(F.avg("fare_amount"), 2).alias("avg_fare"),
        F.round(F.avg("tip_pct"), 1).alias("avg_tip_pct"),
        F.round(F.avg("trip_distance"), 2).alias("avg_miles")
    ).orderBy(F.col("trips").desc())
by_borough.show()

by_hour = enriched.groupBy("pickup_hour").agg(F.count("*").alias("trips")).orderBy("pickup_hour")
by_hour.show(24)
```

## To do 5
```
 w_borough = Window.partitionBy("pu_borough").orderBy(F.col("trips").desc())
 top3 = zone_counts.withColumn("rank", F.dense_rank().over(w_borough)).filter(F.col("rank") <= 3)
```

## To do 6

```
 w_running = Window.orderBy("pickup_date").rowsBetween(Window.unboundedPreceding, Window.currentRow)
 daily = daily.withColumn("prev_day_trips", F.lag("trips", 1).over(w_day)) \
                 .withColumn("dod_change_pct", F.round(((F.col("trips") - F.col("prev_day_trips")) / F.col("prev_day_trips")) * 100, 2)) \
                 .withColumn("running_revenue", F.sum("revenue").over(w_running))    
```
### Question:
> Daily trips with lag (day-over-day change) and a running revenue total. Note the WARN "No Partition Defined for Window operation!
> Moving all data to a single partition" — why is it harmless here (31 rows) and dangerous on the raw trips?
- File daily vô hại vì trước đó đã chạy groupBy gom về chỉ còn 31 dòng => bộ nhớ rất nhỏ
- Nguy hiểm khi không partitionBy trên raw file thì sẽ gây OOM vì chỉ có 1 core duy nhất sắp xếp thứ tự

## To do 7

```
enriched.drop("pickup_ts") \
            .repartition("pickup_date") \
            .write.mode("overwrite") \
            .partitionBy("pickup_date") \
            .parquet(OUTPUT)
```

--------------------------------------------------------
## Checkpoint
### 1.
- căn cứ vào cấu hình spark.sql.files.maxPartitionBytes (mặc định là 128 MB) cùng số lượng CPU cores sẵn có trong cluster (h laf 4)
- 4 cores => 4 partitions

### 2.
- transform:
```
trips.filter(F.col("trip_distance") > 10) \
     .withColumn("fare_per_mile", F.col("fare_amount") / F.col("trip_distance")) \
     .select("PULocationID", "trip_distance", "fare_per_mile")
```
- action:
    lazy.count()
- Nhờ cơ chế Lazy Evaluation

### 3.
- jobs: 1
- stages: 2
- Đường ranh giới nằm ngay tại hoạt động Shuffle (Exchange) do phép toán groupBy("pu_borough") gây ra

### 4.
- Đây là đặc trưng của dữ liệu Taxi NYC thực tế. 140,162 dòng này đại diện cho các chuyến đi bị hỏng dữ liệu hoặc là chuyến đi được ghi nhận qua hệ thống tính tiền tự động/các cuốc xe bị hủy (unreported/corrupted trips), dẫn đến toàn bộ 5 trường dữ liệu này NULL cùng một lúc.
- Filler / Impute 
 
### 5.
- partial_count / HashAggregate: gom nhóm và đếm sơ bộ cục bộ ngay trên từng partition (Local Aggregation) trước khi đẩy dữ liệu qua mạng
- MapReduce (L02): gom dữ liệu trên từng máy trước thành vài dòng tổng hợp, giúp giảm thiểu tối đa dung lượng dữ liệu truyền qua mạng

### 6
- bảng zone có dung lượng cực nhỏ => tự động chọn broadcast. Spark sẽ gửi trọn bộ bảng zones đến tất cả các worker nodes => không cần shuffle
- Spark switch to a sort-merge join: khi cả 2 bảng đều có dung lượng lớn hoặc auto broadcast tắt.

## 7
- cache() change:
  + Trong Execution Plan, thêm toán tử InMemoryTableScan.
  +  UI: Dữ liệu được lưu trữ trong bộ nhớ RAM/Disk sẽ xuất hiện trong tab Storage của Spark UI. Các Job phía sau tái sử dụng DataFrame này sẽ không cần chạy lại các bước đọc file hay filter từ đầu nữa
- caching is a bad idea when: data storage is huge or one-used data frame

## 8.
- Nếu repartition theo pickup_date trước, Spark sẽ gom toàn bộ dữ liệu của cùng 1 ngày về đúng 1 partition/task xử lý => 1 file clean
- If not, Small Files Problem => increasing costs


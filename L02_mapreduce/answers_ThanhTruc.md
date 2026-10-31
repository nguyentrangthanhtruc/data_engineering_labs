# L02 — MapReduce with Hadoop Streaming (Python)
## Họ và tên: Nguyễn Trang Thanh Trúc
------------------------------------
## 1. Word count — write and test locally
## Todo 1
> import re
> import sys
>
> WORD = re.compile(r"[a-z][a-z']*")
>
> for line in sys.stdin:
>     words = WORD.findall(line.lower())
>     for word in words:
>         print(f"{word}\t1")


## Todo 2+3
> import sys
>
> current_word, current_count = None, 0
>
>  for line in sys.stdin:
>     word, _, count = line.rstrip("\n").partition("\t")
>     count = int(count)
>
>     if word == current_word:
>         current_count += count
>     else:
>         if current_word is not None:
>             print(f"{current_word}\t{current_count}")
>         current_word = word
>         current_count = count
>
> if current_word is not None:
>     print(f"{current_word}\t{current_count}")

### Question: what happens if you forget sort in the pipe? Try it.
- Reducer đếm theo cơ chế các key giống nhau sẽ đứng liên tiếp nhau và đếm trong 1 lần.
- Nếu không sort theo cơ chế này, mỗi khi gặp 1 key trùng nó sẽ tạo bộ đếm mới thay vì +1 vào số lượng
 ** => đếm sai**


## 2. Word count on YARN
- While it runs, open http://localhost:8088 → the application → ApplicationMaster. When it finishes, read the counters printed at the end (Map input records, Map output records, Reduce input groups …) and check the result:
  + Map input records=48749   (Tổng số dòng dữ liệu đọc vào từ 4 file sách)
  + Map output records=462618   (Tổng số từ phát sinh ra từ Mapper)
  + Reduce input groups=20967   (Unique words)
  + Launched map tasks=4  (Totals 4 files) 
- Kết quả top 5 giống như lúc chạy mapper.py và reduce.py
  + the     25806
  + and     14340
  + of      14113
  + to      12103
  + a       9045 


## 4. Trips per pickup zone (with a map-side join)
## Todo 4: fill the dict: "132" -> "Queens/JFK Airport"
   zones[row["LocationID"].strip()] = f"{row['Borough'].strip()}/{row['Zone'].strip()}"

##  TODO 5: skip malformed lines (too few fields or non-numeric PULocationID)
  > if len(fields) <= PU_COL or not fields[PU_COL].strip().isdigit():
   > sys.stderr.write("reporter:counter:TaxiLab,BAD_RECORDS,1\n")
     > continue

##  TODO 6: print "<zone name or id>\t1"
   > pu_id = fields[PU_COL].strip()
   > zone_name = zones.get(pu_id, pu_id)
   > print(f"{zone_name}\t1")

### Result:
- Manhattan/East Village  59
- Manhattan/Lincoln Square East   55
- Manhattan/Upper West Side South 45


## 5. Combiner vs. no combiner
| Counter | No combiner | With combiner |
| :--- | :--- | :--- |
| **Launched map tasks** | 6 | 6 |
| **Map output records** | 1,000,000 | 1,000,000 |
| **Combine output records** | 0 | 1,382 |
| **Map output materialized bytes** | 31,039,071 | 41,169 |
| **Reduce shuffle bytes** | 31,039,071 | 41,169 |
| **Reduce input records** | 1,000,000 | 1,382 |
| **Wall-clock time**      | ~178s | ~110s |

- **Top Zone thu được:**
  + `Queens/JFK Airport` (59,777)
  + `Manhattan/Midtown Center` (48,750)

- **Nhận xét:**
  + Khi dùng Combiner, lượng dữ liệu truyền qua mạng (**Reduce shuffle bytes**) giảm từ ~31 MB xuống chỉ còn ~41 KB (giảm hơn 99%).
  + Số dòng đưa vào bước Reduce (**Reduce input records**) giảm từ 1,000,000 dòng xuống còn 1,382 dòng giúp giảm đáng kể tải cho Reducer.
  + Thời gian khác so với expected chỉ là do tải hệ thống vào thời điểm đó, không quyết định trường hợp nào tối ưu hơn

------------------------------------------------------------------
## Checkpoint
### 1.
- plays the role of the shuffle & sort: **sort**
- without sort, reducer will misunderstand and count wrongly

### 2.
- Java Reducer được Framework gộp sẵn trong bộ nhớ Java
- Còn Streaming Reducer nhận dòng văn bản phẳng chảy qua STDIN nên script Python phải tự theo dõi điểm chuyển giao giữa các Key

### 3.
- map tasks:
  + Total: 92.8mb
  + 1 block = 16mb
   => 92.8/16 = 6 blocks = **6 maps**
- reduce tasks: -D mapreduce.job.reduces=2  => **2 reduce tasks**

### 4.
-  31,039,071mb / 41,169 = 754 times
- 230 zones * 6 mappers = 1,382 records.

### 5.
- **In the lecture note scenario (Why it wasn't faster on the sample run):**
  In Hadoop Streaming, using a Combiner requires spawning an extra Python sub-process (`python3 reducer.py`). On a single-node Docker setup with fast local network, the CPU overhead of passing data back and forth between Java and Python streaming pipes can sometimes exceed the time saved from network reduction, making the run slightly slower

- **In my actual execution (Why my Combiner run WAS faster: ~1m50s vs ~2m58s):**
  In my actual run, using the Combiner reduced the Shuffle data volume from **31,039,071 bytes (1,000,000 records)** down to just **41,169 bytes (1,382 records)**—a >99.8% reduction. This massive reduction drastically saved Disk I/O, buffer serialization, and sorting time at the Reducer stage. The time saved during Shuffle and Reduce phases (~1m50s vs ~2m58s) significantly outweighed the Python process overhead.

- **When would a Combiner be much faster?**
  A Combiner will show dramatic speedups on large-scale, multi-node physical clusters processing terabytes of data, where physical network bandwidth and Disk I/O are the primary system bottlenecks.

### 6.
- No, because avearge is non-associative
- Mapper and Reducer works like (Sum, count) type

### 7
- Mapside: gửi file danh mục nhỏ taxi_zone_lookup.csv đến tất cả các node làm việc bằng tham số -files, sau đó nạp toàn bộ file này vào một Dictionary trong bộ nhớ RAM của Python
- Limit: danh mục tra cứu quá lớn

### 8.
- Location: ở mục Counters trong đoạn Log tổng kết khi Job chạy xong trên Terminal
- Nếu chỉ vì 1 dòng lỗi nhỏ mà làm sập toàn bộ Job đang chạy dở thì rất lãng phí tài nguyên.

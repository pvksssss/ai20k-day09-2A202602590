# REPORT — Lab Day 9

> Điền **bằng lời và số liệu của chính bạn** (lấy từ đầu ra của `scripts/lab.sh step ...` và từ `report/evidence/*.json`). Bài làm công khai trên GitHub; giảng viên đối chiếu với lịch sử commit và với bài của các bạn khác. Mỗi câu 3 đến 8 dòng là đủ.

| | |
|---|---|
| Họ và tên | Phạm Văn Kiên |
| MSSV (đúng bằng `LAB_STUDENT_ID` trong `.env`) | `2A202602590` |
| GitHub | [pvksssss](https://github.com/pvksssss) |
| Trip dùng cho bài tập sập và chạy lại (xem GUIDE, bước 5) | `trip-02689` |

MSSV được giữ nguyên chuỗi `2A202602590` trong `.env` và toàn bộ lần chạy. Lệnh chọn trip trong GUIDE dùng `int(MSSV)`, không hỗ trợ MSSV có chữ. Riêng bài tập sập/chạy lại, tôi dùng phần chữ số: `2202602590 % 26 = 6`, lấy phần tử thứ 7 trong danh sách trip sắp xếp tăng dần, được `trip-02689`. Đây là quy ước xử lý MSSV có chữ, không phải quy định bổ sung của giảng viên.

## Kết quả cuối

- `dataset_version` của bạn: `5453ca1b9950` (khớp `report/evidence/run_summary.json`).
- Số khung hình train / val / test của bạn: **724 / 148 / 147**, tổng **1.019**, tương ứng **18 / 4 / 4 trip**.
- `report/verify.txt`: tạo bằng `python3 scripts/check_submission.py --save-verify` (phải có `27/27 checks passed`).
- Ảnh chụp trong `report/screenshots/`: `airflow_graph.png`, `minio_lake.png`, `metabase_dashboard.png`, và ảnh bài tập sập/chạy lại (log task đỏ thấy trip của bạn).

## Bước 1 · Bronze

### Câu 1
Liệt kê **ít nhất 4 khác biệt** giữa telemetry nguồn A và nguồn B mà bạn thấy ở bronze. Vì sao pipeline giữ **mỗi nguồn một bảng bronze** và để mọi cột ở dạng chữ, thay vì gộp và ép kiểu ngay?

Bronze của lần chạy có 3.021 dòng A và 3.234 dòng B. A dùng `ts_utc` dạng ISO UTC, B dùng `timestamp_ms` dạng epoch mili giây; A có `speed_mps` (m/s), B có `spd_kmh` (km/h); A dùng `gas`/`brake` dạng tỷ lệ, B dùng `throttle_pct`/`brake_pct` theo phần trăm. Các cột khác cũng đổi tên: `heading_deg`/`hdg`, `lat`/`latitude`, `lon`/`longitude`, `acc_x`/`ax`, `steer`/`steering`. Đồng hồ B chạy sớm 1.500 ms so với video, A không lệch. Hai bảng riêng giữ nguyên schema từng nguồn; chuỗi rỗng và giá trị lỗi còn nguyên để đối soát, chuẩn hóa lại hoặc điều tra. Ép kiểu và gộp ngay ở bronze có thể làm mất dấu lỗi hoặc làm cả lần ingest thất bại. Nạp lại A báo `already present`, vẫn chỉ có 2 video và 26 trip.

## Bước 2 · Silver telemetry

### Câu 2
Pipeline đưa hai nguồn về một schema như thế nào (đơn vị, kiểu thời gian, **độ lệch đồng hồ**)? Bằng chứng nào trong kết quả bước 2 cho thấy đã đúng? Dán bảng quarantine của bạn (số dòng theo từng lý do, từng drive) và giải thích vì sao dòng lỗi vào `rejects` thay vì bị xoá.

Staging đổi tên cột, đưa tốc độ B về m/s bằng chia 3,6, bàn đạp về tỷ lệ bằng chia 100; thời gian B được tính bằng `to_timestamp((timestamp_ms - offset_ms) / 1000.0)`. A đọc timestamp ISO bằng `timestamptz`. Sau chuẩn hóa, telemetry B bắt đầu `2025-10-08 14:30:00.009 UTC`, chỉ sau video 9 ms thay vì 1.509 ms; kiểm tra clock offset đạt. Tốc độ tối đa làm tròn của A/B là 71/100 km/h.

| Drive | duplicate | speed_missing | speed_out_of_range | Tổng cách ly | Dòng sạch |
|---|---:|---:|---:|---:|---:|
| drive_a | 20 | 30 | 3 | 53 | 2.968 |
| drive_b | 20 | 30 | 3 | 53 | 3.181 |

Đối soát: `2968 + 53 = 3021`, `3181 + 53 = 3234`; cả 7 kiểm tra module 2 đạt. `rejects` giữ dòng lỗi cùng lý do để truy vết, sửa nguồn và tái xử lý, thay vì mất dữ liệu âm thầm khi xóa.

### Câu 3
Vì sao test `max_gap_ms` là `warn` còn `in_range` là `error`? Kết quả thí nghiệm `break-gate` của bạn (`break_gate.json`): test nào bắt được, bao nhiêu dòng, và điều đó cho biết gì về vai trò của "cổng chất lượng"?

`max_gap_ms` là `warn` vì khoảng mất tín hiệu cần được theo dõi nhưng phần dữ liệu còn lại vẫn dùng được; downstream còn gắn `sync_lost` và lọc các khung không đồng bộ. `in_range` là `error` vì tốc độ ngoài 0–70 m/s vi phạm miền dữ liệu được phép, cần chặn trước khi vào gold. Trong `break_gate.json`, nới luật làm sạch lên 1.000 m/s khiến test `in_range_silver_telemetry_speed_mps__70__0` bắt được **6 dòng**; tổng kết `PASS=5 WARN=1 ERROR=1 SKIP=0`. Khôi phục luật gốc được `PASS=6 WARN=1 ERROR=0 SKIP=0`. Cổng chất lượng kiểm tra độc lập với luật biến đổi: biến đổi chạy được không có nghĩa đầu ra đã hợp lệ.

## Bước 3 · Silver frames

### Câu 4
Dựa vào `idempotency.json`: số dòng trước và sau hai lần chạy lại, và khi ghi kiểu `append`. Vì sao Delta `overwrite` theo partition (một trip) an toàn hơn xoá rồi ghi lại, nhất là khi giữa chừng có lỗi?

Thí nghiệm dùng `trip-02689` có **23 khung**. Trước và sau hai lần chạy lại, toàn bảng vẫn **1.246 dòng**, dấu vân tay đều `6c7480dfde7e`; `unchanged=true`. Bảng thử ghi append hai lần có **46 dòng**, nhân đôi một trip. Delta ghi lại theo predicate `trip_id` bằng một commit nguyên tử, nên người đọc thấy snapshot trước hoặc sau commit; lỗi trước commit không làm mất snapshot cũ như thao tác xóa rồi ghi rời rạc. Trong mã này, partition vật lý là `drive_id`, còn phạm vi thay thế logic là một `trip_id`. Tính nguyên tử đó áp dụng cho bảng Delta; việc tải JPEG lên object storage vẫn là các thao tác riêng.

### Câu 5
Khung hình **đen** và khung hình **tối** khác nhau thế nào, pipeline xử lý khác nhau ra sao, bạn có đồng ý không? Dựa vào `threshold.json`: nếu chọn ngưỡng 0, 2, 6 thì bao nhiêu khung bị coi là trùng ở mỗi drive? Bạn chọn ngưỡng nào, vì sao, và ngưỡng đó phụ thuộc vào tốc độ xe thế nào?

| dHash khác tối đa | số khung trùng, drive A | số khung trùng, drive B |
|---|---|---|
| 0 | 61 | 8 |
| 2 | 149 | 53 |
| 6 | 363 | 244 |

Khung đen có độ sáng trung bình `<12`; khung tối nằm trong `[12,40)`. A/B có **6/4 khung đen** bị loại và **4/61 khung tối** được gắn cờ; cờ tối tự nó không loại khung, nhưng khung đó vẫn có thể bị loại vì trùng, mờ hoặc mất đồng bộ. Giữ cảnh tối là hợp lý vì bóng râm/đường hầm là điều kiện thật cần học. Tôi chọn ngưỡng dHash **2** như pipeline: ngưỡng 0 bỏ sót biến đổi nhỏ do nén, còn ngưỡng 6 đánh dấu tới 363/602 khung A và 244/644 khung B, có nguy cơ bỏ quá nhiều cảnh khác nhau. Xe đi chậm hoặc đứng yên làm ảnh liên tiếp giống nhau; xe nhanh đổi cảnh mạnh nên cần đánh giá ngưỡng cùng tốc độ và tần suất lấy mẫu, không suy ra camera hỏng chỉ từ dHash.

## Bước 4 · Gold

### Câu 6
Dựa vào `leakage.json`: chia theo khung hình làm bao nhiêu trip bị chia cắt, so với chia theo trip? Vì sao điều này làm điểm kiểm thử đẹp một cách giả tạo?

`leakage.json` ghi **1.019 khung của 26 trip**. Khi chia ngẫu nhiên theo frame, **26 trip** nằm ở nhiều split và cả **26 trip** có ảnh ở đồng thời train/test; chia theo trip trong gold cho **0 trip** bị chia cắt. Khung trong cùng chuyến gần nhau về cảnh, thời tiết và góc nhìn, nên chia theo frame có thể cho mô hình nhận ra cảnh đã học, tạo điểm test quá lạc quan về khả năng tổng quát. Chia theo trip giảm kiểu rò rỉ này, nhưng vẫn cần kiểm tra lặp tuyến đường giữa các trip nếu dùng cho đánh giá thực tế.

### Câu 7
Vì sao `dataset_version` là **băm nội dung** chứ không phải ngày giờ hay số đếm? Hai lần chạy liên tiếp cho kết quả gì? Vì sao `dataset_version` của bạn **khác** bạn bè dù cùng dữ liệu (liên hệ MSSV)?

Phiên bản là 12 ký tự đầu của SHA-256 trên danh sách `frame_id|split` đã sắp xếp; cùng tập frame và split sẽ ra cùng version dù chạy giờ khác hoặc số lần khác. Bước 4 và export lặp lại đều cho `5453ca1b9950`. MSSV đầy đủ được nối vào `trip_id` trước khi băm MD5 để xếp trip trong từng drive, vì thế đổi MSSV có thể đổi thành viên từng split. Thử `2A202602590-demo` cho `d7720a0fff42`, số train/val/test là **698/157/164**; khôi phục `2A202602590` lại được **724/148/147** và version cũ. Lưu ý đây là băm danh sách frame và split, không phải băm toàn bộ pixel/telemetry; cần thêm checksum nguồn và lineage để xác định đầy đủ dữ liệu.

### Câu 8
Đọc phần `lineage` trong `manifest.json` của dataset bạn. Trả lời "dataset này được tạo từ dữ liệu nào?" bằng chính nội dung đó. Nếu một người hỏi 3 tháng sau, thông tin nào giúp tái tạo lại đúng dataset này?

Manifest đã được lưu tại `report/evidence/manifest.json`. Dataset dùng video A có SHA-256 `636ba1a98dfc5ba665d1c935e1d2064e69f4df360ff9ffbd15795957d5e7e9aa`, B có SHA-256 `bfa62703f645ff77a8578ae3fc74a00881140d38a404d43c8e2af265bbee9fdc`, cùng hai bảng telemetry và metadata trips ở bronze. Lineage ghi version Delta của từng bảng nguồn và bảng silver, cho phép truy đúng snapshot đã dùng. Muốn tái tạo sau ba tháng cần giữ file nguồn khớp checksum, snapshot Delta và file dữ liệu của chúng, mã nguồn/phiên bản thư viện, ngưỡng xử lý, tốc độ lấy mẫu và MSSV `2A202602590`. Không được vacuum mất snapshot cần truy lại; nếu chỉ còn dữ liệu thô thì chạy lại với đúng cấu hình và đối chiếu manifest/version.

## Bước 5 · Điều phối và phục vụ

### Câu 9
Mô tả chính xác điều xảy ra khi trip của bạn sập rồi chạy lại: bao nhiêu task chuyển đỏ, bao nhiêu task `upstream_failed`, bạn clear gì, dữ liệu có nhân đôi không, `dataset_version` có đổi không? Bằng chứng (ảnh, số) là gì?

Trong run `kien_failure_recovery`, task mapped `silver_frames[6]` của `trip-02689` đỏ đúng **1 lần** ở attempt 1; log ghi `simulated crash while extracting trip-02689`. Vì task này là upstream của chuỗi dbt nên **7 task downstream** chuyển `upstream_failed` và run tạm thời failed. Tôi chỉ chọn task đỏ map index 6 trong Airflow để clear/retry; khi Airflow cần tiếp tục scheduler, các downstream đã ở trạng thái upstream_failed được đưa về queued theo cùng run, không chạy lại các trip đã xanh. Lần retry của map index 6 là attempt 2 và thành công; sau đó toàn bộ run có **37/37 task instance success**, trạng thái DAG success. Bằng chứng là `airflow_failure.json`, `airflow_recovery_final.json`, `report/logs/airflow_crash.txt`, ảnh `airflow_crash_log.jpg` và `airflow_failed_graph.jpg`. Sau recovery verify vẫn **27/27**, gold vẫn **1.019 frame**, `dataset_version` vẫn `5453ca1b9950`; Delta overwrite theo trip giữ một partition, không nhân đôi dữ liệu.

### Câu 10
Nâng từ 2 drive lên **500 xe mỗi ngày**: ba thành phần nào của pipeline này sẽ vỡ đầu tiên, và bạn thay bằng gì (nối với slide 12, 22, 23)?

Ba ứng viên nghẽn cần đo tải trước: (1) FFmpeg giải mã và tính tín hiệu ảnh trên một container; DAG còn giới hạn `silver_frames` chạy một task cùng lúc, nên hàng đợi sẽ dài. Tách worker theo trip, dùng Celery/Kubernetes và autoscale theo backlog. (2) DuckDB cùng file cục bộ và các bước dbt/publish tập trung sẽ chạm RAM, I/O và giới hạn một máy; chuyển biến đổi lớn sang Spark/engine phân tán trên Delta, xử lý incremental theo partition. (3) MinIO một node và ổ cục bộ chịu ingest video, đọc lặp để trích frame, nhiều object JPEG nhỏ; chuyển sang object storage phân tán/managed, quản lý dung lượng, compaction và tối ưu cách đọc video. Đây là dự đoán kiến trúc, chưa phải kết luận benchmark 500 xe. Cần đo throughput, thời gian chờ, RAM và I/O trước khi tăng worker; chỉ tăng số task có thể làm nghẽn storage nặng hơn.

## Câu hỏi riêng của bạn trên Metabase

Câu SQL bạn thêm vào dashboard và điều nó cho biết:

**Trip nào có tỷ lệ khung bị loại cao nhất, cần kiểm tra chất lượng trước?** SQL lưu riêng tại `report/metabase_question.sql`:

```sql
SELECT drive_id, trip_id, lighting, conditions, frames, frames_kept,
       frames - frames_kept AS frames_dropped,
       ROUND(100.0 * (frames - frames_kept) / NULLIF(frames, 0), 2) AS dropped_pct,
       avg_speed_kmh, split
FROM marts.mart_trip_summary
ORDER BY dropped_pct DESC, frames_dropped DESC, trip_id;
```

Kết quả: `trip-04622` của A bị loại **34/60 khung (56,67%)**, cao nhất; tiếp theo `trip-03904` loại **11/22 (50%)**, rồi `trip-02086` loại **28/60 (46,67%)**. Bảng giúp ưu tiên xem lại trip có tỷ lệ loại cao, nhưng không tự chứng minh nguyên nhân camera hỏng: cần xem cờ và ảnh cụ thể. Dùng `frames - frames_kept` tránh cộng trùng các cờ lỗi có thể cùng xuất hiện trên một frame.

## Nhìn lại

Điều bạn thấy bất ngờ nhất, điều gì còn chưa rõ, và một lỗi dữ liệu mà bạn nghĩ pipeline **chưa** bắt được (nếu có). Tối đa 8 dòng.

Điểm đáng chú ý là drive B có 61 khung tối nhưng không nên bỏ tất cả: đây có thể là điều kiện đường thật cần giữ trong dataset. Thí nghiệm ngưỡng cho thấy chỉ tăng dHash từ 2 lên 6 đã tăng số khung bị coi là trùng từ 53 lên 244 ở B. Tôi chưa có kiểm tra ảnh gán nhãn thủ công để khẳng định ngưỡng 2 tối ưu. Pipeline hiện chưa kiểm tra GPS nhảy bất hợp lý hoặc camera lệch góc nhưng vẫn sáng và nét. Tôi cũng thấy version chỉ băm frame ID/split, nên việc version không đổi chưa đủ đảm bảo pixel và nhãn không đổi. Khi chạy trên Windows cần chú ý LF và cổng 8080 đã bị ứng dụng khác chiếm.

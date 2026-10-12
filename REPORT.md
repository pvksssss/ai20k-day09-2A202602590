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

- `dataset_version` của bạn: `<điền>`  (phải khớp `report/evidence/run_summary.json`)
- Số khung hình train / val / test của bạn: `<điền>`
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

(trả lời ở đây)

### Câu 7
Vì sao `dataset_version` là **băm nội dung** chứ không phải ngày giờ hay số đếm? Hai lần chạy liên tiếp cho kết quả gì? Vì sao `dataset_version` của bạn **khác** bạn bè dù cùng dữ liệu (liên hệ MSSV)?

(trả lời ở đây)

### Câu 8
Đọc phần `lineage` trong `manifest.json` của dataset bạn. Trả lời "dataset này được tạo từ dữ liệu nào?" bằng chính nội dung đó. Nếu một người hỏi 3 tháng sau, thông tin nào giúp tái tạo lại đúng dataset này?

(trả lời ở đây)

## Bước 5 · Điều phối và phục vụ

### Câu 9
Mô tả chính xác điều xảy ra khi trip của bạn sập rồi chạy lại: bao nhiêu task chuyển đỏ, bao nhiêu task `upstream_failed`, bạn clear gì, dữ liệu có nhân đôi không, `dataset_version` có đổi không? Bằng chứng (ảnh, số) là gì?

(trả lời ở đây)

### Câu 10
Nâng từ 2 drive lên **500 xe mỗi ngày**: ba thành phần nào của pipeline này sẽ vỡ đầu tiên, và bạn thay bằng gì (nối với slide 12, 22, 23)?

(trả lời ở đây)

## Câu hỏi riêng của bạn trên Metabase

Câu SQL bạn thêm vào dashboard và điều nó cho biết:

(trả lời ở đây)

## Nhìn lại

Điều bạn thấy bất ngờ nhất, điều gì còn chưa rõ, và một lỗi dữ liệu mà bạn nghĩ pipeline **chưa** bắt được (nếu có). Tối đa 8 dòng.

(trả lời ở đây)

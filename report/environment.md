# Môi trường thực hiện

- Sinh viên: Phạm Văn Kiên, MSSV `2A202602590`.
- Windows, Docker Desktop 4.41.2, Docker Engine 28.1.1, Linux containers.
- Dữ liệu từ `dataset/day09-data.zip`; `check_data.py` xác nhận 6/6 file khớp bản chuẩn.
- Máy có khoảng 8 GB RAM vật lý, Docker đang có khoảng 4 GB RAM. Đây là điểm chưa đạt khuyến nghị cấp ít nhất 6 GB RAM của đề. Metabase được bật sau phần xử lý pipeline để giảm tải.
- Cổng 8080 đã thuộc ứng dụng có sẵn. Airflow dùng `http://localhost:8081`, thông qua override cục bộ trong `work/compose.local.yml`; file `docker-compose.yml` gốc không thay đổi nội dung.
- Git trên Windows đã checkout CRLF. Khôi phục LF để chạy shell script trong Linux và để hash các file bảo vệ khớp đề. `.gitattributes` giữ LF cho các file văn bản liên quan.
- Repo nộp riêng: https://github.com/pvksssss/ai20k-day09-2A202602590. Bản Git độc lập nằm tại `work/submission/`, không kế thừa `.git` hay lịch sử commit của đề; dữ liệu và môi trường chạy không được đưa lên repo.
- Riêng phép chọn trip thử lỗi: lấy các chữ số của MSSV, `2202602590 % 26 = 6`, ứng với `trip-02689` trong danh sách đã sắp xếp. MSSV dùng chia dataset vẫn là chuỗi đầy đủ `2A202602590`.

## Cấu hình cổng cục bộ khi bị trùng 8080

Tạo `work/compose.local.yml`:

```yaml
services:
  airflow:
    ports: !override
      - "8081:8080"
```

Trên Windows, thêm vào `.env`: `COMPOSE_FILE=docker-compose.yml;work/compose.local.yml`.
Trên Linux/macOS dùng dấu `:` phân cách hai đường dẫn. Nếu 8080 trống, chỉ cần cấu hình gốc của đề.

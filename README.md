# Recruitment Data Project (Pig → Spark → Visualization)

Project này xử lý dữ liệu tuyển dụng (CSV) bằng **Pig** (Hadoop) để tạo các bảng tổng hợp, sau đó dùng **Spark (PySpark)** chạy bằng **Docker** để:

- Đọc các output dạng `part-*` của Pig
- Chuẩn hoá + tổng hợp lại thành các bảng “viz-ready” dạng **mỗi dataset 1 file CSV**
- Tạo các hình (PNG) phục vụ báo cáo/EDA

## TL;DR (chạy nhanh)

1) Chạy Spark job (Docker):

```bash
docker compose up --no-build --pull always
```

2) Kết quả sẽ nằm ở:

- `output/viz/*.csv` (bảng tổng hợp cuối cùng)

3) Vẽ biểu đồ bằng Python:

```bash
E:/BigData/recruitment-data-project/.venv/Scripts/python.exe viz/make_figures.py
```

- Hình nằm ở `output/figures/*.png`

> Nếu chỉ cần các bảng tổng hợp (CSV) thì có thể dừng ở bước (1).

---

## Cấu trúc thư mục

- `vietnamworks_detailed_jobs.csv`: dữ liệu đầu vào (raw)
- `*.pig`: script Pig (Hadoop) để tạo output ban đầu
- `output/`: các output Pig và các output đã xử lý
  - `output/*/part-*`: kết quả Pig (TSV/tab-separated)
  - `output/viz/*.csv`: bảng tổng hợp cuối cùng (1 file / dataset)
  - `output/figures/*.png`: biểu đồ xuất từ Python
- `spark_explore_output.py`: script Spark để đọc Pig output + export viz datasets
- `docker-compose.yml`: chạy Spark job trong Docker (`apache/spark:3.5.0`)
- `viz/make_figures.py`: script Python vẽ biểu đồ từ `output/viz/*.csv`

## Prerequisites

- Windows + Docker Desktop (Compose v2)
- (Tuỳ chọn) Python 3.11+ nếu muốn chạy `viz/make_figures.py`

## Chạy pipeline bằng Docker (Spark)

Chạy:

```bash
docker compose up --no-build --pull always
```

Sau khi chạy xong, container sẽ exit code 0.
Dọn container/network:

```bash
docker compose down
```

### Outputs (viz-ready CSV)

Các file được tạo trong `output/viz/`:

- `industry_total.csv`: tổng job_count theo ngành
- `industry_total_known.csv`: tổng job_count theo ngành (đã loại nhóm không phù hợp)
- `industry_by_location.csv`: job_count theo (province, industry)
- `province_total.csv`: tổng job_count theo tỉnh
- `province_industry_share.csv`: (province, industry) + `share` (tỷ trọng trong tỉnh)
- `top5_industries_by_province.csv`: top ngành theo từng tỉnh
- Requirement totals:
  - `requirement_experience_total.csv`
  - `requirement_education_total.csv`
  - `requirement_employment_type_total.csv`
  - `requirement_skill_total_top500.csv`

> Trong các bảng viz quan trọng, các bucket như `UNKNOWN` và các biến thể chứa `KHÔNG HIỂN THỊ` được lọc bỏ để phù hợp visualization.

## Vẽ biểu đồ bằng Python

Cài package (nếu chưa có):

```bash
E:/BigData/recruitment-data-project/.venv/Scripts/python.exe -m pip install pandas matplotlib seaborn
```

Chạy:

```bash
E:/BigData/recruitment-data-project/.venv/Scripts/python.exe viz/make_figures.py
```

Outputs:

- `output/figures/industry_total_known_top25.png`
- `output/figures/province_total_top25.png`
- `output/figures/province_industry_share_heatmap.png`
- `output/figures/requirement_experience_total.png`
- `output/figures/requirement_education_total.png`
- `output/figures/requirement_employment_type_total.png`
- `output/figures/requirement_skill_top30.png`

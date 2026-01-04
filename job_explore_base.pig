/*
 * SCRIPT: JOB EXPLORE BASE (Data Profiling & Preparation)
 * Mục tiêu: 
 * 1. Parse JSON lấy 9 trường thông tin quan trọng.
 * 2. Sinh ID số nguyên (mahout_id) để dùng cho thuật toán gợi ý sau này.
 * 3. Thống kê nhanh xem dữ liệu bị thiếu/rỗng ở đâu.
 */

-- ============================================================
-- 1. LOAD DỮ LIỆU (RAW)
-- ============================================================
raw_data = LOAD '/user/maria_dev/data/vietnamworks/vietnamworks_detailed_jobs.jsonl'
           USING TextLoader() AS (line:chararray);

-- ============================================================
-- 2. LÀM SẠCH BOM (\uFEFF) + KÝ TỰ LẠ
-- QUAN TRỌNG: nếu không bước này => raw_title = NULL hết
-- ============================================================
clean_lines = FOREACH raw_data GENERATE
    REPLACE(line, '\\uFEFF', '') AS line;

-- ============================================================
-- 3. TRÍCH XUẤT DỮ LIỆU (EXTRACT)
-- ============================================================
parsed = FOREACH clean_lines GENERATE 
    REGEX_EXTRACT(line, '.*"Tên công việc"\\s*:\\s*"([^"]*)".*', 1) AS raw_title,
    REGEX_EXTRACT(line, '.*"Ngành nghề"\\s*:\\s*"([^"]*)".*', 1) AS raw_ind,
    REGEX_EXTRACT(line, '.*"Lĩnh vực"\\s*:\\s*"([^"]*)".*', 1) AS raw_sec,
    REGEX_EXTRACT(line, '.*"Địa điểm"\\s*:\\s*"([^"]*)".*', 1) AS raw_loc,
    REGEX_EXTRACT(line, '.*"Số năm kinh nghiệm"\\s*:\\s*"([^"]*)".*', 1) AS raw_exp,
    REGEX_EXTRACT(line, '.*"Yêu cầu công việc"\\s*:\\s*"([^"]*)".*', 1) AS raw_req,
    REGEX_EXTRACT(line, '.*"Kỹ năng"\\s*:\\s*"([^"]*)".*', 1) AS raw_skill,
    REGEX_EXTRACT(line, '.*"Trình độ học vấn"\\s*:\\s*"([^"]*)".*', 1) AS raw_edu,
    REGEX_EXTRACT(line, '.*"Loại hình làm việc"\\s*:\\s*"([^"]*)".*', 1) AS raw_type;

-- ============================================================
-- 4. LỌC RÁC
-- ============================================================
valid_jobs = FILTER parsed BY
    (raw_title IS NOT NULL) AND (TRIM(raw_title) != '');

-- ============================================================
-- 5. SINH ID ĐỒNG BỘ (MAHOUT ID)
-- ============================================================
ranked_jobs = RANK valid_jobs;

-- ============================================================
-- 6. JOB BASE FINAL
-- ============================================================
job_base_final = FOREACH ranked_jobs GENERATE
    rank_valid_jobs AS mahout_id:long,
    raw_title,
    raw_ind,
    raw_sec,
    raw_loc,
    raw_exp,
    raw_req,
    raw_skill,
    raw_edu,
    raw_type;

STORE job_base_final
INTO '/user/maria_dev/output/job_base_clean'
USING PigStorage('\t');

-- ============================================================
-- 7. DATA PROFILING
-- ============================================================
grp_all = GROUP job_base_final ALL;

stats = FOREACH grp_all {
    total_recs = COUNT(job_base_final);

    valid_sec   = FILTER job_base_final BY raw_sec   IS NOT NULL AND raw_sec   != 'Không hiển thị';
    valid_skill = FILTER job_base_final BY raw_skill IS NOT NULL AND raw_skill != 'Không hiển thị';
    valid_req   = FILTER job_base_final BY raw_req   IS NOT NULL AND raw_req   != 'Không hiển thị';

    GENERATE
        'Data_Quality_Report' AS report_name,
        total_recs            AS total_jobs,
        COUNT(valid_sec)      AS jobs_with_sector,
        COUNT(valid_skill)    AS jobs_with_skills,
        COUNT(valid_req)      AS jobs_with_requirement;
};

STORE stats
INTO '/user/maria_dev/output/data_quality_stats'
USING PigStorage('\t');

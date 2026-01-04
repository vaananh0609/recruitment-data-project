-- ============================================================
-- SCRIPT: INDUSTRY ANALYSIS
-- Mục tiêu:
-- 1. Chuẩn hóa Ngành nghề / Lĩnh vực / Tên công việc
-- 2. Trả lời: "Ngành nào tuyển nhiều?"
-- 3. Sẵn sàng JOIN với Location / Mahout sau này
-- ============================================================


job_base = LOAD '/user/maria_dev/output/job_base_clean'
USING PigStorage('\t')
AS (
    job_id:long,
    raw_title:chararray,
    raw_ind:chararray,
    raw_sec:chararray,
    raw_loc:chararray,
    raw_exp:chararray,
    raw_req:chararray,
    raw_skill:chararray,
    raw_edu:chararray,
    raw_type:chararray
);
log_location = LOAD '/user/maria_dev/output/log_location'
USING PigStorage('\t')
AS (
    job_id:long,
    province_id:int,
    province_name:chararray,
    is_overseas:int,
    is_unknown:int
);
job_base_cleaned = FOREACH job_base GENERATE
    job_id,
    raw_title,
    raw_ind,
    raw_sec,
    raw_loc,
    raw_exp,
    raw_req,
    raw_skill,
    raw_edu,
    raw_type,
    (raw_ind IS NULL OR TRIM(raw_ind)=='' OR raw_ind=='Không hiển thị'
        ? 'UNKNOWN'
        : TRIM(raw_ind)
    ) AS raw_ind_clean;

segment_slash = FOREACH job_base_cleaned GENERATE
    job_id,
    raw_title,
    raw_sec,
    raw_loc,
    raw_exp,
    raw_req,
    raw_skill,
    raw_edu,
    raw_type,
    raw_ind_clean,
    (raw_ind_clean == 'UNKNOWN'
        ? 'UNKNOWN'
        : (raw_ind_clean MATCHES '([^/]+).*'
            ? REGEX_EXTRACT(raw_ind_clean, '([^/]+).*', 1)
            : raw_ind_clean
        )
    ) AS segment_slash;

segment_comma = FOREACH segment_slash GENERATE
    job_id,
    raw_title,
    raw_sec,
    raw_loc,
    raw_exp,
    raw_req,
    raw_skill,
    raw_edu,
    raw_type,
    segment_slash,
    (segment_slash == 'UNKNOWN'
        ? 'UNKNOWN'
        : (segment_slash MATCHES '([^,]+).*'
            ? REGEX_EXTRACT(segment_slash, '([^,]+).*', 1)
            : segment_slash
        )
    ) AS segment_comma;

segment_gt = FOREACH segment_comma GENERATE
    job_id,
    raw_title,
    raw_sec,
    raw_loc,
    raw_exp,
    raw_req,
    raw_skill,
    raw_edu,
    raw_type,
    segment_comma,
    (segment_comma == 'UNKNOWN'
        ? 'UNKNOWN'
        : (segment_comma MATCHES '([^>]+).*'
            ? REGEX_EXTRACT(segment_comma, '([^>]+).*', 1)
            : segment_comma
        )
    ) AS segment_gt;

-- 2. CLEAN & NORMALIZE INDUSTRY

industry_norm = FOREACH segment_gt GENERATE
    job_id,

    -- INDUSTRY
    (segment_gt == 'UNKNOWN'
        ? 'UNKNOWN'
        : (TRIM(REPLACE(segment_gt, '\\s+', ' ')) == ''
            ? 'UNKNOWN'
            : UPPER(TRIM(REPLACE(segment_gt, '\\s+', ' ')))
        )
    ) AS industry_name,

    -- FIELD (DÙNG BỔ TRỢ)
    (raw_sec IS NULL OR TRIM(raw_sec)=='' OR raw_sec=='Không hiển thị'
        ? 'UNKNOWN'
        : UPPER(TRIM(raw_sec))
    ) AS field_name,

    -- JOB TITLE (GIỮ LẠI ĐỂ PHÂN TÍCH SAU)
    UPPER(TRIM(raw_title)) AS job_title,

    raw_loc AS location_raw;

industry_loc_joined = JOIN industry_norm BY job_id LEFT OUTER, log_location BY job_id;

industry_loc_ready = FOREACH industry_loc_joined GENERATE
    industry_norm::industry_name AS industry_name,
    industry_norm::job_id AS job_id,
    industry_norm::job_title AS job_title,
    industry_norm::location_raw AS location_raw,
    (log_location::province_name IS NULL ? 'UNKNOWN' : log_location::province_name) AS location_normalized;

-- 3. THỐNG KÊ NGÀNH TUYỂN NHIỀU (CORE OUTPUT)

grp_industry = GROUP industry_norm BY industry_name;

industry_total = FOREACH grp_industry GENERATE
    group AS industry_name,
    COUNT(industry_norm) AS total_jobs;

industry_sorted = ORDER industry_total BY total_jobs DESC;

total_summary = FOREACH (GROUP industry_sorted ALL) GENERATE
    'TONG_TAT_CA' AS industry_name,
    SUM(industry_sorted.total_jobs) AS total_jobs;

industry_with_total = UNION industry_sorted, total_summary;

industry_with_flags = FOREACH industry_with_total GENERATE
    industry_name,
    total_jobs,
    (industry_name == 'TONG_TAT_CA' ? 1 : 0) AS is_summary;

industry_total_final = FOREACH (ORDER industry_with_flags BY is_summary ASC, total_jobs DESC) GENERATE
    industry_name,
    total_jobs;

rmf /user/maria_dev/output/industry_total;

STORE industry_total_final
INTO '/user/maria_dev/output/industry_total'
USING PigStorage('\t');
-- 4. (NÂNG CAO) NGÀNH × ĐỊA ĐIỂM
-- DÙNG CHO PHÂN TÍCH "NGÀNH NÀO Ở ĐÂU TUYỂN NHIỀU"

grp_industry_location = GROUP industry_loc_ready BY (location_normalized, industry_name);

industry_by_location = FOREACH grp_industry_location GENERATE
    group.location_normalized AS location_name,
    group.industry_name AS industry_name,
    COUNT(industry_loc_ready) AS total_jobs;

industry_loc_sorted = ORDER industry_by_location BY location_name ASC, industry_name ASC;

rmf /user/maria_dev/output/industry_by_location;

STORE industry_loc_sorted
INTO '/user/maria_dev/output/industry_by_location'
USING PigStorage('\t');
REGISTER /usr/hdp/current/pig-client/piggybank.jar;

grp_job_title = GROUP industry_loc_ready BY (location_normalized, job_title);

job_title_stats = FOREACH grp_job_title GENERATE
    group.location_normalized AS location_name,
    group.job_title AS job_title,
    COUNT(industry_loc_ready) AS total_jobs;

job_title_sorted = ORDER job_title_stats BY location_name ASC, job_title ASC;

job_title_for_storage = FOREACH job_title_sorted GENERATE
    location_name,
    job_title,
    total_jobs,
    (REPLACE(REPLACE(UPPER(TRIM(location_name)), '\\s+', '_'), '[^A-Z0-9_-]', '') == ''
        ? 'UNKNOWN'
        : REPLACE(REPLACE(UPPER(TRIM(location_name)), '\\s+', '_'), '[^A-Z0-9_-]', '')
    ) AS location_slug;

rmf /user/maria_dev/output/industry_province_titles;

STORE job_title_for_storage
INTO '/user/maria_dev/output/industry_province_titles'
USING org.apache.pig.piggybank.storage.MultiStorage(
    '/user/maria_dev/output/industry_province_titles',
    '3',
    '\t'
);

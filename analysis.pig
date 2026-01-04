-- ============================================================
-- SCRIPT: REQUIREMENT ANALYSIS
-- Mục tiêu: xác định những phần yêu cầu phổ biến theo từng ngành chuẩn hóa
-- Output: exp_total, skill_total, req_total, edu_total, type_total (có industry_id/industry_name)
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

job_clean = FOREACH job_base GENERATE job_id, raw_exp, raw_req, raw_skill, raw_edu, raw_type;

job_base_cleaned = FOREACH job_base GENERATE job_id, raw_ind;

segment_slash = FOREACH job_base_cleaned GENERATE
    job_id,
    raw_ind,
    (raw_ind IS NULL OR TRIM(raw_ind)=='' ? 'UNKNOWN' : TRIM(raw_ind)) AS raw_ind_clean;

segment_comma = FOREACH segment_slash GENERATE
    job_id,
    raw_ind_clean,
    (raw_ind_clean == 'UNKNOWN'
        ? 'UNKNOWN'
        : (raw_ind_clean MATCHES '([^/]+).*'
            ? REGEX_EXTRACT(raw_ind_clean, '([^/]+).*', 1)
            : raw_ind_clean
        )
    ) AS by_slash;

segment_gt = FOREACH segment_comma GENERATE
    job_id,
    by_slash,
    (by_slash == 'UNKNOWN'
        ? 'UNKNOWN'
        : (by_slash MATCHES '([^,]+).*'
            ? REGEX_EXTRACT(by_slash, '([^,]+).*', 1)
            : by_slash
        )
    ) AS by_comma;

segment_final = FOREACH segment_gt GENERATE
    job_id,
    by_comma,
    (by_comma == 'UNKNOWN'
        ? 'UNKNOWN'
        : (by_comma MATCHES '([^>]+).*'
            ? REGEX_EXTRACT(by_comma, '([^>]+).*', 1)
            : by_comma
        )
    ) AS industry_seed;

industry_norm = FOREACH segment_final GENERATE
    job_id,
    (industry_seed == 'UNKNOWN'
        ? 'UNKNOWN'
        : (TRIM(REPLACE(industry_seed, '\\s+', ' ')) == ''
            ? 'UNKNOWN'
            : UPPER(TRIM(REPLACE(industry_seed, '\\s+', ' ')))
        )
    ) AS industry_name;

industry_slugged = FOREACH industry_norm GENERATE
    job_id,
    industry_name,
    REPLACE(REPLACE(UPPER(TRIM(industry_name)), '\\s+', '_'), '[^A-Z0-9_-]', '') AS industry_slug;

industry_with_id = FOREACH industry_slugged GENERATE
    job_id,
    industry_name,
    (industry_name == 'UNKNOWN'
        ? 'UNKNOWN'
        : (industry_slug == '' ? 'UNKNOWN' : industry_slug)
    ) AS industry_id;

industry_lookup = FOREACH industry_with_id GENERATE job_id, industry_id, industry_name;

-- ========== kinh nghiệm ==========
exp_tokens = FOREACH job_clean GENERATE
    job_id,
    (raw_exp IS NULL OR TRIM(raw_exp)=='' ? 'UNKNOWN' : UPPER(TRIM(raw_exp))) AS exp_raw,
    REGEX_EXTRACT((raw_exp IS NULL ? '' : TRIM(raw_exp)), '^(\\d+)', 1) AS exp_digits;

exp_normalized = FOREACH exp_tokens GENERATE
    job_id,
    (CASE
        WHEN exp_raw IN ('KHÔNG YÊU CẦU','KHÔNG HIỂN THỊ','UNKNOWN','NULL') THEN 'KHÔNG YÊU CẦU'
        WHEN exp_raw MATCHES '.*DƯỚI 1 NĂM.*' THEN 'DƯỚI 1 NĂM'
        WHEN exp_digits MATCHES '^[123]$' THEN '1-3 NĂM'
        WHEN exp_digits MATCHES '^[45]$' THEN '3-5 NĂM'
        WHEN exp_raw MATCHES '.*5\\s*-\\s*10.*' THEN '3-5 NĂM'
        WHEN exp_digits MATCHES '^[6-9]$|^[1-9][0-9]+$' THEN 'TRÊN 5 NĂM'
        WHEN exp_raw MATCHES '.*TRÊN\\s*5.*' THEN 'TRÊN 5 NĂM'
        WHEN exp_raw == 'DƯỚI 1 NĂM' THEN 'DƯỚI 1 NĂM'
        ELSE exp_raw
    END) AS exp_required;

exp_with_industry = JOIN exp_normalized BY job_id LEFT OUTER, industry_lookup BY job_id;

enriched_exp = FOREACH exp_with_industry GENERATE
    (industry_lookup::industry_id IS NULL ? 'UNKNOWN' : industry_lookup::industry_id) AS industry_id,
    (industry_lookup::industry_name IS NULL ? 'UNKNOWN' : industry_lookup::industry_name) AS industry_name,
    exp_normalized::exp_required AS exp_required;

grp_exp = GROUP enriched_exp BY (industry_id, industry_name, exp_required);
exp_total = FOREACH grp_exp GENERATE
    group.industry_id AS industry_id,
    group.industry_name AS industry_name,
    group.exp_required AS exp_required,
    COUNT(enriched_exp) AS total_jobs;

exp_sorted = ORDER exp_total BY industry_id ASC, exp_required ASC;

rmf /user/maria_dev/output/requirement_analysis/exp_total;
STORE exp_sorted
INTO '/user/maria_dev/output/requirement_analysis/exp_total'
USING PigStorage('\t');

-- ========== kỹ năng ==========
skill_tokens = FOREACH job_clean GENERATE
    job_id,
    FLATTEN(
        (
            (raw_skill IS NULL OR TRIM(raw_skill)=='')
                ? TOBAG('UNKNOWN')
                : TOKENIZE(UPPER(TRIM(raw_skill)), '[,;/|]+')
        )
    ) AS skill_token;

skill_normalized = FOREACH skill_tokens GENERATE
    job_id,
    (skill_token IS NULL OR TRIM(skill_token)=='' ? 'UNKNOWN' : UPPER(TRIM(skill_token))) AS skill_name;

skill_with_industry = JOIN skill_normalized BY job_id LEFT OUTER, industry_lookup BY job_id;

enriched_skill = FOREACH skill_with_industry GENERATE
    (industry_lookup::industry_id IS NULL ? 'UNKNOWN' : industry_lookup::industry_id) AS industry_id,
    (industry_lookup::industry_name IS NULL ? 'UNKNOWN' : industry_lookup::industry_name) AS industry_name,
    skill_normalized::skill_name AS skill_name;

grp_skill = GROUP enriched_skill BY (industry_id, industry_name, skill_name);
skill_total = FOREACH grp_skill GENERATE
    group.industry_id AS industry_id,
    group.industry_name AS industry_name,
    group.skill_name AS skill_name,
    COUNT(enriched_skill) AS total_jobs;

skill_sorted = ORDER skill_total BY industry_id ASC, skill_name ASC;

rmf /user/maria_dev/output/requirement_analysis/skill_total;
STORE skill_sorted
INTO '/user/maria_dev/output/requirement_analysis/skill_total'
USING PigStorage('\t');

-- ========== yêu cầu ==========
req_tokens = FOREACH job_clean GENERATE
    job_id,
    FLATTEN(
        (
            (raw_req IS NULL OR TRIM(raw_req)=='')
                ? TOBAG('UNKNOWN')
                : TOKENIZE(UPPER(TRIM(raw_req)), '[,;/|]+')
        )
    ) AS req_token;

req_normalized = FOREACH req_tokens GENERATE
    job_id,
    (req_token IS NULL OR TRIM(req_token)=='' ? 'UNKNOWN' : UPPER(TRIM(req_token))) AS requirement;

req_with_industry = JOIN req_normalized BY job_id LEFT OUTER, industry_lookup BY job_id;

enriched_req = FOREACH req_with_industry GENERATE
    (industry_lookup::industry_id IS NULL ? 'UNKNOWN' : industry_lookup::industry_id) AS industry_id,
    (industry_lookup::industry_name IS NULL ? 'UNKNOWN' : industry_lookup::industry_name) AS industry_name,
    req_normalized::requirement AS requirement;

grp_req = GROUP enriched_req BY (industry_id, industry_name, requirement);
req_total = FOREACH grp_req GENERATE
    group.industry_id AS industry_id,
    group.industry_name AS industry_name,
    group.requirement AS requirement,
    COUNT(enriched_req) AS total_jobs;

req_sorted = ORDER req_total BY industry_id ASC, requirement ASC;

rmf /user/maria_dev/output/requirement_analysis/req_total;
STORE req_sorted
INTO '/user/maria_dev/output/requirement_analysis/req_total'
USING PigStorage('\t');

-- ========== trình độ học vấn ==========
edu_ready = FOREACH job_clean GENERATE
    job_id,
    (CASE
        WHEN raw_edu IS NULL OR TRIM(raw_edu)=='' THEN 'UNKNOWN'
        ELSE UPPER(TRIM(raw_edu))
    END) AS edu_raw;

edu_normalized = FOREACH edu_ready GENERATE
    job_id,
    (CASE
        WHEN edu_raw IN ('KHÔNG HIỂN THỊ','UNKNOWN','KHÔNG GIỚI HẠN','KHÁC') THEN 'KHÔNG YÊU CẦU'
        WHEN edu_raw == 'TRUNG HỌC CƠ SỞ (CẤP 2) TRỞ LÊN' THEN 'THCS'
        WHEN edu_raw IN ('TRUNG HỌC PHỔ THÔNG (CẤP 3) TRỞ LÊN','TRUNG HỌC') THEN 'THPT'
        WHEN edu_raw IN ('ĐẠI HỌC TRỞ LÊN','CỬ NHÂN') THEN 'ĐẠI HỌC'
        WHEN edu_raw IN ('CAO HỌC TRỞ LÊN','THẠC SĨ') THEN 'CAO HỌC'
        WHEN edu_raw IN ('TRUNG CẤP','TRUNG CẤP TRỞ LÊN') THEN 'TRUNG CẤP'
        ELSE edu_raw
    END) AS edu_level;

edu_with_industry = JOIN edu_normalized BY job_id LEFT OUTER, industry_lookup BY job_id;

enriched_edu = FOREACH edu_with_industry GENERATE
    (industry_lookup::industry_id IS NULL ? 'UNKNOWN' : industry_lookup::industry_id) AS industry_id,
    (industry_lookup::industry_name IS NULL ? 'UNKNOWN' : industry_lookup::industry_name) AS industry_name,
    edu_normalized::edu_level AS edu_level;

grp_edu = GROUP enriched_edu BY (industry_id, industry_name, edu_level);
edu_total = FOREACH grp_edu GENERATE
    group.industry_id AS industry_id,
    group.industry_name AS industry_name,
    group.edu_level AS edu_level,
    COUNT(enriched_edu) AS total_jobs;

edu_sorted = ORDER edu_total BY industry_id ASC, edu_level ASC;

rmf /user/maria_dev/output/requirement_analysis/edu_total;
STORE edu_sorted
INTO '/user/maria_dev/output/requirement_analysis/edu_total'
USING PigStorage('\t');

-- ========== loại hình làm việc ==========
type_ready = FOREACH job_clean GENERATE
    job_id,
    (CASE
        WHEN raw_type IS NULL OR TRIM(raw_type)=='' THEN 'UNKNOWN'
        ELSE UPPER(TRIM(raw_type))
    END) AS type_raw;

type_normalized = FOREACH type_ready GENERATE
    job_id,
    (CASE
        WHEN type_raw IN ('TOÀN THỜI GIAN CỐ ĐỊNH','TOÀN THỜI GIAN','TOÀN THỜI GIAN TẠM THỜI') THEN 'TOÀN THỜI GIAN'
        WHEN type_raw == 'BÁN THỜI GIAN' THEN 'BÁN THỜI GIAN'
        WHEN type_raw IN ('THỜI VỤ','HỢP ĐỒNG THỜI VỤ') THEN 'THỜI VỤ / HỢP ĐỒNG'
        WHEN type_raw == 'THỰC TẬP' THEN 'THỰC TẬP'
        WHEN type_raw IN ('KHÁC','KHÔNG HIỂN THỊ','UNKNOWN') THEN 'KHÁC / KHÔNG HIỂN THỊ'
        ELSE type_raw
    END) AS job_type;

type_with_industry = JOIN type_normalized BY job_id LEFT OUTER, industry_lookup BY job_id;

enriched_type = FOREACH type_with_industry GENERATE
    (industry_lookup::industry_id IS NULL ? 'UNKNOWN' : industry_lookup::industry_id) AS industry_id,
    (industry_lookup::industry_name IS NULL ? 'UNKNOWN' : industry_lookup::industry_name) AS industry_name,
    type_normalized::job_type AS job_type;

grp_type = GROUP enriched_type BY (industry_id, industry_name, job_type);
type_total = FOREACH grp_type GENERATE
    group.industry_id AS industry_id,
    group.industry_name AS industry_name,
    group.job_type AS job_type,
    COUNT(enriched_type) AS total_jobs;

type_sorted = ORDER type_total BY industry_id ASC, job_type ASC;

rmf /user/maria_dev/output/requirement_analysis/type_total;
STORE type_sorted
INTO '/user/maria_dev/output/requirement_analysis/type_total'
USING PigStorage('\t');

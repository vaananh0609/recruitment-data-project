-- ============================================================
-- SCRIPT: MAHOUT FEATURE MATRIX GENERATOR
-- Mục tiêu: gom các bảng requirement + location thành 1 file toàn số để nạp vào Mahout
-- ============================================================

exp_features = LOAD '/user/maria_dev/output/requirement_analysis/exp_total'
    USING PigStorage('\t')
    AS (industry_id:chararray, industry_name:chararray, feature_value:chararray, total_jobs:long);
exp_features = FOREACH exp_features GENERATE industry_id, industry_name, 'exp' AS feature_type, feature_value, total_jobs;

edu_features = LOAD '/user/maria_dev/output/requirement_analysis/edu_total'
    USING PigStorage('\t')
    AS (industry_id:chararray, industry_name:chararray, feature_value:chararray, total_jobs:long);
edu_features = FOREACH edu_features GENERATE industry_id, industry_name, 'edu' AS feature_type, feature_value, total_jobs;

type_features = LOAD '/user/maria_dev/output/requirement_analysis/type_total'
    USING PigStorage('\t')
    AS (industry_id:chararray, industry_name:chararray, feature_value:chararray, total_jobs:long);
type_features = FOREACH type_features GENERATE industry_id, industry_name, 'type' AS feature_type, feature_value, total_jobs;

skill_features = LOAD '/user/maria_dev/output/requirement_analysis/skill_total'
    USING PigStorage('\t')
    AS (industry_id:chararray, industry_name:chararray, feature_value:chararray, total_jobs:long);
skill_features = FOREACH skill_features GENERATE industry_id, industry_name, 'skill' AS feature_type, feature_value, total_jobs;

req_features = LOAD '/user/maria_dev/output/requirement_analysis/req_total'
    USING PigStorage('\t')
    AS (industry_id:chararray, industry_name:chararray, feature_value:chararray, total_jobs:long);
req_features = FOREACH req_features GENERATE industry_id, industry_name, 'req' AS feature_type, feature_value, total_jobs;

all_features = UNION exp_features, edu_features, type_features, skill_features, req_features;

industry_lookup_tmp = FOREACH all_features GENERATE industry_id, industry_name;
industry_lookup = DISTINCT industry_lookup_tmp;

location_source = LOAD '/user/maria_dev/output/industry_by_location'
    USING PigStorage('\t')
    AS (location_name:chararray, industry_name_loc:chararray, location_jobs:long);
location_joined = JOIN location_source BY industry_name_loc LEFT OUTER, industry_lookup BY industry_name;
location_features = FOREACH location_joined GENERATE
    (industry_lookup::industry_id IS NULL ? 'UNKNOWN' : industry_lookup::industry_id) AS industry_id,
    (location_source::industry_name_loc IS NULL ? 'UNKNOWN' : location_source::industry_name_loc) AS industry_name,
    'location' AS feature_type,
    (location_source::location_name IS NULL ? 'UNKNOWN' : location_source::location_name) AS feature_value,
    (location_source::location_jobs IS NULL ? 0 : location_source::location_jobs) AS total_jobs;

merged_features = UNION all_features, location_features;

industry_tmp = FOREACH merged_features GENERATE industry_id;
industry_distinct = DISTINCT industry_tmp;
industry_distinct_ordered = ORDER industry_distinct BY industry_id ASC;
industry_ranked = RANK industry_distinct_ordered;

feature_value_tmp = FOREACH merged_features GENERATE feature_type, feature_value;
feature_value_distinct = DISTINCT feature_value_tmp;
feature_value_ordered = ORDER feature_value_distinct BY feature_type ASC, feature_value ASC;
feature_value_ranked = RANK feature_value_ordered;

features_with_ind = JOIN merged_features BY industry_id LEFT OUTER, industry_ranked BY industry_id;
features_with_index = JOIN features_with_ind BY (merged_features::feature_type, merged_features::feature_value) LEFT OUTER,
    feature_value_ranked BY (feature_type, feature_value);

mahout_features = FOREACH features_with_index GENERATE
    (industry_ranked::rank_industry_distinct_ordered IS NULL ? -1 : industry_ranked::rank_industry_distinct_ordered) AS industry_idx,
    (CASE
        WHEN merged_features::feature_type == 'exp' THEN 1
        WHEN merged_features::feature_type == 'edu' THEN 2
        WHEN merged_features::feature_type == 'type' THEN 3
        WHEN merged_features::feature_type == 'skill' THEN 4
        WHEN merged_features::feature_type == 'req' THEN 5
        WHEN merged_features::feature_type == 'location' THEN 6
        ELSE 0
    END) AS feature_type_idx,
    (feature_value_ranked::rank_feature_value_ordered IS NULL ? 0 : feature_value_ranked::rank_feature_value_ordered) AS feature_value_idx,
    (merged_features::total_jobs IS NULL ? 0 : merged_features::total_jobs) AS feature_count;

rmf /user/maria_dev/output/mahout_features;
STORE mahout_features INTO '/user/maria_dev/output/mahout_features' USING PigStorage('\t');

-- Optional maps for lookup
industry_index = FOREACH industry_ranked GENERATE rank_industry_distinct_ordered AS industry_idx, industry_id;
rmf /user/maria_dev/output/mahout_industry_index;
STORE industry_index INTO '/user/maria_dev/output/mahout_industry_index' USING PigStorage('\t');
feature_value_index = FOREACH feature_value_ranked GENERATE rank_feature_value_ordered AS feature_value_idx, feature_type, feature_value;
rmf /user/maria_dev/output/mahout_feature_value_index;
STORE feature_value_index INTO '/user/maria_dev/output/mahout_feature_value_index' USING PigStorage('\t');

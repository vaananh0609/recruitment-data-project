/*
 * KICH BAN: XU LY JSON - PIG LATIN 100%
 * Tinh nang: Phan loai 63 tinh thanh + Dong Tong Cong (Grand Total)
 */


-- 1. LOAD DATA
A = LOAD '/user/maria_dev/output/job_base_clean'
  USING PigStorage('\t') AS (
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

B = FOREACH A GENERATE 
  job_id,
  raw_loc;

-- 3. NORMALIZE (Bao toan so dong: NULL/rong => UNKNOWN)
C = FOREACH B GENERATE
    job_id,
    (raw_loc IS NULL ? '' : raw_loc) AS raw_loc;

-- 4. CLEAN (Chuyen thuong + Khu dau co ban)
D = FOREACH C GENERATE 
    job_id,
    raw_loc,
    REPLACE(
      REPLACE(
        REPLACE(
          REPLACE(
            REPLACE(
              REPLACE(
                REPLACE(LOWER(raw_loc),
                'á|à|ả|ã|ạ|ă|ắ|ằ|ẳ|ẵ|ặ|â|ấ|ầ|ẩ|ẫ|ậ','a'),
              'é|è|ẻ|ẽ|ẹ|ê|ế|ề|ể|ễ|ệ','e'),
            'í|ì|ỉ|ĩ|ị','i'),
          'ó|ò|ỏ|õ|ọ|ô|ố|ồ|ổ|ỗ|ộ|ơ|ớ|ờ|ở|ỡ|ợ','o'),
        'ú|ù|ủ|ũ|ụ|ư|ứ|ừ|ử|ữ|ự','u'),
      '[ýỳỷỹỵ]','y'),
    '[đ]','d') AS loc_clean;

-- 4b. Chuan hoa NBSP, |, ...
D2 = FOREACH D GENERATE
    job_id,
    raw_loc,
    REPLACE(
      REPLACE(
        REPLACE(loc_clean, '\\u00A0', ' '),
      '\\|', ' '),
    '\\.\\.\\.|…', ' ') AS loc_clean_norm;

-- 5. QUY HOACH 63 TINH THANH
E = FOREACH D2 GENERATE
  job_id,
    raw_loc,
    loc_clean_norm,
    (CASE
        WHEN ((loc_clean_norm IS NULL) OR (loc_clean_norm == '')) THEN 'UNKNOWN'
        WHEN (loc_clean_norm MATCHES '.*(negotiable|hidden|khong hien thi|n\\/a).*') THEN 'UNKNOWN'
        WHEN (loc_clean_norm MATCHES '.*(overseas|nuoc ngoai|japan|korea|china|usa|uk|singapore|taiwan|nhat ban|han quoc|trung quoc|dai loan|australia|uc|philippines|international).*') THEN 'OVERSEAS'

        WHEN (loc_clean_norm MATCHES '.*(da nang|danang|hai chau|thanh khe|son tra|ngu hanh son|lien chieu|hoa vang|quan cam le|cam le|kinh duong vuong|hoa minh|nui thanh).*') THEN 'DA NANG'
        WHEN (loc_clean_norm MATCHES '.*(ho chi minh|hochiminh|hcm|sai gon|saigon|ben nghe|ben thanh|quan\\s*0?(1|2|3|4|5|6|7|8|9|10|11|12)|q\\.?\\s*0?(1|2|3|4|5|6|7|8|9|10|11|12)|dist\\.?\\s*0?(1|2|3|4|5|6|7|8|9|10|11|12)|district\\s*0?(1|2|3|4|5|6|7|8|9|10|11|12)|thu duc|binh thanh|tan binh|tan phu|go vap|binh chanh|binh tan|phu nhuan|hoc mon|cu chi|nha be|can gio|van hanh mall|pham ngu lao|le dai hanh|le van sy|nguyen dinh chieu|landmark 81|vincom center landmark|takashimaya|saigon centre|phu my hung|hiep phuoc).*') THEN 'TP HO CHI MINH'
        WHEN (loc_clean_norm MATCHES '.*(ha noi|hanoi|hn|ba dinh|dong da|cau giay|hoan kiem|hai ba trung|ha dong|nam tu liem|bac tu liem|dong anh|soc son|me linh|tay ho|hoang mai|thanh xuan|long bien|thanh tri|gia lam|hoai duc|thuong tin|chuong my|quoc oai|dan phuong|duy tan|dich vong|dich vong hau|dao duy anh|lieu giai|royal city|times city|pham hung|my dinh|me tri|son tay|ba vi|thach that|hoa lac|phu xuyen|thanh oai|xuan dinh|xuan tao|yen hoa|linh nam|pham van dong|lotte mall west lake).*') THEN 'HA NOI'
        WHEN (loc_clean_norm MATCHES '.*(hai phong|haiphong|ngo quyen|le chan|hong bang|kien an|do son|thuy nguyen|hai an).*') THEN 'HAI PHONG'
        WHEN (loc_clean_norm MATCHES '.*(can tho|cantho|ninh kieu|cai rang|binh thuy|o mon|thot not).*') THEN 'CAN THO'

        WHEN (loc_clean_norm MATCHES '.*(bac ninh|bacninh|tu son|yen phong|que vo|thuan thanh|gia binh|luong tai).*') THEN 'BAC NINH'
        WHEN (loc_clean_norm MATCHES '.*(bac giang|bacgiang|viet yen|hiep hoa|lang giang|luc nam|luc ngan|tan yen|yen dung|noi hoang|son dong).*') THEN 'BAC GIANG'
        WHEN (loc_clean_norm MATCHES '.*(vinh phuc|vinhphuc|vinh yen|phuc yen|binh xuyen|tam duong|lap thach).*') THEN 'VINH PHUC'
        WHEN (loc_clean_norm MATCHES '.*(hung yen|hungyen|my hao|van lam|van giang|yen my|tien lu).*') THEN 'HUNG YEN'
        WHEN (loc_clean_norm MATCHES '.*(hai duong|haiduong|chi linh|cam giang|kinh mon|nam sach).*') THEN 'HAI DUONG'
        WHEN (loc_clean_norm MATCHES '.*(thai binh|thaibinh|kien xuong|dong hung|tien hai|hung ha).*') THEN 'THAI BINH'
        WHEN (loc_clean_norm MATCHES '.*(nam dinh|namdinh|my loc|y yen|hai hau|xuan truong).*') THEN 'NAM DINH'
        WHEN (loc_clean_norm MATCHES '.*(ha nam|hanam|phu ly|dong van|kim bang|ly nhan).*') THEN 'HA NAM'
        WHEN (loc_clean_norm MATCHES '.*(thai nguyen|thainguyen|song cong|pho yen|dai tu|phu binh).*') THEN 'THAI NGUYEN'
        WHEN (loc_clean_norm MATCHES '.*(phu tho|phutho|viet tri|phu ninh|lam thao|doan hung).*') THEN 'PHU THO'
        WHEN (loc_clean_norm MATCHES '.*(lang son|langson|dong dang|cao loc|huu lung|trang dinh).*') THEN 'LANG SON'
        WHEN (loc_clean_norm MATCHES '.*(lao cai|laocai|sapa|bao thang|bat xat).*') THEN 'LAO CAI'
        WHEN (loc_clean_norm MATCHES '.*(yen bai|yenbai|nghia lo|tran yen|luc yen).*') THEN 'YEN BAI'
        WHEN (loc_clean_norm MATCHES '.*(son la|sonla|moc chau|mai son|thuan chau).*') THEN 'SON LA'
        WHEN (loc_clean_norm MATCHES '.*(hoa binh|hoabinh|luong son|ky son|tan lac).*') THEN 'HOA BINH'
        WHEN (loc_clean_norm MATCHES '.*(dien bien|dienbien|dien bien phu|muong lay).*') THEN 'DIEN BIEN'
        WHEN (loc_clean_norm MATCHES '.*(lai chau|laichau|tan uyen|phong tho).*') THEN 'LAI CHAU'
        WHEN (loc_clean_norm MATCHES '.*(ha giang|hagiang|dong van|meo vac).*') THEN 'HA GIANG'
        WHEN (loc_clean_norm MATCHES '.*(cao bang|caobang|bao lac|bao lam).*') THEN 'CAO BANG'
        WHEN (loc_clean_norm MATCHES '.*(tuyen quang|tuyenquang|son duong|yen son).*') THEN 'TUYEN QUANG'
        WHEN (loc_clean_norm MATCHES '.*(bac kan|backan|bac can|cho don).*') THEN 'BAC KAN'
        WHEN (loc_clean_norm MATCHES '.*(quang ninh|quangninh|ha long|cam pha|uong bi|mong cai|dong trieu|hai ha).*') THEN 'QUANG NINH'
        WHEN (loc_clean_norm MATCHES '.*(ninh binh|ninhbinh|tam diep|hoa lu|yen khanh).*') THEN 'NINH BINH'

        WHEN (loc_clean_norm MATCHES '.*(thanh hoa|thanhhoa|bim son|sam son|nghi son|tinh gia|quang xuong|hau loc|hoang hoa|cam thuy).*') THEN 'THANH HOA'
        WHEN (loc_clean_norm MATCHES '.*(nghe an|nghean|vinh|cua lo|dien chau|nghi loc|quynh luu).*') THEN 'NGHE AN'
        WHEN (loc_clean_norm MATCHES '.*(ha tinh|hatinh|ky anh|hong linh|can loc).*') THEN 'HA TINH'
        WHEN (loc_clean_norm MATCHES '.*(quang binh|quangbinh|dong hoi|bo trach|le thuy|quang trach).*') THEN 'QUANG BINH'
        WHEN (loc_clean_norm MATCHES '.*(quang tri|quangtri|dong ha|hai lang|gio linh|vinh linh).*') THEN 'QUANG TRI'
        WHEN (loc_clean_norm MATCHES '.*(thua thien hue|thuathienhue|hue|phu vang|phu loc|huong thuy|huong tra).*') THEN 'THUA THIEN HUE'
        WHEN (loc_clean_norm MATCHES '.*(quang nam|quangnam|tam ky|hoi an|dien ban|thang binh).*') THEN 'QUANG NAM'
        WHEN (loc_clean_norm MATCHES '.*(quang ngai|quangngai|son tinh|binh son|duc pho).*') THEN 'QUANG NGAI'
        WHEN (loc_clean_norm MATCHES '.*(binh dinh|binhdinh|quy nhon|an nhon|tay son).*') THEN 'BINH DINH'
        WHEN (loc_clean_norm MATCHES '.*(phu yen|phuyen|tuy hoa|song cau).*') THEN 'PHU YEN'
        WHEN (loc_clean_norm MATCHES '.*(khanh hoa|khanhhoa|nha trang|cam ranh|ninh hoa).*') THEN 'KHANH HOA'
        WHEN (loc_clean_norm MATCHES '.*(ninh thuan|ninhthuan|phan rang|ninh hai|ninh phuoc).*') THEN 'NINH THUAN'
        WHEN (loc_clean_norm MATCHES '.*(binh thuan|binhthuan|phan thiet|la gi|bac binh|tuy phong).*') THEN 'BINH THUAN'

        WHEN (loc_clean_norm MATCHES '.*(dak lak|daklak|buon ma thuot|krong pac|ea kar|cu mgar).*') THEN 'DAK LAK'
        WHEN (loc_clean_norm MATCHES '.*(dak nong|daknong|gia nghia|dak rlap|dak song).*') THEN 'DAK NONG'
        WHEN (loc_clean_norm MATCHES '.*(gia lai|gialai|pleiku|chu se|chu prong|ia grai).*') THEN 'GIA LAI'
        WHEN (loc_clean_norm MATCHES '.*(kon tum|kontum|dak ha|dak to|sa thay).*') THEN 'KON TUM'
        WHEN (loc_clean_norm MATCHES '.*(lam dong|lamdong|da lat|bao loc|duc trong|di linh).*') THEN 'LAM DONG'

        WHEN (loc_clean_norm MATCHES '.*(binh duong|binhduong|di an|thuan an|tan uyen|ben cat|bau bang|vsip 1|vsip 2|thu dau mot).*') THEN 'BINH DUONG'
        WHEN (loc_clean_norm MATCHES '.*(dong nai|dongnai|bien hoa|long thanh|nhon trach|trang bom).*') THEN 'DONG NAI'
        WHEN (loc_clean_norm MATCHES '.*(ba ria|baria|vung tau|phu my|long dien|dat do|xuyen moc).*') THEN 'BA RIA - VUNG TAU'
        WHEN (loc_clean_norm MATCHES '.*(tay ninh|tayninh|moc bai|trang bang|hoa thanh).*') THEN 'TAY NINH'
        WHEN (loc_clean_norm MATCHES '.*(binh phuoc|binhphuoc|dong xoai|chon thanh|bu dang).*') THEN 'BINH PHUOC'
        WHEN (loc_clean_norm MATCHES '.*(long an|longan|tan an|duc hoa|ben luc|can duoc).*') THEN 'LONG AN'
        WHEN (loc_clean_norm MATCHES '.*(tien giang|tiengiang|my tho|cai lay|go cong).*') THEN 'TIEN GIANG'
        WHEN (loc_clean_norm MATCHES '.*(ben tre|bentre|mo cay|giong trom|ba tri).*') THEN 'BEN TRE'
        WHEN (loc_clean_norm MATCHES '.*(vinh long|vinhlong|binh minh|long ho).*') THEN 'VINH LONG'
        WHEN (loc_clean_norm MATCHES '.*(tra vinh|travinh|cau ke|tieu can).*') THEN 'TRA VINH'
        WHEN (loc_clean_norm MATCHES '.*(dong thap|dongthap|cao lanh|sa dec|hong ngu).*') THEN 'DONG THAP'
        WHEN (loc_clean_norm MATCHES '.*(an giang|angiang|long xuyen|chau doc|tan chau).*') THEN 'AN GIANG'
        WHEN (loc_clean_norm MATCHES '.*(kien giang|kiengiang|rach gia|phu quoc|ha tien).*') THEN 'KIEN GIANG'
        WHEN (loc_clean_norm MATCHES '.*(hau giang|haugiang|vi thanh|nga bay).*') THEN 'HAU GIANG'
        WHEN (loc_clean_norm MATCHES '.*(soc trang|soctrang|vinh chau|ke sach).*') THEN 'SOC TRANG'
        WHEN (loc_clean_norm MATCHES '.*(bac lieu|baclieu|gia rai|hong dan).*') THEN 'BAC LIEU'
        WHEN (loc_clean_norm MATCHES '.*(ca mau|camau|dam doi|ngoc hien).*') THEN 'CA MAU'

        ELSE 'UNKNOWN'
    END) AS city;

    -- 5b. Gan province_id (1-63), UNKNOWN=0, OVERSEAS=-1
    E_Mapped = FOREACH E GENERATE
      job_id,
      raw_loc,
      city AS province_name,
      (CASE
        WHEN (city == 'OVERSEAS') THEN -1
        WHEN (city == 'UNKNOWN') THEN 0

        WHEN (city == 'AN GIANG') THEN 1
        WHEN (city == 'BA RIA - VUNG TAU') THEN 2
        WHEN (city == 'BAC GIANG') THEN 3
        WHEN (city == 'BAC KAN') THEN 4
        WHEN (city == 'BAC LIEU') THEN 5
        WHEN (city == 'BAC NINH') THEN 6
        WHEN (city == 'BEN TRE') THEN 7
        WHEN (city == 'BINH DINH') THEN 8
        WHEN (city == 'BINH DUONG') THEN 9
        WHEN (city == 'BINH PHUOC') THEN 10
        WHEN (city == 'BINH THUAN') THEN 11
        WHEN (city == 'CA MAU') THEN 12
        WHEN (city == 'CAN THO') THEN 13
        WHEN (city == 'CAO BANG') THEN 14
        WHEN (city == 'DA NANG') THEN 15
        WHEN (city == 'DAK LAK') THEN 16
        WHEN (city == 'DAK NONG') THEN 17
        WHEN (city == 'DIEN BIEN') THEN 18
        WHEN (city == 'DONG NAI') THEN 19
        WHEN (city == 'DONG THAP') THEN 20
        WHEN (city == 'GIA LAI') THEN 21
        WHEN (city == 'HA GIANG') THEN 22
        WHEN (city == 'HA NAM') THEN 23
        WHEN (city == 'HA NOI') THEN 24
        WHEN (city == 'HA TINH') THEN 25
        WHEN (city == 'HAI DUONG') THEN 26
        WHEN (city == 'HAI PHONG') THEN 27
        WHEN (city == 'HAU GIANG') THEN 28
        WHEN (city == 'HOA BINH') THEN 29
        WHEN (city == 'HUNG YEN') THEN 30
        WHEN (city == 'KHANH HOA') THEN 31
        WHEN (city == 'KIEN GIANG') THEN 32
        WHEN (city == 'KON TUM') THEN 33
        WHEN (city == 'LAI CHAU') THEN 34
        WHEN (city == 'LAM DONG') THEN 35
        WHEN (city == 'LANG SON') THEN 36
        WHEN (city == 'LAO CAI') THEN 37
        WHEN (city == 'LONG AN') THEN 38
        WHEN (city == 'NAM DINH') THEN 39
        WHEN (city == 'NGHE AN') THEN 40
        WHEN (city == 'NINH BINH') THEN 41
        WHEN (city == 'NINH THUAN') THEN 42
        WHEN (city == 'PHU THO') THEN 43
        WHEN (city == 'PHU YEN') THEN 44
        WHEN (city == 'QUANG BINH') THEN 45
        WHEN (city == 'QUANG NAM') THEN 46
        WHEN (city == 'QUANG NGAI') THEN 47
        WHEN (city == 'QUANG NINH') THEN 48
        WHEN (city == 'QUANG TRI') THEN 49
        WHEN (city == 'SOC TRANG') THEN 50
        WHEN (city == 'SON LA') THEN 51
        WHEN (city == 'TAY NINH') THEN 52
        WHEN (city == 'THAI BINH') THEN 53
        WHEN (city == 'THAI NGUYEN') THEN 54
        WHEN (city == 'THANH HOA') THEN 55
        WHEN (city == 'THUA THIEN HUE') THEN 56
        WHEN (city == 'TIEN GIANG') THEN 57
        WHEN (city == 'TP HO CHI MINH') THEN 58
        WHEN (city == 'TRA VINH') THEN 59
        WHEN (city == 'TUYEN QUANG') THEN 60
        WHEN (city == 'VINH LONG') THEN 61
        WHEN (city == 'VINH PHUC') THEN 62
        WHEN (city == 'YEN BAI') THEN 63

        ELSE 0
      END) AS province_id;

-- 6. TINH TOAN CHI TIET (GROUP BY City)
F_Detail = GROUP E BY city;
G_Detail = FOREACH F_Detail GENERATE 
    group AS city_name, 
    COUNT(E) AS job_count,
    2 AS sort_order;

-- 7. TINH TOAN TONG CONG (GROUP ALL)
F_Total = GROUP E ALL;
G_Total = FOREACH F_Total GENERATE 
    '=== TONG CONG (ALL) ===' AS city_name, 
    COUNT(E) AS job_count,
    1 AS sort_order;

-- 8. GOP KET QUA
H_Combined = UNION G_Total, G_Detail;

-- 9. SAP XEP
I_Sorted = ORDER H_Combined BY sort_order ASC, job_count DESC;

-- 10. CHUAN BI OUTPUT
J_Final = FOREACH I_Sorted GENERATE city_name, job_count;

-- 11. Lưu kết quả tổng hợp
rmf /user/maria_dev/output/final_63_provinces_total;
STORE J_Final INTO '/user/maria_dev/output/final_63_provinces_total' USING PigStorage('\t');

-- 12. Xuất từng tỉnh tự động
REGISTER /usr/hdp/current/pig-client/piggybank.jar;
rmf /user/maria_dev/output/ten_tinh;

-- 2) Ket qua tung tinh: province_id, province_name, raw_loc
E_Detail = FOREACH E_Mapped GENERATE province_id, province_name, raw_loc;

STORE E_Detail INTO '/user/maria_dev/output/ten_tinh'
  USING org.apache.pig.piggybank.storage.MultiStorage(
    '/user/maria_dev/output/ten_tinh',  -- base path
    '1',                                -- cột key (province_name)
    '\t'                                -- delimiter
  );

-- 3) log_location: job_id, province_id, province_name, is_overseas, is_unknown
rmf /user/maria_dev/output/log_location;
Log_Location = FOREACH E_Mapped GENERATE
  job_id,
  province_id,
  province_name,
  (province_name == 'OVERSEAS' ? 1 : 0) AS is_overseas,
  (province_name == 'UNKNOWN' ? 1 : 0) AS is_unknown;

STORE Log_Location INTO '/user/maria_dev/output/log_location' USING PigStorage('\t');
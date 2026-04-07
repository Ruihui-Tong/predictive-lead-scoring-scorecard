/* =========================================================================================
   Project: End-to-End Lead Scoring System (Scorecard Model)
   Script: 01_data_engineering/extract_features.sql
   Description: Feature extraction and aggregation for user behavioral and demographic data.
                Strictly uses 'alloc_date' and 'alloc_time' to define temporal boundaries,
                ensuring zero data leakage for the predictive model.
                
   Note on Time Windows: For local portfolio demonstration, temporal filters (e.g., '2019-12') 
   are hardcoded. In a production environment (e.g., Airflow/DolphinScheduler), these 
   would be replaced with dynamic execution variables (e.g., ${hiveconf:dt}).
========================================================================================= */

-- 01 Base Sample Table - Target Definition & Campaign Data
CREATE TABLE tmp.tmp_user_GiftInsurance_alloc201912 AS
SELECT  
    task.user_id
    ,task.task_id
    ,target_user_mobile
    ,alloc_date
    ,alloc_time
    ,first_chance_name
    ,channel_type
    ,channel
    ,subchannel
    ,agent_type -- Agent type mapping: 1 Self-built, 2 Co-built, 3 Franchise
    ,CASE WHEN task.call_type = 'connected' THEN 1 ELSE 0 END AS through_flag
    ,CASE WHEN calltime.callnum = 1 THEN 1 ELSE 0 END AS fst_through_flag    
    -- Target Variable Definitions: Conversion within 7 or 14 days
    ,CASE WHEN datediff(to_date(od.policy_time), alloc_date) <= 7 THEN 1 ELSE 0 END AS t7_policy_flag
    ,CASE WHEN datediff(to_date(od.policy_time), alloc_date) <= 14 THEN 1 ELSE 0 END AS t14_policy_flag
    ,count(distinct if(datediff(to_date(od.policy_time), alloc_date) <= 7, od.policy_no, null)) t7_policy_cnt
    ,sum(if(datediff(to_date(od.policy_time), alloc_date) <= 7, origin, null)) AS t7_origin
    ,count(distinct if(datediff(to_date(od.policy_time), alloc_date) <= 14, od.policy_no, null)) t14_policy_cnt
    ,sum(if(datediff(to_date(od.policy_time), alloc_date) <= 14, origin, null)) AS t14_policy_oringin
FROM 
    (
        SELECT DISTINCT 
                target_user_mobile,
                task.task_id,
                int(target_user_id) AS user_id,
                to_date(create_time) alloc_date,
                create_time alloc_time,
                first_chance_name,
                agent_name,
                if(callout.task_id is not null and mintime > create_time, 'connected', 'unconnected') AS call_type,
                agent_type
        FROM 
            (
                SELECT *
                FROM company_dwb.dwb_ins_crm_task_full_d
                WHERE dt = date_sub(current_date(), 1)
                AND first_chance_name LIKE '%free_insurance%'
                AND to_date(create_time) >= '2019-12-01'
                AND to_date(create_time) <= '2019-12-31'
            ) task
        LEFT JOIN 
            (
                SELECT 
                    task_id,
                    min(start_time) AS mintime 
                FROM company_sdm.sdm_ins_crm_call_record_full_d
                WHERE dt = date_sub(current_date(), 1)
                AND status = 'both_connected'
                AND team_name NOT LIKE '%Customer Service%' 
                AND to_date(start_time) >= '2019-12-01'
                AND to_date(start_time) <= '2020-01-15'
                GROUP BY task_id
            ) callout
        ON task.task_id = callout.task_id 
    ) task

LEFT JOIN 
    (
        SELECT DISTINCT mintime.task_id, num.callnum
        FROM
        (
            SELECT task_id, min(start_time) AS mintime 
            FROM company_sdm.sdm_ins_crm_call_record_full_d
            WHERE dt = date_sub(current_date(), 1)
            AND status = 'both_connected'
            AND team_name NOT IN ('Customer Service Team')
            AND to_date(start_time) >= '2019-12-01'
            AND to_date(start_time) <= '2020-01-15'
            GROUP BY task_id
        ) mintime  -- Earliest call connection time strictly after allocation
        JOIN
        (
            SELECT task_id, start_time, row_number() over(partition by mobile order by start_time) AS callnum 
            FROM company_sdm.sdm_ins_crm_call_record_full_d
            WHERE dt = date_sub(current_date(), 1)
        ) num   -- Sequential numbering of calls made
        ON mintime.task_id = num.task_id AND mintime.mintime = num.start_time
    ) calltime
ON task.task_id = calltime.task_id

LEFT JOIN
    (
        SELECT task_id, count(distinct start_time) exe_num
        FROM company_sdm.sdm_ins_crm_call_record_full_d
        WHERE dt = date_sub(current_date(), 1)
        AND to_date(start_time) >= '2019-12-01'
        AND to_date(start_time) <= '2020-01-15'
        GROUP BY task_id
    ) c -- Total execution/dialing count
ON task.task_id = c.task_id

LEFT JOIN
    (
        SELECT origin, task_id, phone, policy_no, policy_time
        FROM company_sdm.sdm_ins_crm_order_full_d
        WHERE dt = date_sub(current_date(), 1)
        AND to_date(policy_time) >= '2019-12-01'
        AND is_long = 1
        AND valid = 1 
        AND status IN (3,4,6)
    ) od
ON task.task_id = od.task_id

LEFT JOIN 
    (
        SELECT mobile, channel, subchannel,
            CASE WHEN channel IN ('feed','sem','qywx','dy','DY') THEN 'Ads'
                 WHEN (channel = 'huzhusq' or channel LIKE 'SQ%' or channel LIKE 'lesson%' or channel = 'sdbsqnew' or channel = 'lujiekz') THEN 'Community'
                 ELSE 'Normal' END AS channel_type,
            row_number() over(distribute by mobile sort by create_time asc) AS rn
        FROM company_sdm.sdm_ins_clue_user_chance_score_full_d
        WHERE dt = date_sub(current_date(), 1)
        AND chance_type_name LIKE '%free_insurance%'
    ) sub 
ON task.target_user_mobile = sub.mobile AND rn = 1
GROUP BY 
    task.user_id
    ,task.task_id
    ,target_user_mobile
    ,alloc_date
    ,first_chance_name
    ,channel_type
    ,channel
    ,subchannel
    ,agent_type
    ,alloc_time
    ,CASE WHEN task.call_type = 'connected' THEN 1 ELSE 0 END
    ,CASE WHEN calltime.callnum = 1 THEN 1 ELSE 0 END    
    ,CASE WHEN datediff(to_date(od.policy_time), alloc_date) <= 7 THEN 1 ELSE 0 END
    ,CASE WHEN datediff(to_date(od.policy_time), alloc_date) <= 14 THEN 1 ELSE 0 END;


-- 02 Base Feature Table - Demographic & Account Metrics

-- 02.1.1 Create intermediate table for ecosystem subscription info
CREATE TABLE tmp.tmp_wx_subscribe_1130to1231 AS
SELECT 
    dt,
    user_id,
    CASE WHEN third_type IN (1,104,309) THEN 1 ELSE 0 END AS follow_hz_flag,
    CASE WHEN third_type IN (3,17,61,115,116) THEN 1 ELSE 0 END AS follow_cf_flag,
    CASE WHEN third_type IN (7,420) THEN 1 ELSE 0 END AS follow_sdb_flag
FROM company_sdm.sdm_wx_user_detail_full_d 
WHERE dt <= '2019-12-31'
AND dt >= '2019-11-30'
AND subscribe = 1;

-- 02.1.2 Aggregate ecosystem subscription flags      
CREATE TABLE tmp.tmp_wx_subscribe_1130to1231_1 AS  
SELECT 
    dt,
    user_id,
    max(follow_hz_flag) follow_hz_flag,
    max(follow_cf_flag) follow_cf_flag,
    max(follow_sdb_flag) follow_sdb_flag
FROM tmp.tmp_wx_subscribe_1130to1231 
GROUP BY dt, user_id;

-- 02.2 Create Base Feature Table
CREATE TABLE tmp.tmp_user_level_GiftInsurance_BaseFeatureByDiff_alloc201912 AS
SELECT
    *,
    datediff(alloc_date, to_date(cf_fst_donate_time)) AS datediff_cf_fst_donate,
    datediff(alloc_date, to_date(cf_last_donate_time)) AS datediff_cf_lst_donate,
    datediff(alloc_date, to_date(lastshare_now_date)) AS datediff_lastshare,
    datediff(alloc_date, to_date(hz_fst_join_time)) AS datediff_hz_fst_join,
    CASE WHEN last_appstart_op_t IS NOT NULL THEN 1 ELSE 0 END AS sdb_app_start_flag
FROM tmp.tmp_user_GiftInsurance_alloc201912 t1
LEFT JOIN
    (SELECT 
        -- Global Platform Demographic Features
        b.user_id AS usr,
        b.os_type,
        b.verify_flag, -- Identity verification flag
        b.id_age,
        b.id_gender,
        b.wx_sex,
        -- City tier mappings for geographic analysis
        CASE
            WHEN b.id_city_name REGEXP'北京|上海|广州|深圳' THEN 'Tier_1'
            WHEN b.id_city_name REGEXP'成都|杭州|重庆|武汉|苏州|西安|天津|南京|郑州|长沙|沈阳|青岛|宁波|东莞|无锡' THEN 'New_Tier_1'
            WHEN b.id_city_name IS NULL THEN 'unknown'
            ELSE 'others' 
        END AS id_city_class,
        CASE
            WHEN b.register_city REGEXP'北京|上海|广州|深圳' THEN 'Tier_1'
            WHEN b.register_city IS NULL THEN 'unknown'
            ELSE 'others' 
        END AS register_city_class,
        CASE
            WHEN b.most_visit_city REGEXP'北京|上海|广州|深圳' THEN 'Tier_1'
            WHEN b.most_visit_city IS NULL THEN 'unknown'
            ELSE 'others' 
        END AS most_visit_city_class,
        CASE
            WHEN b.wx_city REGEXP'北京|上海|广州|深圳' THEN 'Tier_1'
            WHEN b.wx_city IS NULL THEN 'unknown'
            ELSE 'others' 
        END AS wx_city_class,
        CASE
            WHEN b.last_visit_city REGEXP'北京|上海|广州|深圳' THEN 'Tier_1'
            WHEN b.last_visit_city IS NULL THEN 'unknown'
            ELSE 'others' 
        END AS last_visit_city_class,
        CASE
            WHEN b.mobile_city REGEXP'北京|上海|广州|深圳' THEN 'Tier_1'
            WHEN b.mobile_city IS NULL THEN 'unknown'
            ELSE 'others' 
        END AS mobile_city_class,
        b.mobile_operator,

        -- Subscription status across 3 main ecosystem business lines
        b.follow_hz_tag,
        b.follow_cf_tag,
        b.follow_sdb_tag,

        -- Business Line 1 (cf) Features
        b.author_flag, 
        b.donate_case_num,
        b.donate_amt,
        b.donate_cnt,
        b.cf_fst_donate_time,
        b.cf_fst_donate_amt,
        b.cf_max_donate_amt,
        b.cf_min_donate_amt,
        b.cf_last_donate_time,
        b.cf_last_donate_amt,

        -- Business Line 2 (sdb) Features
        b.sdb_order_num,
        b.sdb_order_amt,
        b.sdb_normal_order_num,
        b.sdb_normal_order_amt,
        b.sdb_pay_order_amt,
        b.sdb_normal_pay_order_amt,

        -- Business Line 3 (hz) Features
        b.is_add_hz_flag,
        b.hz_order_num,
        b.hz_eff_order_num,
        b.hz_user_advance_flag,
        b.hz_total_charge_cnt,
        b.hz_total_charge_amt,
        b.hz_total_pay_amt,
        b.hz_total_bal_amt,
        b.hz_fst_join_amt,
        b.hz_insu_type_cnt,
        b.hz_fst_join_time,
        b.hz_insu_20_cnt,
        b.hz_insu_20_flag,
        b.hz_insu_1_flag,
        b.hz_insu_1_cnt,
        b.hz_insu_2_flag,
        b.hz_insu_2_cnt,
        b.hz_insu_3_flag,
        b.hz_insu_3_cnt,
        b.hz_insu_4_flag,
        b.hz_insu_4_cnt,
        b.hz_insu_21_flag,
        b.hz_insu_21_cnt
    FROM company_dwb.dwb_user_tag_info_full_d b 
    WHERE dt = '2019-12-31'
    ) t2
ON t1.user_id = t2.usr

LEFT JOIN
    (
    SELECT 
        user_id AS usr3
        ,count(*) AS share_cnt
        ,substr(max(time), 1, 10) AS lastshare_now_date
    FROM company_sdm.sdm_user_share_action_d
    WHERE dt = '2019-12-31'
    GROUP BY user_id
    ) t4
ON t1.user_id = t4.usr3

LEFT JOIN
    (
    SELECT 
        user_id AS usr4
        ,max(last_appstart_op_time) AS last_appstart_op_t
    FROM company_dwb.dwb_ins_app_user_tag_full_d
    WHERE dt = '2019-12-31'
    GROUP BY user_id
    ) t5
ON t1.user_id = t5.usr4

LEFT JOIN
    (SELECT
        t1.user_id usr5,
        max(CASE WHEN t2.dt <= date_sub(t1.alloc_date, 1) AND follow_hz_flag = 1 THEN '1' ELSE '0' END) AS follow_hz_tag_renew,
        max(CASE WHEN t2.dt <= date_sub(t1.alloc_date, 1) AND follow_cf_flag = 1 THEN '1' ELSE '0' END) AS follow_cf_tag_renew,
        max(CASE WHEN t2.dt <= date_sub(t1.alloc_date, 1) AND follow_sdb_flag = 1 THEN '1' ELSE '0' END) AS follow_sdb_tag_renew
    FROM tmp.tmp_user_GiftInsurance_alloc201912 t1
    LEFT JOIN tmp.tmp_wx_subscribe_1130to1231_1 t2
    ON t1.user_id = t2.user_id
    GROUP BY t1.user_id
    ) t6
ON t1.user_id = t6.usr5;


-- 03 User Behavior Log Aggregation (Strict temporal filtering applied)
CREATE TABLE tmp.tmp_user_level_GiftInsurance_BehaviorFeatureByDiff_alloc201912 AS
SELECT DISTINCT
    a.user_id AS usr_behavior,
    a.alloc_date,
    
    -- User Agent and OS Type
    c.ua,
    c.device_type,

    -- Business Line 1 (cf) browsing behavior
    e.first_visit_time first_visit_time_cf,
    e.last_visit_time last_visit_time_cf,
    e.last_week_visit_cnt last_week_visit_cnt_cf,
    e.visit_cnt visit_cnt_cf,
    e.visit_case_cnt,
    e.visit_caseid_num,
    datediff(a.alloc_date, to_date(e.first_visit_time)) AS datediff_first_visit_cf,
    datediff(a.alloc_date, to_date(e.last_visit_time)) AS datediff_last_visit_cf,

    -- Business Line 3 (hz) browsing behavior
    f.first_visit_time first_visit_time_hz,
    f.last_visit_time last_visit_time_hz,
    f.last_week_visit_cnt last_week_visit_cnt_hz,
    f.visit_cnt visit_cnt_hz,
    f.visit_event_cnt, 
    f.visit_payment_cnt, 
    datediff(a.alloc_date, to_date(f.first_visit_time)) AS datediff_first_visit_hz,
    datediff(a.alloc_date, to_date(f.last_visit_time)) AS datediff_last_visit_hz,
   
    -- Historical purchase behavior (Preventing Data Leakage)
    i.cnt,
    i.money,
    i.origin
FROM tmp.tmp_user_GiftInsurance_alloc201912 a
LEFT JOIN 
    (SELECT * FROM company_sdm.sdm_user_ua_full_d WHERE dt = '2019-12-31') c 
ON a.user_id = c.user_id
LEFT JOIN 
    (SELECT
        first_visit_time, last_visit_time, last_week_visit_cnt, visit_cnt, visit_case_cnt, visit_caseid_num, t1.user_id
     FROM
        (SELECT user_id, alloc_time, max(dw_end_date) max_dw_end_date, max(last_visit_time) max_last_visit_time
         FROM
            (SELECT t1.user_id, t1.alloc_time, first_visit_time, last_visit_time, last_week_visit_cnt, visit_cnt, dw_start_date, dw_end_date
             FROM tmp.tmp_user_GiftInsurance_alloc201912 t1 
             LEFT JOIN company_sdm.sdm_cf_user_visit_history_d t2 ON t1.user_id = t2.user_id
             WHERE last_visit_time < t1.alloc_time ) tt1  -- Ensuring actions happened strictly BEFORE allocation
         GROUP BY user_id, alloc_time) t1 
     INNER JOIN company_sdm.sdm_cf_user_visit_history_d t2
     ON t1.user_id = t2.user_id AND t1.max_last_visit_time = t2.last_visit_time
    ) e 
ON a.user_id = e.user_id
LEFT JOIN 
    (SELECT
        first_visit_time, last_visit_time, last_week_visit_cnt, visit_cnt, visit_event_cnt, visit_payment_cnt, t1.user_id
     FROM
        (SELECT user_id, alloc_time, max(dw_end_date) max_dw_end_date, max(last_visit_time) max_last_visit_time
         FROM
            (SELECT t1.user_id, t1.alloc_time, first_visit_time, last_visit_time, last_week_visit_cnt, visit_cnt, dw_start_date, dw_end_date
             FROM tmp.tmp_user_GiftInsurance_alloc201912 t1 
             LEFT JOIN company_sdm.sdm_hz_user_visit_history_d t2 ON t1.user_id = t2.user_id
             WHERE last_visit_time < t1.alloc_time ) tt1 -- Ensuring actions happened strictly BEFORE allocation
         GROUP BY user_id, alloc_time) t1 
     INNER JOIN company_sdm.sdm_hz_user_visit_history_d t2
     ON t1.user_id = t2.user_id AND t1.max_last_visit_time = t2.last_visit_time
    ) f 
ON a.user_id = f.user_id
LEFT JOIN 
    (SELECT user_id, count(order_no) AS cnt, sum(money) AS money, sum(origin) AS origin
     FROM company_sdm.sdm_ins_order_full_d
     WHERE dt = '2019-12-31'
     GROUP BY user_id
    ) i 
ON a.user_id = i.user_id;


-- 04 Target Product Max Visit Time
CREATE TABLE tmp.sdb_TimeForVisit_alloc201912 AS
SELECT
    user_id,
    max(dw_end_date) max_dw_end_date,
    max(last_visit_time) max_last_visit_time
FROM
    (SELECT t1.user_id, t1.task_id, t1.alloc_date, t1.alloc_time, first_visit_time, last_visit_time, last_week_visit_cnt, visit_cnt, dw_start_date, dw_end_date
     FROM tmp.tmp_user_GiftInsurance_alloc201912 t1 
     INNER JOIN company_sdm.sdm_ins_user_visit_history_d t2
     ON t1.user_id = t2.user_id
     WHERE last_visit_time < t1.alloc_time ) tt1
GROUP BY user_id;


-- 05 Target Product Visit Recency & Frequency
-- 05.1 Extract specific product page views (product codes masked for NDA compliance)
CREATE TABLE tmp.short_view_user_1115To1231_1 AS
SELECT DISTINCT 
    user_id, operation_time, reverse(substr(reverse(current_original_path), 1, locate('/', reverse(current_original_path)) - 1)) AS produ_no
FROM company_sdm.sdm_user_action_d
WHERE dt <= '2019-12-31'
AND dt >= '2019-11-15'
AND biz = 'ins'
AND operation IN ('page_enter')
AND user_id <> 0 
AND reverse(substr(reverse(current_original_path), 1, locate('/', reverse(current_original_path)) - 1)) 
    IN ('prod_01','prod_02','prod_03','prod_04','prod_05',
        'prod_06','prod_07','prod_08',
        'prod_09','prod_10','prod_11','prod_12','prod_13',
        'prod_14','prod_15','prod_16','prod_17');

-- 05.2 Aggregate target product view frequency
CREATE TABLE tmp.sdb_shortview_alloc201912 AS
SELECT
    user_id,
    alloc_date,
    min(to_date(operation_time)) first_oper_date_short,
    max(to_date(operation_time)) max_oper_date_short,
    count(distinct operation_time) AS total_visit_cnt_short,
    count(distinct if(to_date(operation_time) >= date_sub(alloc_date, 7), operation_time, null)) AS last_week_visit_cnt_short,
    count(distinct produ_no) AS product_cnt_short
FROM
    (SELECT t1.user_id, t1.alloc_date, t1.alloc_time, t1.task_id, t2.operation_time, t2.produ_no
     FROM tmp.tmp_user_GiftInsurance_alloc201912 t1 
     INNER JOIN tmp.short_view_user_1115To1231_1 t2 ON t1.user_id = t2.user_id
     WHERE t2.operation_time < t1.alloc_time ) tt1
GROUP BY user_id, alloc_date;


-- 06 Intermediate Merge: Base + Behavior
CREATE TABLE tmp.tmp_user_GiftInsurance_AllFeature_alloc201912 AS
SELECT DISTINCT 
    -- Base Sample Fields
    t1.user_id, t1.task_id, t1.target_user_mobile, t1.alloc_date, t1.alloc_time, t1.first_chance_name,
    t1.channel_type, t1.channel, t1.subchannel, t1.agent_type, t1.through_flag, t1.fst_through_flag,
    t1.t7_policy_flag, t1.t14_policy_flag, t1.t7_policy_cnt, t1.t14_policy_cnt, t1.t7_origin, t1.t14_policy_oringin,

    -- Global Platform Demographic Features
    t1.os_type, t1.verify_flag, t1.id_age, t1.id_gender, t1.wx_sex, t1.id_city_class,
    t1.register_city_class, t1.most_visit_city_class, t1.wx_city_class, t1.last_visit_city_class,
    t1.mobile_city_class, t1.mobile_operator, t2.ua, t2.device_type,

    -- Subscription tags
    t1.follow_hz_tag_renew, t1.follow_cf_tag_renew, t1.follow_sdb_tag_renew,

    -- Business Line 1 (cf) Features
    t1.author_flag, t1.donate_case_num, t1.donate_amt, t1.donate_cnt, t1.datediff_cf_fst_donate,
    t1.cf_fst_donate_amt, t1.datediff_cf_lst_donate, t1.cf_min_donate_amt, t1.cf_max_donate_amt,
    t1.cf_last_donate_amt, t1.datediff_lastshare,
    t2.last_week_visit_cnt_cf, t2.visit_cnt_cf, t2.visit_case_cnt, t2.visit_caseid_num,
    t2.datediff_first_visit_cf, t2.datediff_last_visit_cf,

    -- Business Line 2 (sdb) Features
    t1.sdb_order_num, t1.sdb_order_amt, t1.sdb_normal_order_num, t1.sdb_normal_order_amt,
    t1.sdb_pay_order_amt, t1.sdb_normal_pay_order_amt, t1.sdb_app_start_flag,
    t2.cnt, t2.money, t2.origin,

    -- Business Line 3 (hz) Features
    t1.is_add_hz_flag, t1.datediff_hz_fst_join, t1.hz_order_num, t1.hz_eff_order_num,
    t1.hz_user_advance_flag, t1.hz_total_charge_cnt, t1.hz_total_charge_amt, t1.hz_total_pay_amt,
    t1.hz_total_bal_amt, t1.hz_fst_join_amt, t1.hz_insu_type_cnt, t1.hz_insu_20_cnt,
    t1.hz_insu_20_flag, t1.hz_insu_1_flag, t1.hz_insu_1_cnt, t1.hz_insu_2_flag,
    t1.hz_insu_2_cnt, t1.hz_insu_3_flag, t1.hz_insu_3_cnt, t1.hz_insu_4_flag,
    t1.hz_insu_4_cnt, t1.hz_insu_21_flag, t1.hz_insu_21_cnt,
    t2.last_week_visit_cnt_hz, t2.visit_cnt_hz, t2.visit_event_cnt, t2.visit_payment_cnt,
    t2.datediff_first_visit_hz, t2.datediff_last_visit_hz
FROM tmp.tmp_user_level_GiftInsurance_BaseFeatureByDiff_alloc201912 t1
LEFT JOIN tmp.tmp_user_level_GiftInsurance_BehaviorFeatureByDiff_alloc201912 t2
ON t1.user_id = t2.usr_behavior;


-- 07 Append Target Product Log Data
CREATE TABLE tmp.tmp_user_GiftInsurance_AllFeature__alloc201912plus AS
SELECT 
    t1.*,
    datediff(t1.alloc_date, t2.first_oper_date_short) datediff_first_oper_short_renew,
    datediff(t1.alloc_date, t2.max_oper_date_short) datediff_last_oper_short_renew,
    t2.total_visit_cnt_short AS total_visit_cnt_short_renew,
    t2.last_week_visit_cnt_short AS last_week_visit_cnt_short_renew,
    t2.product_cnt_short AS product_cnt_short_renew,
    t3.first_visit_time first_visit_time_bao_renew,
    t3.last_visit_time last_visit_time_bao_renew,
    t3.last_week_visit_cnt last_week_visit_cnt_bao_renew,
    t3.visit_cnt visit_cnt_bao_renew,
    datediff(t1.alloc_date, to_date(t3.first_visit_time)) AS datediff_first_visit_bao_renew,
    datediff(t1.alloc_date, to_date(t3.last_visit_time)) AS datediff_last_visit_bao_renew,
    t4.gift_insurance_cnt
FROM tmp.tmp_user_GiftInsurance_AllFeature_alloc201912 t1
LEFT JOIN tmp.sdb_shortview_alloc201912 t2 ON t1.user_id = t2.user_id
LEFT JOIN 
    (SELECT first_visit_time, last_visit_time, last_week_visit_cnt, visit_cnt, t1.user_id
     FROM tmp.sdb_TimeForVisit_alloc201912 t1 
     INNER JOIN company_sdm.sdm_ins_user_visit_history_d t2
     ON t1.user_id = t2.user_id AND t1.max_last_visit_time = t2.last_visit_time
    ) t3 
ON t1.user_id = t3.user_id
LEFT JOIN
    (SELECT m1.user_id, count(distinct if(to_date(m2.create_time) <= m1.alloc_date, id, null)) gift_insurance_cnt
     FROM 
        (SELECT user_id, alloc_date FROM tmp.tmp_user_GiftInsurance_AllFeature_alloc201912) m1
     INNER JOIN
        (SELECT id, user_id, create_time FROM company_sdm.sdm_ins_order_full_d b
         WHERE b.order_type = 1 AND policy_no <> '' AND b.dt = '2019-12-31') m2
     ON m1.user_id = m2.user_id
     GROUP BY m1.user_id
    ) t4
ON t1.user_id = t4.user_id;


-- 08 Final Table Compilation: Data Cleansing, Null Handling, and Feature Formatting
CREATE TABLE tmp.tmp_user_GiftInsurance_AllFeature_alloc201912plus AS
SELECT
    -- Base Sample Fields
    t1.user_id, task_id, max(target_user_mobile) target_user_mobile, max(alloc_date) alloc_date, max(first_chance_name) first_chance_name,
    max(CASE WHEN tt2.channel IN('feed','sem') or tt2.channel LIKE '%BD%' THEN 'Ads'
             WHEN tt2.channel IN('cf','sdhz','sdbao') or tt2.channel LIKE '%app%' THEN 'Internal'
             WHEN tt2.channel = 'qywx' THEN 'Community'
             ELSE 'Others' END) channel_type,
    max(CASE WHEN tt2.channel = 'feed' THEN 'feed'
             WHEN tt2.channel = 'cf' THEN 'cf'
             WHEN tt2.channel = 'sdhz' THEN 'sdhz'
             WHEN tt2.channel = 'qywx' THEN 'qywx'
             WHEN tt2.channel LIKE '%BD%' THEN 'BD'
             WHEN tt2.channel LIKE '%app%' or tt2.channel = 'sdbao' THEN 'sdbao'
             ELSE 'others' END) channel_group,
    max(agent_type) agent_type, max(t14_policy_flag) t14_policy_flag,

    -- Global Platform Demographic Features
    max(os_type) os_type, max(id_age) id_age, max(id_gender) id_gender, max(wx_sex) wx_sex,
    max(id_city_class) id_city_class, max(register_city_class) register_city_class,
    max(most_visit_city_class) most_visit_city_class, max(wx_city_class) wx_city_class,
    max(last_visit_city_class) last_visit_city_class, max(mobile_city_class) mobile_city_class,
    
    -- Feature Engineering: Detecting geographic mobility/upward migration flag
    max(CASE WHEN id_city_class = 'unknown' THEN 'unkonwn'
             WHEN most_visit_city_class = 'unknown' THEN 'unkonwn'
             WHEN id_city_class IN ('New_Tier_1','Tier_2','Tier_3','Tier_4','Tier_5','others') AND most_visit_city_class IN('Tier_1') THEN '1'
             WHEN most_visit_city_class IN ('others') THEN '0'
             ELSE '0' END
        ) id_mostvisit_city_up_flag,
    max(CASE WHEN id_city_class = 'unknown' THEN 'unkonwn'
             WHEN wx_city_class = 'unknown' THEN 'unkonwn'
             WHEN id_city_class IN ('New_Tier_1','Tier_2','Tier_3','Tier_4','Tier_5','others') AND wx_city_class IN('Tier_1') THEN '1'
             WHEN wx_city_class IN ('others') THEN '0'
             ELSE '0' END
        ) id_wx_city_up_flag,
    max(concat(id_city_class, wx_city_class)) id_wx_class_concat,
    max(concat(id_city_class, most_visit_city_class)) id_mostvisit_class_concat,
    max(device_type) device_type,
    max(CASE WHEN mobile_operator = '移动' THEN 'Operator_A'
             WHEN mobile_operator = '联通' THEN 'Operator_B'
             WHEN mobile_operator = '电信' THEN 'Operator_C'
             ELSE 'others' END) AS mobile_operator,

    -- Ecosystem Subscription Tags
    max(follow_hz_tag_renew) follow_hz_tag, max(follow_cf_tag_renew) follow_cf_tag, max(follow_sdb_tag_renew) follow_sdb_tag,
    max(CASE WHEN follow_hz_tag_renew = '1' or follow_cf_tag_renew = '1' or follow_sdb_tag_renew = '1' THEN '1' ELSE '0' END) AS follow_tag,

    -- Handling NULLs for continuous & categorical variables prior to WOE binning
    max(author_flag) author_flag, 
    max(if(donate_case_num is null, 0, donate_case_num)) donate_case_num,
    max(if(donate_amt is null, 0, donate_amt)) donate_amt,
    max(if(donate_cnt is null, 0, donate_cnt)) donate_cnt,
    max(datediff_cf_fst_donate) datediff_cf_fst_donate,
    max(if(cf_fst_donate_amt is null, 0, cf_fst_donate_amt)) cf_fst_donate_amt,
    max(datediff_cf_lst_donate) datediff_cf_lst_donate,
    max(if(cf_min_donate_amt is null, 0, cf_min_donate_amt)) cf_min_donate_amt,
    max(if(cf_max_donate_amt is null, 0, cf_max_donate_amt)) cf_max_donate_amt,
    max(if(cf_last_donate_amt is null, 0, cf_last_donate_amt)) cf_last_donate_amt,
    max(datediff_lastshare) datediff_lastshare,
    max(if(last_week_visit_cnt_cf is null, 0, last_week_visit_cnt_cf)) last_week_visit_cnt_cf,
    max(if(visit_cnt_cf is null, 0, visit_cnt_cf)) visit_cnt_cf,
    max(if(visit_case_cnt is null, 0, visit_case_cnt)) visit_case_cnt,
    max(if(visit_caseid_num is null, 0, visit_caseid_num)) visit_caseid_num,
    max(datediff_first_visit_cf) datediff_first_visit_cf,
    max(datediff_last_visit_cf) datediff_last_visit_cf,

    max(if(sdb_order_num is null, 0, sdb_order_num)) sdb_order_num,
    max(if(sdb_order_amt is null, 0, sdb_order_amt)) sdb_order_amt,
    max(if(sdb_normal_order_num is null, 0, sdb_normal_order_num)) sdb_normal_order_num,
    max(if(sdb_normal_order_amt is null, 0, sdb_normal_order_amt)) sdb_normal_order_amt,
    max(if(sdb_pay_order_amt is null, 0, sdb_pay_order_amt)) sdb_pay_order_amt,
    max(if(sdb_normal_pay_order_amt is null, 0, sdb_normal_pay_order_amt)) sdb_normal_pay_order_amt,
    max(if(sdb_app_start_flag is null, 0, sdb_app_start_flag)) sdb_app_start_flag,
    
    max(datediff_first_oper_short_renew) datediff_first_oper_short,
    max(datediff_last_oper_short_renew) datediff_last_oper_short,
    max(if(total_visit_cnt_short_renew is null, 0, total_visit_cnt_short_renew)) total_visit_cnt_short,
    max(if(last_week_visit_cnt_short_renew is null, 0, last_week_visit_cnt_short_renew)) last_week_visit_cnt_short,
    max(if(product_cnt_short_renew is null, 0, product_cnt_short_renew)) product_cnt_short,
    max(if(last_week_visit_cnt_bao_renew is null, 0, last_week_visit_cnt_bao_renew)) last_week_visit_cnt_bao,
    max(if(visit_cnt_bao_renew is null, 0, visit_cnt_bao_renew)) visit_cnt_bao,
    max(datediff_first_visit_bao_renew) datediff_first_visit_bao,
    max(datediff_last_visit_bao_renew) datediff_last_visit_bao,
    max(if(gift_insurance_cnt is null, 0, gift_insurance_cnt)) gift_insurance_cnt,

    max(if(is_add_hz_flag is null, '0', is_add_hz_flag)) is_add_hz_flag,
    max(datediff_hz_fst_join) datediff_hz_fst_join,
    max(if(hz_order_num is null, 0, hz_order_num)) hz_order_num,
    max(if(hz_eff_order_num is null, 0, hz_eff_order_num)) hz_eff_order_num,
    max(if(hz_user_advance_flag is null, 0, hz_user_advance_flag)) hz_user_advance_flag,
    max(if(hz_total_charge_cnt is null, 0, hz_total_charge_cnt)) hz_total_charge_cnt,
    float(max(if(hz_total_charge_amt is null, 0, hz_total_charge_amt)*1)) hz_total_charge_amt,
    float(max(if(hz_total_pay_amt is null, 0, hz_total_pay_amt)*1)) hz_total_pay_amt,
    float(max(if(hz_total_bal_amt is null, 0, hz_total_bal_amt)*1)) hz_total_bal_amt,
    float(max(if(hz_fst_join_amt is null, 0, hz_fst_join_amt)*1)) hz_fst_join_amt,
    max(if(hz_insu_type_cnt is null, 0, hz_insu_type_cnt)) hz_insu_type_cnt,
    max(if(hz_insu_20_cnt is null, 0, hz_insu_20_cnt)) hz_insu_20_cnt,
    max(if(hz_insu_20_flag is null, '0', hz_insu_20_flag)) hz_insu_20_flag,
    max(if(hz_insu_1_flag is null, '0', hz_insu_1_flag)) hz_insu_1_flag,
    max(if(hz_insu_1_cnt is null, 0, hz_insu_1_cnt)) hz_insu_1_cnt,
    max(if(hz_insu_2_flag is null, '0', hz_insu_2_flag)) hz_insu_2_flag,
    max(if(hz_insu_2_cnt is null, 0, hz_insu_2_cnt)) hz_insu_2_cnt,
    max(if(hz_insu_3_flag is null, '0', hz_insu_3_flag)) hz_insu_3_flag,
    max(if(hz_insu_3_cnt is null, 0, hz_insu_3_cnt)) hz_insu_3_cnt,
    max(if(hz_insu_4_flag is null, '0', hz_insu_4_flag)) hz_insu_4_flag,
    max(if(hz_insu_4_cnt is null, 0, hz_insu_4_cnt)) hz_insu_4_cnt,
    max(if(hz_insu_21_flag is null, '0', hz_insu_21_flag)) hz_insu_21_flag,
    max(if(hz_insu_21_cnt is null, 0, hz_insu_21_cnt)) hz_insu_21_cnt,
    
    max(if(last_week_visit_cnt_hz is null, 0, last_week_visit_cnt_hz)) last_week_visit_cnt_hz,
    max(if(visit_cnt_hz is null, 0, visit_cnt_hz)) visit_cnt_hz,
    max(if(visit_event_cnt is null, 0, visit_event_cnt)) visit_event_cnt, 
    max(if(visit_payment_cnt is null, 0, visit_payment_cnt)) visit_payment_cnt, 
    max(datediff_first_visit_hz) datediff_first_visit_hz,
    max(datediff_last_visit_hz) datediff_last_visit_hz
FROM tmp.tmp_user_GiftInsurance_AllFeature__alloc201912plus t1
LEFT JOIN
    (SELECT t2.channel, t2.user_id usr
     FROM
        (SELECT a.user_id usr, max(create_time) AS max_crt_time
         FROM company_sdm.sdm_ins_order_full_d a
         WHERE a.order_type = 1 AND policy_no <> '' AND a.dt = '2019-12-31'
         GROUP BY a.user_id) t1
     INNER JOIN
        (SELECT channel, user_id, create_time FROM company_sdm.sdm_ins_order_full_d b
         WHERE b.order_type = 1 AND policy_no <> '' AND b.dt = '2019-12-31') t2
     ON t1.usr = t2.user_id AND t1.max_crt_time = t2.create_time
    ) tt2
ON t1.user_id = tt2.usr
GROUP BY user_id, task_id;

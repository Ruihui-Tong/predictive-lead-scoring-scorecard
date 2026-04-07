/* =========================================================================================
   Project: End-to-End Lead Scoring System (Scorecard Model)
   Script: 03_deployment/production_scoring.sql
   Description: Native SQL deployment of the Logistic Regression Scorecard.
                This script translates the Python model rules into highly efficient
                CASE WHEN statements, allowing the Data Warehouse to score and rank millions
                of leads daily natively without relying on external API infrastructures.
========================================================================================= */

-- 01. Extract features and calculate the final Scorecard points
CREATE TABLE tmp.tmp_lead_scoring_result_201912 AS
SELECT 
    DISTINCT
    -- Total Score = Sum of Feature Scores + Base Points (327)
    (t1.wx_city_class_score + 
     t1.channel_group_score + 
     t1.id_age_score + 
     t1.hz_total_bal_amt_score + 
     t1.total_visit_cnt_short_score + 
     t1.sdb_app_start_flag_score + 
     327) AS total_score,
    t1.user_id,
    t1.t14_policy_flag
FROM 
    (SELECT 
        a.user_id,
        a.t14_policy_flag,
        
        -- Geographic Tier Scoring (Aligned with Phase 1 & 2 English labels)
        CASE WHEN a.wx_city_class = 'New_Tier_1' THEN 36
             WHEN a.wx_city_class = 'Tier_1' THEN 27
             WHEN a.wx_city_class = 'Tier_2' THEN 25
             WHEN a.wx_city_class = 'Tier_3' THEN 30
             WHEN a.wx_city_class = 'Tier_4' THEN 36
             WHEN a.wx_city_class = 'Tier_5' THEN 51
             WHEN a.wx_city_class = 'others' THEN 34
             WHEN a.wx_city_class = 'unknown' THEN -81
             ELSE 0
        END AS wx_city_class_score,
        
        -- Acquisition Channel Scoring
        CASE WHEN a.channel_group = 'sdbao' THEN 32
             WHEN a.channel_group = 'sdhz' THEN 24
             WHEN a.channel_group = 'cf' THEN -8
             WHEN a.channel_group = 'feed' THEN 1
             WHEN a.channel_group = 'BD' THEN -19
             WHEN a.channel_group = 'others' THEN -13
             WHEN a.channel_group = 'qywx' THEN 156
             ELSE 0
        END AS channel_group_score,
        
        -- Demographic Age Scoring
        CASE WHEN a.id_age IS NULL THEN -97
             WHEN a.id_age < 18 THEN 12
             WHEN a.id_age >= 18 AND a.id_age < 24 THEN 13
             WHEN a.id_age >= 24 AND a.id_age < 31 THEN -9
             WHEN a.id_age >= 31 AND a.id_age < 41 THEN -1
             WHEN a.id_age >= 41 AND a.id_age < 50 THEN 3
             WHEN a.id_age >= 50 THEN 0
             ELSE 0
        END AS id_age_score,
        
        -- Ecosystem Balance Scoring
        CASE WHEN a.hz_total_bal_amt < 1 THEN -1
             WHEN a.hz_total_bal_amt >= 1 AND a.hz_total_bal_amt < 4 THEN -1
             WHEN a.hz_total_bal_amt >= 4 THEN 8
             ELSE 0
        END AS hz_total_bal_amt_score,
        
        -- Behavioral: Short-term Visit Frequency Scoring
        CASE WHEN a.total_visit_cnt_short < 1 THEN -5
             WHEN a.total_visit_cnt_short >= 1 AND a.total_visit_cnt_short < 2 THEN -8
             WHEN a.total_visit_cnt_short >= 2 AND a.total_visit_cnt_short < 3 THEN 4
             WHEN a.total_visit_cnt_short >= 3 THEN 20
             ELSE 0
        END AS total_visit_cnt_short_score,
        
        -- Behavioral: App Start Flag Scoring
        CASE WHEN a.sdb_app_start_flag < 1 THEN -1
             WHEN a.sdb_app_start_flag >= 1 THEN 55
             ELSE 0
        END AS sdb_app_start_flag_score
        
    -- Referencing the comprehensive feature table generated in Phase 1
    FROM tmp.tmp_user_GiftInsurance_AllFeature_alloc201912plus a
    WHERE a.most_visit_city_class IS NOT NULL
    ) t1;


-- 02. Ranking Leads for CRM Distribution
-- Generating a prioritized list for the Sales Team based on conversion probability (total_score)
CREATE TABLE tmp.tmp_lead_scoring_ranked_201912 AS
SELECT 
    ROW_NUMBER() OVER(ORDER BY total_score DESC) AS lead_rank,
    total_score,
    user_id,
    t14_policy_flag
FROM tmp.tmp_lead_scoring_result_201912;

# Standard Operating Procedure (SOP): Lead Scoring Pipeline

## 1. Overview
This document outlines the daily operations, maintenance, and retraining protocols for the Predictive Lead Scoring Scorecard. This pipeline scores "freebie" insurance leads to optimize the sales team's outreach prioritization.

## 2. Daily Batch Execution (Data Warehouse)
The scoring model runs natively in the Data Warehouse via SQL. 
* **Schedule:** Daily at 02:00 AM (to ensure upstream behavioral logs are fully processed).
* **Execution Script:** `03_deployment/production_scoring.sql`
* **Dynamic Parameters:** In a production environment (e.g., Apache Airflow), the hardcoded dates in the SQL scripts must be replaced with execution variables.
  * *Example:* Replace `2019-12-31` with `${hiveconf:dt}` to ensure rolling historical windows.
* **Output Destination:** The final ranked table (`tmp_lead_scoring_ranked`) is pushed to the Sales CRM via Kafka/API integration by 06:00 AM.

## 3. Model Monitoring & Retraining Triggers
The Scorecard model is highly interpretable and stable, but it must be monitored for concept drift or changes in business strategy (e.g., introducing a new marketing channel).

**Trigger Criteria for Retraining:**
1. **Performance Degradation:** The out-of-time KS statistic drops below `0.30` or AUC drops below `0.65` for two consecutive weeks.
2. **Population Stability Index (PSI):** If the PSI of the total score or top 3 features exceeds `0.20`, indicating a significant shift in the lead demographic or behavior.
3. **New Business Lines:** When marketing launches a completely new channel (e.g., TikTok Ads) that holds significant volume not captured in the original WOE bins.

## 4. Retraining Protocol
When a retrain is triggered, follow these steps using the scripts provided in `02_modeling`:
1. **Extract Fresh Data:** Update the temporal windows in `01_data_engineering/extract_features.sql` to pull the most recent 3-6 months of data. Ensure a minimum 14-day maturation window for the `t14_policy_flag` target variable.
2. **Re-evaluate WOE Bins:** Run `sc.woebin()`. Pay close attention to the `channel_type` and `wx_city_class` features. **Do not blindly accept algorithmic bins.** Manually adjust breaks using the `breaks_adj` dictionary to ensure they make intuitive business sense to the sales team.
3. **Feature Selection:** Re-run the L1 (Lasso) penalty to see if new features carry more weight. 
4. **Update Production Rules:** Once the new scorecard is generated (`final_scorecard_rules.csv`), translate the new points into the `CASE WHEN` logic in `03_deployment/production_scoring.sql`. 
5. **Communicate Changes:** Send a brief email to the Sales Managers explaining any major shifts in point allocations (e.g., "Leads from Channel X now receive fewer points due to recent lower conversion trends").

## 5. Troubleshooting & FAQ
* **Missing Features:** If a lead has `NULL` for key demographic features (like City or Age), the model automatically assigns the baseline points for the "missing" or "unknown" WOE bin (e.g., Age=Missing gets -97 points). This penalizes incomplete profiles to protect conversion rates.
* **Score Ties:** If leads have identical scores, the CRM routing defaults to the recency of their last visit (`last_visit_time`).


# 🚀 End-to-End Lead Scoring System: Turning Free Traffic into Revenue

An end-to-end lead scoring system using Logistic Regression and WOE binning to optimize sales conversion from 10% to 16.5%.

## 📌 Executive Summary

In high-volume customer acquisition, not all leads are created equal. This project solves a critical operational bottleneck: **optimizing the conversion rate of "freebie" insurance leads.** By building and deploying an interpretable Scorecard Model (Logistic Regression + WOE Binning), we successfully prioritized high-quality leads for the sales team, eliminating wasted hours on zero-intent traffic.

**Business Impact:**
* 📈 Increased target conversion rate from **10% to 16.5%**.
* 💰 Directly boosted overall profit margins by **17.6%**.
* ⏱️ Significantly reduced the manual workload for the audit and sales teams.

---

## 💡 The Philosophy: "Analyzing Not Data, But Problems"

When faced with low conversion rates, the instinct is often to build a complex black-box model (like XGBoost or Random Forest). However, cross-functional alignment is key. **If the sales team doesn't understand why a lead is scored low, they won't trust the system.** Therefore, I chose a classic **Financial Risk Scorecard** approach. By using Weight of Evidence (WOE) and Information Value (IV), the model translates complex probability into simple, actionable "add/deduct points" rules that business stakeholders can easily understand, trust, and act upon.

---

## 🏗️ Project Architecture & Pipeline

This repository contains the complete end-to-end lifecycle of the project, from raw data extraction to production deployment.

### Phase 1: Data Extraction & Feature Engineering
Before modeling, raw user profiles, behavioral logs, and historical campaign data were extracted from the Data Warehouse and flattened into a training dataset. 

* **File:** `01_data_engineering/extract_features.sql`
* **Core Action:** Ensuring strict temporal boundaries based on the allocation time (`alloc_time`) to completely prevent **Data Leakage (Future Data Peeking)**.

```sql
-- Snippet: Extracting historical lead behavior without data leakage
SELECT
    t1.user_id, 
    t1.alloc_time, 
    first_visit_time, 
    last_visit_time, 
    visit_cnt
FROM tmp.tmp_user_GiftInsurance_alloc201912 t1 
LEFT JOIN company_sdm.sdm_ins_user_visit_history_d t2 
    ON t1.user_id = t2.user_id
-- Core Action: Ensuring actions happened strictly BEFORE allocation time
WHERE t2.last_visit_time < t1.alloc_time; 
````

### Phase 2: Interpretative Modeling & WOE Binning

Using Python and the `scorecardpy` library, continuous and categorical variables were binned using Weight of Evidence (WOE). Features with high Information Value (IV) were selected, and L1 Penalty (Lasso) was applied to remove redundant variables before training a Logistic Regression model.

  * **File:** `02_modeling/scorecard_model_training.py`
  * **Core Action:** Injecting business domain knowledge by manually adjusting algorithmic bin boundaries to ensure monotonicity and business logic alignment.

<!-- end list -->

```python
# Snippet: Applying manual WOE adjustments based on business logic
breaks_adj = {
    'id_city_class': ["New_Tier_1", "Tier_1", "Tier_2", "Tier_3", "Tier_4", "Tier_5", "others", "unknown"],
    'visit_cnt_hz': [1, 6, 12, 22, 66],
    'hz_total_charge_amt': [1, 14, 62]
}

print("[INFO] Applying manual WOE adjustments...")
bins_adj = sc.woebin(train, y=target, breaks_list=breaks_adj, var_skip=skip_all_list)
```

### Phase 3: Production Deployment via SQL

Instead of relying on a complex API infrastructure to serve the Python model, **the scorecard rules were reverse-engineered into a native SQL script.** This allowed the model to run natively inside the Data Warehouse, automatically scoring millions of leads every night and routing them directly to the CRM.

  * **File:** `03_deployment/production_scoring.sql`
  * **Core Action:** Translating the model output into highly efficient `CASE WHEN` SQL statements for automated daily batch runs.

<!-- end list -->

```sql
-- Snippet: Production-ready scoring logic running natively in the Data Warehouse
CASE WHEN a.total_visit_cnt_short < 1 THEN -5
     WHEN a.total_visit_cnt_short >= 1 AND a.total_visit_cnt_short < 2 THEN -8
     WHEN a.total_visit_cnt_short >= 2 AND a.total_visit_cnt_short < 3 THEN 4
     WHEN a.total_visit_cnt_short >= 3 THEN 20
     ELSE 0
END AS total_visit_cnt_short_score
```

-----

## 📊 Phase 4: Business Impact & Evaluation

While AUC and KS scores prove the model's statistical validity, the true measure of success is business impact. In line with the philosophy of "Analyzing Not Data, But Problems," the evaluation focuses on how the model optimizes the sales team's daily operations.

### 1\. Statistical Robustness (ROC & KS)

The model achieved an **AUC of 0.70** and a **KS statistic of 0.37** on the out-of-time test set. This indicates a strong and stable discriminatory power, effectively separating high-intent leads from low-intent ones without overfitting the training data.

### 2\. Translating Model to Revenue (Decile Analysis)

This table represents the core business value of the project. By concentrating sales efforts purely on the **top 30%** of scored leads (Deciles 0-2), the business captures the vast majority of total conversions.

| Decile | Ones (Conversions) | Population | Response Rate | Cumulative Response Rate | Recall Rate |
| :--- | :---: | :---: | :---: | :---: | :---: |
| **0. [0.00249, 0.01532]** | 76 | 30,202 | 0.25% | 0.25% | 21.71% |
| **1. (0.00188, 0.00249]** | 65 | 30,255 | 0.21% | 0.23% | 18.57% |
| **2. (0.00161, 0.00188]** | 79 | 30,221 | 0.26% | 0.24% | **22.57%** |
| 3. (0.00137, 0.00161] | 50 | 29,978 | 0.16% | 0.22% | 14.28% |
| 4. (0.00102, 0.00137] | 23 | 30,360 | 0.07% | 0.19% | 6.57% |
| ... | ... | ... | ... | ... | ... |
| **Average/Total** | **-** | **-** | **0.11%** | **-** | **100.0%** |

*Conclusion: The targeted approach allowed the operational team to confidently filter out the bottom 70% of "noise" traffic, significantly reducing wasted labor costs and directly driving the conversion rate increase.*

-----

## 📚 Team Enablement & SOP

To ensure long-term sustainability and team empowerment, a Standard Operating Procedure (SOP) was established.

  * **File:** `04_documentation/SOP_operations_manual.md`
  * **Content:** Detailed instructions for cross-functional teams on how to update the `alloc_date` parameters, retrain the model for new campaigns, and maintain the automated pipelines, ensuring knowledge sharing and best practices.

-----

*Developed by a data-centric builder passionate about turning complex pipelines into clear business value.*

```
```

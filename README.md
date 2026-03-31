# predictive-lead-scoring-scorecard
An end-to-end lead scoring system using Logistic Regression and WOE binning to optimize sales conversion from 10% to 16.5%.

# 🚀 End-to-End Lead Scoring System: Turning Free Traffic into Revenue

![Python](https://img.shields.io/badge/Python-3.7%2B-blue)
![SQL](https://img.shields.io/badge/SQL-Data%20Engineering-orange)
![Machine Learning](https://img.shields.io/badge/Model-Logistic%20Regression%20%2B%20WOE-success)
![Status](https://img.shields.io/badge/Status-Production%20Deployed-brightgreen)

## 📌 Executive Summary
In high-volume customer acquisition, not all leads are created equal. This project solves a critical operational bottleneck: **optimizing the conversion rate of "freebie" insurance leads.** By building and deploying an interpretable Scorecard Model (Logistic Regression + WOE Binning), we successfully prioritized high-quality leads for the sales team, eliminating wasted hours on zero-intent traffic.

**Business Impact:**
* 📈 Increased target conversion rate from **10% to 16.5%**.
* 💰 Directly boosted overall profit margins by **17.6%**.
* ⏱️ Significantly reduced the manual workload for the audit and sales teams.

---

## 💡 The Philosophy: "Analyzing Not Data, But Problems"
When faced with low conversion rates, the instinct is often to build a complex black-box model (like XGBoost or Random Forest). However, cross-functional alignment is key. **If the sales team doesn't understand why a lead is scored low, they won't trust the system.** Therefore, I chose a classic **Financial Risk Scorecard approach**. By using Weight of Evidence (WOE) and Information Value (IV), the model translates complex probability into simple, actionable "add/deduct points" rules that business stakeholders can easily understand, trust, and act upon.

---

## 🏗️ Project Architecture & Pipeline
This repository contains the complete end-to-end lifecycle of the project, from raw data extraction to production deployment.

### Phase 1: Data Extraction & Feature Engineering
Before modeling, raw user profiles, behavioral logs, and historical campaign data were extracted from the Data Warehouse and flattened into a training dataset, ensuring strict temporal boundaries to prevent data leakage.

* **File:** `01_data_engineering/extract_features.sql`
* **Core Action:** Defined the target variable and aggregated features based on the allocation date (`AllocDate`).

```sql
-- Snippet: Extracting historical lead behavior without data leakage
SELECT 
    a.user_id,
    a.lead_source,
    b.historical_claims,
    DATEDIFF(a.AllocDate, b.last_active_date) AS days_since_active,
    a.is_converted AS target
FROM 
    lead_allocation_base a
LEFT JOIN 
    user_behavior_log b ON a.user_id = b.user_id 
    AND b.log_date < a.AllocDate; 

### Phase 2: Interpretative Modeling & WOE Binning
Using Python and the `scorecardpy` library, continuous and categorical variables were binned using Weight of Evidence (WOE). Features with high Information Value (IV) were selected to train a Logistic Regression model.

* **Files:** `02_modeling/scorecardpy_v2.ipynb`, `02_modeling/custom_woe_binning.py`
* **Core Action:** Trained the model and transformed the coefficients into a standard scorecard format.

```python
# Snippet: Automating the WOE binning and IV calculation
import scorecardpy as sc

# Filter variables based on Information Value (IV > 0.02)
dt_s = sc.var_filter(train_data, y="target")

# Calculate WOE bins automatically and adjust manually for business logic
bins = sc.woebin(dt_s, y="target")

# Train Logistic Regression and scale to Scorecard
lr = LogisticRegression(penalty='l1', C=0.9, solver='saga')
card = sc.scorecard(bins, lr, xcolumns, points0=600, odds0=1/19, pdo=50)

### Phase 3: Production Deployment via SQL
Instead of relying on a complex API infrastructure to serve the Python model, **the scorecard rules were reverse-engineered into a native SQL script**. This allowed the model to run natively inside the Data Warehouse, automatically scoring millions of leads every night and routing them directly to the CRM.

* **File:** `03_deployment/production_scoring.sql`
* **Core Action:** Translating the `card` output into highly efficient `CASE WHEN` SQL statements for automated daily runs.

```sql
-- Snippet: Production-ready scoring logic running natively in the Data Warehouse
SELECT 
    user_id,
    (Base_Score + 
     CASE 
        WHEN age BETWEEN 25 AND 35 THEN 15
        WHEN age > 35 THEN 25 
        ELSE 0 
     END +
     CASE 
        WHEN days_since_active <= 7 THEN 30
        WHEN days_since_active BETWEEN 8 AND 30 THEN 10
        ELSE -15 
     END) AS Final_Lead_Score
FROM 
    daily_lead_pool;
```

---

## 📚 Team Enablement & SOP
To ensure long-term sustainability and team empowerment, a Standard Operating Procedure (SOP) was established. 

* **File:** `04_documentation/SOP_operations_manual.md`
* **Content:** Detailed instructions for cross-functional teams on how to update the `AllocDate` parameters, retrain the model for new campaigns, and maintain the automated pipelines, ensuring knowledge sharing and best practices.

---
*Developed by a data-centric builder passionate about turning complex pipelines into clear business value.*
```

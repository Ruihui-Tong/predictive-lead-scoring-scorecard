"""
Project: End-to-End Lead Scoring System
Script: 02_modeling/scorecard_model_training.py
Description: 
    This script performs Interpretative Modeling using Logistic Regression and 
    Weight of Evidence (WOE) binning. It includes robust feature selection (L1 penalty), 
    manual business-logic bin adjustments, decile analysis for business evaluation, 
    and translates the model into a standard Financial Risk Scorecard.
"""

import pandas as pd
import numpy as np
import scorecardpy as sc
import warnings
from sklearn.linear_model import LogisticRegression
from sklearn import metrics

# Set pandas display options for better readability
pd.set_option('display.max_columns', None)
pd.set_option('precision', 4)
warnings.filterwarnings('ignore')

# ==========================================
# 1. Data Loading & Preparation
# ==========================================
print("[INFO] Loading dataset...")
# In production, data is fetched via Data Warehouse connections. 
# For this portfolio, we load the sanitized extract from Phase 1.
dst = pd.read_csv('../01_data_engineering/feature_extract_201912.csv')

# Explicitly cast IDs to object to prevent them from being treated as numerics
dst['user_id'] = dst['user_id'].astype(object)

# Define Target, IDs, and features to explicitly skip during binning
target = 't14_policy_flag'
id_list = ['user_id', 'task_id', 'target_user_mobile']
skip_list = [
    'device_type', 'alloc_date', 'first_chance_name', 'agent_type',
    'datediff_lastshare', 'sdb_normal_order_amt', 'sdb_normal_order_num',
    'sdb_normal_pay_order_amt', 'sdb_order_amt', 'sdb_order_num', 'sdb_pay_order_amt'
]
skip_all_list = id_list + skip_list

# Split Data into Train and Test (70/30 split)
print("[INFO] Splitting dataset...")
train, test = sc.split_df(dst, target, ratio=0.7, seed=20200107).values()


# ==========================================
# 2. Automated & Manual WOE Binning
# ==========================================
print("[INFO] Calculating initial WOE bins...")
# Initial automated tree-based binning
bins = sc.woebin(train, y=target, method='tree', var_skip=skip_all_list)

# --- Manual Bin Adjustments (Business Logic Alignment) ---
# Aligning categorical breaks with the English labels generated in Phase 1 SQL
breaks_adj = {
    'author_flag': [1],
    'cf_fst_donate_amt': [1, 15, 50],
    'cf_last_donate_amt': [1, 15, 50],
    'cf_max_donate_amt': [1, 15, 25, 100],
    'cf_min_donate_amt': [1, 6, 20, 50],
    'channel_group': ["sdbao", "sdhz", "cf", "feed", "BD", "others", "qywx"],
    'channel_type': ["Internal", "Ads", "Community", "Others"], 
    'datediff_cf_fst_donate': ["missing", 181, 361, 541, 721],
    'datediff_first_visit_bao': ["missing", 10, 80, 270],
    'datediff_last_visit_hz': ["missing", 91, 181, 361],
    'datediff_first_oper_short': ["missing", 2, 5, 8],
    'donate_amt': [1, 50, 100, 150],
    'hz_order_num': [1, 2, 3, 5],
    'hz_total_bal_amt': [1, 4],
    'hz_total_charge_amt': [1, 14, 62],
    'id_age': ["missing", 18, 24, 31, 41, 50],
    
    # Geographic tiers matching SQL output
    'id_city_class': ["New_Tier_1", "Tier_1", "Tier_2", "Tier_3", "Tier_4", "Tier_5", "others", "unknown"],
    'last_visit_city_class': ["New_Tier_1", "Tier_1", "Tier_2", "Tier_3", "Tier_4", "Tier_5", "others", "unknown"],
    'mobile_city_class': ["New_Tier_1", "Tier_1", "Tier_2", "Tier_3", "Tier_4", "Tier_5", "others", "unknown"],
    'most_visit_city_class': ["New_Tier_1", "Tier_1", "Tier_2", "Tier_3", "Tier_4", "Tier_5", "others", "unknown"],
    'wx_city_class': ["New_Tier_1", "Tier_1", "Tier_2", "Tier_3", "Tier_4", "Tier_5", "others", "unknown"],
    
    'id_gender': ["missing", "M", "F"],
    'wx_sex': ["unknown", "M", "F"],
    'mobile_operator': ["Operator_A", "Operator_C", "Operator_B", "others"],
    'total_visit_cnt_short': [1, 2, 3],
    'visit_cnt_bao': [7, 16, 25],
    'visit_cnt_hz': [1, 6, 12, 22, 66]
}

print("[INFO] Applying manual WOE adjustments...")
bins_adj = sc.woebin(train, y=target, breaks_list=breaks_adj, var_skip=skip_all_list)

# Transform raw data into WOE values
train_woe = sc.woebin_ply(train, bins_adj)
test_woe = sc.woebin_ply(test, bins_adj)


# ==========================================
# 3. Model Training & Feature Selection
# ==========================================
# Initial full feature set
y_train = train_woe[target]
X_train = train_woe.drop(columns=[target])

print("[INFO] Performing Feature Selection using L1 Penalty (Lasso)...")
# First Pass: Use L1 Penalty to force weak feature coefficients to zero
lr_initial = LogisticRegression(penalty='l1', C=0.1, solver='saga', n_jobs=-1, random_state=202001)
lr_initial.fit(X_train, y_train)

# Filter features strictly based on non-zero coefficients and business intuition
selected_features = [
    'wx_city_class_woe', 'channel_group_woe', 'id_age_woe', 
    'hz_total_bal_amt_woe', 'total_visit_cnt_short_woe', 
    'sdb_app_start_flag_woe', 'datediff_last_visit_hz_woe', 
    'hz_eff_order_num_woe'
]

X_train_final = X_train[selected_features]
X_test_final = test_woe[selected_features]
y_test = test_woe[target]

print("[INFO] Training Final Logistic Regression Model...")
lr_final = LogisticRegression(penalty='l1', C=0.1, solver='saga', n_jobs=-1, random_state=202001)
lr_final.fit(X_train_final, y_train)


# ==========================================
# 4. Model Evaluation & Decile Analysis
# ==========================================
def get_deciles_analysis(df, score_col, target_col):
    """
    Business evaluation metric: Generates a decile analysis table to track 
    cumulative response rate and recall rate across score buckets.
    """
    df1 = df[[score_col, target_col]].dropna().copy()
    _, bins = pd.qcut(df1[score_col], 10, retbins=True, duplicates='drop')
    bins[0] -= 0.00001
    bins[-1] += 0.00001
    
    bins_labels = [f'{9-i}.({bins[i]:.5f},{bins[i+1]:.5f}]' for i in range(len(bins)-1)]
    bins_labels[0] = bins_labels[0].replace('(', '[')
    
    df1['Decile'] = pd.cut(df1[score_col], bins=bins, labels=bins_labels)
    df1['Population'] = 1
    df1['Ones'] = df1[target_col]
    
    summary = df1.groupby(['Decile'])[['Ones', 'Population']].sum().sort_index(ascending=False)
    summary['ResponseRate'] = summary['Ones'] / summary['Population']
    summary['CumulativeResponseRate'] = summary['Ones'].cumsum() / summary['Population'].cumsum()
    summary['RecallRate'] = summary['Ones'] / summary['Ones'].sum()
    summary['CumulativeRecallRate'] = summary['Ones'].cumsum() / summary['Ones'].sum()
    
    return summary

print("[INFO] Evaluating Model Performance...")
train_pred = lr_final.predict_proba(X_train_final)[:, 1]
test_pred = lr_final.predict_proba(X_test_final)[:, 1]

# Calculate metrics
fpr, tpr, _ = metrics.roc_curve(y_test, test_pred)
print(f"Test AUC: {metrics.auc(fpr, tpr):.4f}")
print(f"Test KS: {np.max(np.abs(tpr - fpr)):.4f}")

# Generate Decile Analysis for Business Stakeholders
test_results = pd.DataFrame({"pred": test_pred, "real": y_test})
decile_summary = get_deciles_analysis(test_results, score_col="pred", target_col="real")
print("\n[INFO] Decile Analysis (Top 3 Deciles):")
print(decile_summary.head(3))


# ==========================================
# 5. Scorecard Generation
# ==========================================
print("[INFO] Converting Logistic Regression to Standard Scorecard...")
# Base Score: 600 points, Odds: 1/19, PDO (Points to Double Odds): 50
card = sc.scorecard(bins_adj, lr_final, X_train_final.columns, points0=600, odds0=1/19, pdo=50)

# Format into a neat DataFrame for deployment
card_df = pd.concat(card.values(), ignore_index=True)
print("\n[INFO] Scorecard Snippet:")
print(card_df[['variable', 'bin', 'points']].head(10))

# Save for SQL deployment
card_df.to_csv('final_scorecard_rules.csv', encoding='utf_8_sig', index=False)
print("[INFO] Scorecard rules saved. Ready for SQL injection.")

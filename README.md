---
title: "Scalable Customer Segmentation"
author: "Demward"
date: "2026-03-02"
output: github_document
---

# Customer Segmentation Analysis
## Scalable Clustering with PySpark and KMeans

This project demonstrates a scalable machine learning pipeline to segment customers based on purchasing behavior using **PySpark** on the **Databricks** platform.

---

# Getting Started

### Data Setup
To begin, unzip the `online_retail_09_10.csv.zip` and `online_retail_10_11.csv.zip` files. This will provide the raw CSV datasets for the analysis.

### Databricks Data Ingestion
Follow these steps to upload and register your data in the Databricks Catalog:

**Step 1.** Navigate to the **Catalog** on the left-hand sidebar.

<img width="204" height="314" alt="Step1_Click_on_Catalog" src="https://github.com/user-attachments/assets/fc94f031-2ab1-41d2-bd38-568f73b6632f" />

**Step 2.** Select **Create** > **Add data**. 

<img width="1072" height="418" alt="Step2_Add_data" src="https://github.com/user-attachments/assets/59319562-8943-46ac-9fe9-3fd14bee59c9" />

**Step 3.** Choose **Create or modify table**. 

<img width="979" height="362" alt="Step3_Create_or_modify_table" src="https://github.com/user-attachments/assets/21fbb447-04ac-43b9-b3f5-c5e4ce5c9c5b" />

**Step 4.** Upload your CSV files. Since both files share the same schema, they can be processed simultaneously.

<img width="1104" height="605" alt="Step4_Select_files" src="https://github.com/user-attachments/assets/cbfe60bc-7052-4d2e-af9c-ddc39edec000" />

**Step 5.** Review the table preview, adjust the schema if necessary, and name the table `online_retail`. Click **Create table** to finalize.

<img width="1902" height="892" alt="Step5_Create_Table_from_online_retail_datasets" src="https://github.com/user-attachments/assets/fbcaea87-3b9c-4e32-b39d-b13f39966540" />

---

# Data Extraction & Feature Engineering
In this phase, we extract the raw data and engineer features that represent customer value (RFM Analysis).

**1. Initial Data Load:**
<img width="960" height="354" alt="Code1_Data_Extraction" src="https://github.com/user-attachments/assets/a8941d02-2c22-46df-9b51-9b4b24bed6e0" />

**2. Feature Engineering:**
We transform the transactional data into a customer-centric dataframe using the following metrics:
* **Recency:** Days since the last purchase (relative to 2012-12-10).
* **Age:** Days since the first purchase (relative to 2012-12-10).
* **Frequency:** Total number of transactions per customer.
* **Diversity:** Count of unique items purchased.
* **Monetary:** Total spend per customer.
* **Average Ticket:** Average spend per customer

<img width="940" height="551" alt="Code2_Create_CustomerGrouped_dataframe" src="https://github.com/user-attachments/assets/7a55dd23-7d41-4157-8ede-78c0191aa362" />

**3. Final Feature Set:**
<img width="928" height="336" alt="Code3_Customers_with_key_features" src="https://github.com/user-attachments/assets/3de6d71c-eb0a-486b-a778-1bc9602a9622" />

---

# Data Preprocessing

### Outlier Removal
To ensure model stability, we remove outliers from the `monetary` variable using the **Interquartile Range (IQR)** method:

$$IQR = Q_3 - Q_1$$

The bounds are defined as:
* **Lower Bound:** $Q_1 - 1.5 \times IQR$
* **Upper Bound:** $Q_3 + 1.5 \times IQR$

### Principal Component Analysis (PCA)

To reduce computational overhead and simplify the model, I applied Principal Component Analysis (PCA). To determine the optimal number of components, I analyzed the cumulative explained variance. I selected a threshold of 95% to ensure maximum information retention with minimal complexity.

<img width="442" height="124" alt="PCA_cummulative_variance" src="https://github.com/user-attachments/assets/cdbe676c-9f92-4279-bcbe-74bd6d56e421" />

In this instance, 3 components were selected, as they account for 96% of the total variance.

---

# KMeans Modeling

### Hyperparameter Tuning: The Elbow Method
To determine the optimal number of clusters ($k$), we evaluate the cost function across a range of values.

<img width="571" height="437" alt="image" src="https://github.com/user-attachments/assets/2585eb6b-e205-4aaf-b6e0-aa29508b6993" />

Based on the "elbow" observed in the plot, **$k=3$** was selected as the optimal cluster count.

### Implementation & Evaluation
The KMeans algorithm was trained using PySpark's distributed engine to ensure scalability.
<img width="920" height="351" alt="Code8_KMeans" src="https://github.com/user-attachments/assets/8d61ee3a-1cf9-4417-911b-dd09deb6489f" />

**Performance Metric:** The model achieved a **Silhouette Score of 0.63**, indicating high cluster density and clear separation between segments.

---

# Cluster profiling 
Before profiling, I validated the distinctness of the customer segments by performing a **Multivariate Analysis of Variance (MANOVA)** across six key features: Recency, Frequency, Monetary value, Average Ticket, Age, and Diversity. With a resulting **p < 0.05**, I rejected the null hypothesis, statistically confirming that the identified groups are significantly different from one another.

**Segment 0: "The Champions" (n=1,712)** 
* **The Data:** Lowest Recency (79 days), Highest Age (611.8 days), Highest Frequency (8.8), Highest Spend ($2,216).
* **Insight:** These are your most active and loyal customers. With the lowest recency, highest frequency and highest age, they are in a virtuous cycle of buying. They are the core engine of your revenue.
* **Action:** Implement a VIP Loyalty Program. Focus on retention and brand advocacy.

**Segment 2: "Occasional Explorers" (n=1,715)** 
* **The Data:** Moderate Recency (86 days), Low Frequency (2.8), and High Diversity (42 items).
* **Insight:** This segment displays a bimodal age distribution, mixing true "New" acquisitions (Age $\approx$ 1) with "Legacy" customers (Age > 400) who have become infrequent. Despite low visit frequency, their **High Average Ticket ($283)** indicates high potential value per transaction.
* **Action:**
* * **For New Leads:** Implement personalized "Next-Best-Offer" recommendations to build habit.
* * **For Legacy Leads:** Deploy re-activation "Win-back" campaigns with limited-time incentives to reduce churn risk.
 
**Segment 1: "Lost Customers" (n=1,796)**
* **The Data:** Extremely High Recency (471 days), High Age (565 days), Low Frequency (2.44), and Moderate Monetary ($552).
* **Insight:** These are "One-and-Done" or early-lifecycle customers who joined nearly a year and a half ago but stopped engaging shortly after. With an average Recency of 471 days, they are well beyond the standard churn window. The fact that their Min Recency is 144 days suggests that not a single person in this group has shopped in the last 4–5 months.
* **Action:**  Win-Back or Archive: This group requires aggressive "Win-back" offers (e.g., "We Miss You" deep discounts) to determine if they are still reachable.
* * Churn Analysis: Use this group to study why they left. Since their Diversity (33) is the lowest of all groups, they may not have found enough variety to keep them interested.
* * Cost Optimization: If re-activation fails, remove them from active marketing spend (emails/SMS) to optimize ROI.


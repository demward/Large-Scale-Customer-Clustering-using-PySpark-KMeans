---
title: "Scalable Customer Segmentation"
author: "Demward"
date: "2026-02-04"
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
* **Frequency:** Total number of transactions per customer.
* **Diversity:** Count of unique items purchased.
* **Monetary:** Total spend per customer.

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

### ML Pipeline Preparation
Since PySpark MLlib requires numerical input vectors, we implement the following transformation steps:

1. **StringIndexer:** Converts categorical country labels into numerical indices.
<img width="989" height="397" alt="Code4_StringIndexer" src="https://github.com/user-attachments/assets/a4d0a23b-671e-445e-9841-edbd0745533a" />

2. **OneHotEncoder (OHE):** Transforms indexed categories into binary vectors to avoid assuming an ordinal relationship between countries.
<img width="933" height="356" alt="Code5_OHE" src="https://github.com/user-attachments/assets/808d2e5b-0d7d-4c9b-ba90-7923bb34c984" />

3. **VectorAssembler:** Consolidates all feature columns into a single `features` vector.
<img width="926" height="363" alt="Code6_VectorAssembler" src="https://github.com/user-attachments/assets/f20964e9-18d7-4916-96f8-013cbc27568b" />

---

# KMeans Modeling

### Hyperparameter Tuning: The Elbow Method
To determine the optimal number of clusters ($k$), we evaluate the cost function across a range of values.

<img width="576" height="448" alt="Code7_ElbowMethod" src="https://github.com/user-attachments/assets/9766b55f-5872-4172-9163-b53fdd625200" />

Based on the "elbow" observed in the plot, **$k=4$** was selected as the optimal cluster count.

### Implementation & Evaluation
The KMeans algorithm was trained using PySpark's distributed engine to ensure scalability.
<img width="920" height="351" alt="Code8_KMeans" src="https://github.com/user-attachments/assets/8d61ee3a-1cf9-4417-911b-dd09deb6489f" />

**Performance Metric:** The model achieved a **Silhouette Score of 0.7**, indicating high cluster density and clear separation between segments.

---

# Cluster profiling 

Through K-Means clustering of transaction data, four customer segments emerged based on their engagement and spending habits. These segments represent a clear hierarchy of customer health, defined by a 'Power User' core (Segment 1) and a large base of lapsed shoppers (Segment 0). The following profiles outline the statistical behavior and strategic recommendations for each group.

**Segment 1: "The Champions" (n=409)** 
* **The Data:** Lowest Recency (73 days), Highest Frequency (12.8), Highest Spend ($4,045).
* **Insight:** These are your most active and freshest customers. With the lowest recency and highest frequency, they are in a virtuous cycle of buying. They are the core engine of your revenue.
* **Action:** Implement a VIP Loyalty Program. Focus on retention and brand advocacy.

**Segment 3: "Active Loyalists" (n=707)**
* **The Data:** Low Recency (104 days), High Frequency (9.2), High Spend ($2,439).
* **Insight:** A very healthy segment. While they don't spend as much as Segment 1, they are still very recent (shopping roughly every 3.5 months). They are highly engaged and represent the best opportunity for upselling.
* **Action:** Personalized Recommendations. Use their purchase history to suggest higher-value items to move them toward Segment 1.

**Segment 2: "Fading/Occasional Shoppers" (n=1,423)**
* **The Data:** Moderate Recency (156 days), Moderate Frequency (5.0), Moderate Spend ($1,192).
* **Insight:** This group is at a crossroads. A mean recency of 156 days (~5 months) suggests they are starting to drift away. Because the standard deviation (165) is larger than the mean, this group is highly polarized: some are still active, but many have already crossed the 300-day mark.
* **Action:** Re-activation Campaigns. Targeted "limited time" offers are needed here to trigger a purchase before they lapse into Segment 0.

**Segment 0: "Lapsed/Inactive" (n=2,705)**
* **The Data:** Highest Recency (299 days), Lowest Frequency (2.0), Lowest Spend ($343).
* **Insight:** These customers are largely cold. With a mean recency of nearly 300 days and only 2 purchases on average, these are likely one-time shoppers or people who have switched to a competitor.
* **Action:** Win-Back or Sunset. Attempt one deep-discount "Win-back" campaign. If they don't respond, it is more cost-effective to stop marketing to them to save on campaign costs.

# Getting started
First, you need to unzip the documents "online_retail_09_10.csv.zip" and "online_retail_09_10.csv.zip" this will generate two csv files. 

Once you have the two csv files we upload the files on databricks following the next steps: 

Step 1. Click on Catalog on the left panel.

<img width="204" height="314" alt="Step1_Click_on_Catalog" src="https://github.com/user-attachments/assets/fc94f031-2ab1-41d2-bd38-568f73b6632f" />

Step 2. Click on Create > Add data. 

<img width="1072" height="418" alt="Step2_Add_data" src="https://github.com/user-attachments/assets/59319562-8943-46ac-9fe9-3fd14bee59c9" />

Step 3. Click on Create or modify table. 

<img width="979" height="362" alt="Step3_Create_or_modify_table" src="https://github.com/user-attachments/assets/21fbb447-04ac-43b9-b3f5-c5e4ce5c9c5b" />

Step 4. Drag or browse the csv files (we can upload both tables at the same time because they have the same columns with the same format).

<img width="1104" height="605" alt="Step4_Select_files" src="https://github.com/user-attachments/assets/cbfe60bc-7052-4d2e-af9c-ddc39edec000" />

Step 5. In this window you can see a preview of the table, change the schema, rename the table, once you are satisfied with the schema and name the table (I named it as online_retail) click on Create table.

<img width="1902" height="892" alt="Step5_Create_Table_from_online_retail_datasets" src="https://github.com/user-attachments/assets/fbcaea87-3b9c-4e32-b39d-b13f39966540" />

After the previous steps you will have the table ready to use on the project. 

# Data Extraction

In this section we will extract and preparate the data for the model. 

First, we will read and see the date

<img width="960" height="354" alt="Code1_Data_Extraction" src="https://github.com/user-attachments/assets/a8941d02-2c22-46df-9b51-9b4b24bed6e0" />

After that, we will create new interesting variables. 

* lest_puchase: Last purchase of the customer.
* frequency: Number of times the customer bought in the store.
* diversity: Different number of items the customer bought.
* monetary: Money spended from the customer.

and create a new dataframe grouped by CustomerID and Country.

<img width="940" height="551" alt="Code2_Create_CustomerGrouped_dataframe" src="https://github.com/user-attachments/assets/7a55dd23-7d41-4157-8ede-78c0191aa362" />

We replace last purchase with recency and create the dataframe with key features.

* recency: Number of days from the last purchase to current date (2012-12-10)

<img width="928" height="336" alt="Code3_Customers_with_key_features" src="https://github.com/user-attachments/assets/3de6d71c-eb0a-486b-a778-1bc9602a9622" />

# Deleting outliers 

We delete outliers from monetary value using IQR method where IQR it's defined as IQR = Q3 - Q1 and the bounds are defined by: 
* lower bound = Q1 - 1.5 IQR
* upper bound = Q3 + 1.5 IQR

We delete everything out of the bounds.

# Data preparation for PySpark model. 

## One Hot Categorical Data 

For One Hot Encoding, first we need to use String Indexer to assign a index to each categorical label (In this case one index for each Country). 

<img width="989" height="397" alt="Code4_StringIndexer" src="https://github.com/user-attachments/assets/a4d0a23b-671e-445e-9841-edbd0745533a" />

After that we will One Hot Encode the countries. 

<img width="933" height="356" alt="Code5_OHE" src="https://github.com/user-attachments/assets/808d2e5b-0d7d-4c9b-ba90-7923bb34c984" />

## Vector Assembler 

Once we have everything as numerical variable, we use VectorAseembler to prepare the features for the KMeans model.

<img width="926" height="363" alt="Code6_VectorAssembler" src="https://github.com/user-attachments/assets/f20964e9-18d7-4916-96f8-013cbc27568b" />

# KMeans 

## Elbow method

Before of doing the K Means, we need to define the number of clusters, for that we will use the Elbow Method. 

<img width="576" height="448" alt="Code7_ElbowMethod" src="https://github.com/user-attachments/assets/9766b55f-5872-4172-9163-b53fdd625200" />

From this image, we can see that the optimal number of clusters is 4.

## K Means Implementation 

We implemnt K Means with PySpark. 

<img width="920" height="351" alt="Code8_KMeans" src="https://github.com/user-attachments/assets/8d61ee3a-1cf9-4417-911b-dd09deb6489f" />

After that we evaluate the model and we have a silhouette score of 0.7

# Future work 

For the future I want to analyze each cluster to find similarities between customers to do marketing targeted for the best customers.

# Azure Databricks ETL pipeline

The ETL pipeline used for this project is NYC yellow, green and fhv taxi trips data. 

**ETL process**
1. The data is extracted in a raw format from Azure Data Lake gen 1 (**Bronze**) 
2. Transfromed to add KPIs (**Silver**) 
3. Facts and reports are created (**Gold**)
4. The reports are then stored it in a **delta lake** in **Azure Datalake Gen 2 storage**

ETL code for respective taxi trips can be found here https://github.com/santhoshraj2960/AzureDatabricksLearn/tree/main/notebooks/ETLProdNotebooks/

Two CI/CDs have been developed for this project (**Azure devops and Jenkins**). Both of them perform the following tasks
1. Detect push (or PR merge) to main branch
2. Deploy to staging databricks workspace
3. Run unit tests (This part is under construction)
4. Delply to production databricks workspace

**CI/CD Azure devops**

**CI/CD Jenkins**



# ETL pipeline for generating facts of different NYC taxi

## Order of execution of tasks graph
![alt text](https://github.com/santhoshraj2960/airflow_pluralsight/blob/main/screenshots/tasks_graph.png)

## Execution tree
![alt text](https://github.com/santhoshraj2960/airflow_pluralsight/blob/main/screenshots/tasks_tree.png)


## Databricks ETL jobs
![alt text](https://github.com/santhoshraj2960/airflow_pluralsight/blob/main/screenshots/azure_databricks_jobs.png)

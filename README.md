# Azure Databricks ETL pipeline

The ETL pipeline used for this project is to extract, tranform and load New York City's yellow, green and fhv taxi trips. 

## ETL process
1. The data is extracted in a raw format from Azure Data Lake gen 1 (**Bronze**) 
2. Transfromed to add KPIs (**Silver**) 
3. Facts and reports are created and stored in **delta lake** in **Azure Datalake Gen 2 storage** (**Gold**)

ETL code for respective taxi trips can be found here 
[ETL notebooks](https://github.com/santhoshraj2960/AzureDatabricksLearn/tree/main/notebooks/ETLProdNotebooks/)

Same CI / CD process is implemented using 2 different tools (**Azure devops and Jenkins**). Both of them perform the following tasks
1. Detect push (or PR merge) to main branch
2. Deploy to staging databricks workspace
3. Run unit tests (This part is under construction)
4. Delply to production databricks workspace

## CI/CD Jenkins
**Refer to [Jenkins file](https://github.com/santhoshraj2960/ETL-Databricks-Azure/blob/main/Jenkinsfile) in the home directory to see jenkins workflow**
<br/>
<br/>
![alt text](https://github.com/santhoshraj2960/ETL-Databricks-Azure/blob/main/screenshots/Jenkins.png)
<br/>
<br/>
## CI/CD Azure devops
![alt text](https://github.com/santhoshraj2960/ETL-Databricks-Azure/blob/main/screenshots/AzureDevops.png)
![alt text](https://github.com/santhoshraj2960/ETL-Databricks-Azure/blob/main/screenshots/AzureDevops2.png)
<br/>
<br/>

## ETL pipeline for generating facts of different NYC taxi 

- Please refer this repo for the [Airflow workflow orchestration code](https://github.com/santhoshraj2960/Airflow-ETL-workflow)

## Order of execution of tasks graph (DAG)
![alt text](https://github.com/santhoshraj2960/Airflow-ETL-workflow/blob/main/screenshots/tasks_graph.png)
<br/>
<br/>
## Execution tree
![alt text](https://github.com/santhoshraj2960/Airflow-ETL-workflow/blob/main/screenshots/tasks_tree.png)
<br/>
<br/>
## Databricks ETL jobs
![alt text](https://github.com/santhoshraj2960/Airflow-ETL-workflow/blob/main/screenshots/azure_databricks_jobs.png)

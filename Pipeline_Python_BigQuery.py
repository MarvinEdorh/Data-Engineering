################################# Data Pipeline Python to Google Cloud Platform BigQuery ####################################

import os; os.chdir('C:/Users/marvin/Desktop/SQL BigQuery')

import numpy as np ; import pandas as pd ; from google.cloud import bigquery

#https://cloud.google.com/docs/authentication/production

client = bigquery.Client.from_service_account_json(
json_credentials_path='C:/Users/marvin/Desktop/SQL BigQuery/data_pipeline-bbc9aec8eae9.json', 
project='data_pipeline')

query = """
SELECT fullvisitorid AS ID_Visitor,SUM(totals.visits) AS Visits 
FROM `bigquery-public-data.google_analytics_sample.ga_sessions_*` 
GROUP BY fullvisitorid ORDER BY Visits DESC """

query_results = client.query(query) ; query_results = query_results.result()

ID_Visitor = [] ; Visits = []

for row in query_results: 
    ID_Visitor.append(row[0]) 
    Visits.append(row[1])

BigQuery_table = { 'ID_Visitor' : ID_Visitor, 'Visits' : Visits } ; BigQuery_table = pd.DataFrame(BigQuery_table)

from pandas.io import gbq

BigQuery_table.to_gbq(destination_table='test.BigQuery_table', project_id='data_pipeline', if_exists='replace')

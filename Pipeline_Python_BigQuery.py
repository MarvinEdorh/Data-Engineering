############################### Data Pipeline Google Cloud Platform BigQuery to Python ##################################

#Process de connexion des données Google Analytics stoquées dans GCP BigQuery Storage avec une interface Python 
#afin de soulettre ces données requêtées à des méthodes d'analyses exploratoires non réalisables en langage SQL

import os; os.chdir('C:/Users/marvin/Desktop/SQL BigQuery')

import numpy as np ; import pandas as pd ; from google.cloud import bigquery

#Création d'un compte de service GCP : https://cloud.google.com/docs/authentication/production
#Authentification du compte dans Python en ajouant lien du fichier JSON téléchargé en local après la creation de la clé

client = bigquery.Client.from_service_account_json(
json_credentials_path='C:/Users/marvin/Desktop/SQL BigQuery/data_pipeline-bbc9aec8eae9.json', 
project='data_pipeline')

#Requête SQL

query = """
SELECT fullvisitorid AS ID_Visitor,SUM(totals.visits) AS Visits 
FROM `bigquery-public-data.google_analytics_sample.ga_sessions_*` 
GROUP BY fullvisitorid ORDER BY Visits DESC """

query_results = client.query(query) ; query_results = query_results.result()

#Table des résultats 
ID_Visitor = [] ; Visits = []

for row in query_results: 
    ID_Visitor.append(row[0]) 
    Visits.append(row[1])

BigQuery_table = { 'ID_Visitor' : ID_Visitor, 'Visits' : Visits } ; BigQuery_table = pd.DataFrame(BigQuery_table)

#Cette table va pouvoir être soumise à des tests statistiques ou des modèles de machine learning afin d'effectuer 
#par exemple de l'A/B testing ou des analyses predictives (conf repositories Data-Mining & Machine Learning)

#Après analyses, export des résultats vers Google Cloud Platform BigQuery Storage 
#afin de mieux les visualiser sur des outils BI de Data Visualisation comme Tableau ou Data Studio
from pandas.io import gbq
BigQuery_table.to_gbq(destination_table='test.BigQuery_table', project_id='data_pipeline', if_exists='replace')
#copier coller le code d´autorisation dans la console

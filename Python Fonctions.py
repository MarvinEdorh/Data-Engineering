############################### Initiation Python pour Calculs et Fonctions ###################################

import os; os.chdir('C:/Users/marvi/Desktop/MsMDA/AutoFormation/Python')
import pandas as pd
data_test = pd.read_csv('Data_analyst_test.csv', sep=";")

############################################### Fonctions D'aggrégation ########################################## 

data_test.mean() #par défaut moyenne de toutes les colonnes avec variable numérique
data_test['CONNECTED'].sum() #pour une variable precise (ici la variable "CONNECTED")
data_test.min() 
data_test.max()
data_test.prod() 
data_test.var()
data_test.std() #standard deviation (normalisés par défaut par n - 1)
data_test.median() 
data_test.quantile(q = 0.25) #quantile
data_test.mad() #median absolute deviate.
data_test.nunique() #nombre de valeurs uniques.
data_test.rank() #rang dans le groupe.

#dataframe avec la moyenne de toutes les variables numérique
data_test_avg=pd.DataFrame(data_test.mean(), columns = ['Avg'])
print(data_test_avg)
 
#dataframe avec la sum de toutes les variables numérique groupée par une vriable
data_test_sum_OS = pd.DataFrame(data_test.groupby('OS').sum()) 
print(data_test_sum_OS)

#dataframe avec la sum de toutes les variables numérique groupée par plusieurs vriable
data_test_avg_OS_COUNTRY = pd.DataFrame(data_test.groupby(['OS','COUNTRY']).sum()) 
print(data_test_avg_OS_COUNTRY)

################################ Calculer une nouvelle variable #################################### 
data_test_sum_OS['average_likes_sent'] = data_test_sum_OS['LIKES_SENT'] / data_test_sum_OS['CONNECTED']

import os; os.chdir('C:/Users/marvi/Desktop/MsMDA/AutoFormation/Python')
import numpy as np ; import pandas as pd

ar = np.array([[1.1, 2, 3.3, 4], [2.7, 10, 5.4, 7], [5.3, 9, 1.5, 15]]) #lignes
df = pd.DataFrame(ar, index = ['a1', 'a2', 'a3'], columns = ['A', 'B', 'C', 'D']) #index

print(df) 

df_2 = pd.DataFrame({'A': [1.1, 2.7, 5.3], 'B': [2, 10, 9], 'C': [3.3, 5.4, 1.5], 'D': [4, 7, 15]}, index = ['a1', 'a2', 'a3'])
#colonnes

print(df_2) 

df_3 = pd.DataFrame({'col1': pd.Series([2, 3, 4], index = ['a', 'b', 'c']), 'col2': pd.Series([6, 7, 8], index = ['b', 'a', 'd'])})

print(df_3) 

df['A']

df['A'][0:3] #les 3 premières valeurs des 3 premières lignes de la colonne 'A' (sous forme de Series)

df.loc['a2'] #renvoie la Series correspondant à la ligne d'index a2

df.loc[['a2', 'a3'], ['A', 'C']] # renvoie un dataframe avec un sous-ensemble des lignes et des colonnes 

df.loc[:,['A', 'C']] # toutes les lignes et seulement les colonnes A et B.
df.loc['a2', 'C'] # accès à la valeur de la ligne a2 et de la colonne C : 5.4.
df.at['a2', 'C'] # autre façon recommandée d'accéder à la valeur de la ligne a2 et de la colonne C : 5.4.
df.at['a2', 'C'] = 7 # on peut aussi faire une affectation pour changer la valeur : .
df.at[0, 1] # on peut aussi utiliser des indices numériques : (ou même un mélange des deux).
df[df['A'] > 2] #renvoie un dataframe avec seulement les lignes où la condition est vérifiée 
df.iloc[1] # renvoie la deuxième ligne.
df.iloc[1:3,[0, 2]] # renvoie le dataframe avec les lignes 1 à 3 exclue, et les colonnes numéros 0 et 2.
df.iloc[:,2:4] #renvoie toutes les lignes et les colonnes 2 à 4 exclue.
df.iloc[1,2] #renvoie la valeur à la ligne 2 et la colonne 3.
df.iat[1,2] #renvoie la valeur à la ligne 2 et la colonne 3, mais c'est la façon recommandée d'accéder aux valeurs.
df.iat[1, 2] = 7 # on peut aussi faire une affectation pour changer la valeur 


df = df.rename(columns = {'A': 'aa', 'B': 'bb'}) 

df.rename(columns = {'C': 'A'}) 
# renomme les colonnes A et B en a et b, mais pas les autres s'il y en a d'autres.
df.head(2)

df.rename(index = {0: 'a', 1: 'b'}, inplace = True)  
#on peut aussi utiliser des numéros, ici sur les lignes, et ici en modifiant directement le dataframe.

df['E'] = pd.Series([1, 0, 1], index = ['a1', 'a2', 'a3'])

df.assign(E = df['A'] + df['B'], F = 2 * df['A']) 
#renvoie une copie du dataframe avec deux nouvelles colonnes E et F (sans modifier le dataframe original)

del df['A'] # permet de détruire la colonne A
df_2.drop(['a', 'c'], inplace = True) #détruit les lignes d'index 'a' et 'c'
df.drop(['A', 'C'], axis = 1, inplace = True) #permet de détruire plusieurs colonnes en même temps.
df.drop(columns = ['A', 'C'], inplace = True) #alternative à l'indication de l'axis.
df.drop(index = ['a', 'c'], inplace = True) #alternative à l'indication de l'axis (destruction de lignes)
df.astype(np.float64) # renvoie un dataframe avec toutes les colonnes converties dans le type indiqué
df.astype({'A': int, 'B': np.float64}) 
#renvoie un dataframe avec les colonnes A et B converties selon les types indiqués.
df['A'][df['A'] < 2] = 0 #on peut faire 

df.dropna(how = 'any') 
df.dropna() 
#renvoie un dataframe avec les lignes contenant au moins 
#une valeur NaN supprimée (how = 'all' : supprime les lignes où toutes les valeurs sont NaN)
df.dropna(axis = 1, how = 'any') 
#supprime les colonnes ayant au moins un NaN plutôt que les lignes (le défaut est axis = 0).
df.dropna(inplace = True) #ne renvoie rien, mais fait la modification en place.
df.fillna(0) #renvoie un dataframe avec toutes les valeurs NaN remplacées par 0.
df['A'].fillna(0, inplace = True) #remplace tous les NA de la colonne A par 0, sur place.
df.isnull() #renvoie un dataframe de booléens, avec True dans toutes les cellules non définies.
df = df.replace(np.inf, 99) #remplace les valeurs infinies par 99 (on peut utiliser inplace = True)

df2 = df.copy() # df2 est alors un dataframe indépendant.
df.replace(1, 5) # remplace tous les 1 par 5.
df.replace([1, 2], [5, 7]) #remplace tous les 1 par 5 et tous les 2 par 7.
df.replace({1: 5, 2: 7}) #remplace tous les 1 par 5 et tous les 2 par 7.

df.drop_duplicates() 
#renvoie un dataframe avec les lignes redondantes enlevées 
#en n'en conservant qu'une seule (ici 3 lignes restant)
df.drop_duplicates(keep = False) 
#renvoie un dataframe avec les lignes redondantes toutes enlevées (ici 2 lignes restant)
df.drop_duplicates(inplace = True) # fait la modification en place.
df.drop_duplicates(subset = ['A', 'B']) 
#renvoie un dataframe avec les doublons enlevés en considérant seulement les colonnes A et B, 
#et en renvoyant la 1ère ligne pour chaque groupe ayant mêmes valeurs de A et B.
df.drop_duplicates(subset = ['A', 'B'], keep = 'last') 
# on conserve la dernière ligne plutôt que la première (keep = first, qui est le défaut).

df.T #renvoie le dataframe transposé.

df.sort_index(axis = 0, ascending = False) 
#renvoie un dataframe avec les lignes triées par ordre décroissant des labels (le défaut est ascendant)

############################################################################################################

df.info() #imprime des infos sur le dataframe : les noms et types des colonnes, 
#le nombre de valeurs non nulles et la place occupée.

df.head(2) #renvoie un dataframe avec les 2 premières lignes. 
#Idem avec df.tail(2) pour les deux dernières.

df.head() #les 5 premières

df.tail() #les 5 dernières.

df.columns #les noms des colonnes, par exemple Index(['A', 'B', 'C', 'D'], dtype='object').

df.columns.values #le nom des colonnes sous forme d'array numpy.

df.index #les noms des lignes (individus), par exemple Index(['a1', 'a2', 'a3'], dtype='object').

df.index.values #le nom des lignes sous forme d'array numpy.

df.values #pour récupérer le dataframe sous forme d'array numpy 2d.

df.describe() #renvoie un dataframe donnant des statistiques sur 
#les valeurs (nombres de valeurs, moyenne, écart-type, ...), 
#mais uniquement sur les colonnes numériques (faire df.describe(include = 'all') 

############################################################################################################

data = pd.read_csv('df1.csv', sep=";")

data_2 = pd.read_csv('df2.csv', sep=";")

data_3 = pd.read_csv('df1_bis.csv', sep=";")

data_4 = pd.read_csv('df2_bis.csv', sep=";")

pd.concat([data, data_2])

data_5=pd.merge(data, data_2, on = ['ID']) 
# merge = inner join par défaut, on est facultatif si variable commune aux 2 tables

print(data_5)

data_6 = data.merge(data_2) # inner join méthode 2


data_7 = pd.merge(data, data_4, on = ['ID'], how = 'outer') # full join

print(data_7)

data_8 = pd.merge(data_3, data_2, how = 'left') # left join

print(data_8)

data_9 = pd.merge(data, data_4, how = 'right') # right join

print(data_9)

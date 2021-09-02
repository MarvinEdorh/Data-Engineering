###################################### BigQuery SQL Code & Google Analytics #############################################

###################################################### syntaxe ##########################################################

SELECT fullvisitorid, SUM(totals.visits) #selection
FROM `bigquery-public-data.google_analytics_sample.ga_sessions_*` 
WHERE _TABLE_SUFFIX = '20161201' #conditions
GROUP BY fullvisitorid #aggregation
HAVING SUM(totals.visits) > 0 #filtre
UNION ALL #concatenation / jointure
SELECT fullvisitorid, SUM(totals.visits)
FROM `bigquery-public-data.google_analytics_sample.ga_sessions_*` 
WHERE _TABLE_SUFFIX = '20161202'
GROUP BY fullvisitorid
ORDER BY fullvisitorid

##################################################### selection ########################################################

SELECT DISTINCT fullvisitorid, device.deviceCategory, 10 AS dix,
CASE WHEN device.deviceCategory = "desktop" THEN 1 
     WHEN device.deviceCategory = "tablet" THEN 2 ELSE 3 END
FROM `bigquery-public-data.google_analytics_sample.ga_sessions_20161201` 

##################################################### conditions #######################################################

SELECT DISTINCT fullvisitorid, device.deviceCategory
FROM `bigquery-public-data.google_analytics_sample.ga_sessions_20161201` 
WHERE device.deviceCategory = "desktop" #egale

SELECT DISTINCT fullvisitorid, device.deviceCategory
FROM `bigquery-public-data.google_analytics_sample.ga_sessions_20161201` 
WHERE device.deviceCategory <> "desktop" #different

SELECT DISTINCT fullvisitorid, device.deviceCategory
FROM `bigquery-public-data.google_analytics_sample.ga_sessions_20161201` 
WHERE device.deviceCategory != "desktop" #different

SELECT DISTINCT fullvisitorid, device.deviceCategory
FROM `bigquery-public-data.google_analytics_sample.ga_sessions_20161201` 
WHERE device.deviceCategory IN ("mobile", "tablet") #parmis

SELECT DISTINCT fullvisitorid, device.deviceCategory
FROM `bigquery-public-data.google_analytics_sample.ga_sessions_20161201` 
WHERE (device.deviceCategory LIKE "le%" OR device.deviceCategory LIKE "%le" #commence par ... ou fini par ...
OR device.deviceCategory LIKE "%le%") AND device.deviceCategory NOT LIKE "d%k" #ou contient ... et ne commence et fini par ...

SELECT DISTINCT fullvisitorid, date
FROM `bigquery-public-data.google_analytics_sample.ga_sessions_*` 
WHERE _TABLE_SUFFIX > '20161201' ORDER BY date #superieur à

SELECT DISTINCT fullvisitorid, date
FROM `bigquery-public-data.google_analytics_sample.ga_sessions_*` 
WHERE _TABLE_SUFFIX >= '20161201' ORDER BY date #superieur ou egal à

SELECT DISTINCT fullvisitorid, date
FROM `bigquery-public-data.google_analytics_sample.ga_sessions_*` 
WHERE _TABLE_SUFFIX < '20161201' ORDER BY date DESC #inferieur à

SELECT DISTINCT fullvisitorid, date
FROM `bigquery-public-data.google_analytics_sample.ga_sessions_*` 
WHERE _TABLE_SUFFIX <= '20161201' ORDER BY date DESC #inferieur ou egal à

SELECT DISTINCT fullvisitorid, date
FROM `bigquery-public-data.google_analytics_sample.ga_sessions_*` 
WHERE _TABLE_SUFFIX BETWEEN '20161201' AND '20161231' ORDER BY date #entre

SELECT DISTINCT fullvisitorid
FROM `bigquery-public-data.google_analytics_sample.ga_sessions_20161201`AS ga, 
UNNEST(ga.hits) AS hits 
WHERE hits.transaction.transactionId IS NULL #est vide

SELECT DISTINCT fullvisitorid
FROM `bigquery-public-data.google_analytics_sample.ga_sessions_20161201`AS ga, 
UNNEST(ga.hits) AS hits 
WHERE hits.transaction.transactionId IS NOT NULL #est non vide

###################################################### aggregation ######################################################

SELECT fullvisitorid, 
SUM(totals.visits) #somme
ROUND(AVG(totals.visits),2) #moyenne arrondie
COUNT(DISTINCT device.deviceCategory) #compte
MIN(date), #minimum
MAX(date), #maximum
FROM `bigquery-public-data.google_analytics_sample.ga_sessions_201612*` 
GROUP BY fullvisitorid

###################################################### filtre ##########################################################

SELECT fullvisitorid, SUM(totals.visits) AS visits
FROM `bigquery-public-data.google_analytics_sample.ga_sessions_201612*` 
GROUP BY fullvisitorid
HAVING SUM(totals.visits) >= 2
ORDER BY visits DESC

##################################################### concatenation ###################################################

SELECT DISTINCT fullvisitorid, 
FROM `bigquery-public-data.google_analytics_sample.ga_sessions_20161224` 
UNION ALL #fusion
SELECT DISTINCT fullvisitorid, 
FROM `bigquery-public-data.google_analytics_sample.ga_sessions_20161225`

SELECT DISTINCT fullvisitorid, 
FROM `bigquery-public-data.google_analytics_sample.ga_sessions_20161224` 
INTERSECT DISTINCT #intersection
SELECT DISTINCT fullvisitorid, 
FROM `bigquery-public-data.google_analytics_sample.ga_sessions_20161225` 

SELECT DISTINCT fullvisitorid, 
FROM `bigquery-public-data.google_analytics_sample.ga_sessions_20161224` 
EXCEPT DISTINCT #soustraction
SELECT DISTINCT fullvisitorid, 
FROM `bigquery-public-data.google_analytics_sample.ga_sessions_20161225` 

##################################################### jointure #########################################################

WITH visits AS (
SELECT fullvisitorid, SUM(totals.visits) AS visits
FROM `bigquery-public-data.google_analytics_sample.ga_sessions_2016*`
GROUP BY fullvisitorid),
transactions AS (
SELECT fullvisitorid, COUNT(DISTINCT hits.transaction.transactionId) AS transactions
FROM `bigquery-public-data.google_analytics_sample.ga_sessions_2016*`AS ga, 
UNNEST(ga.hits) AS hits WHERE  hits.transaction.transactionId IS NOT NULL
GROUP BY fullvisitorid)
SELECT visits.fullvisitorid, visits.visits, transactions.transactions  FROM visits
INNER JOIN  transactions #jointure unique sur les 2 tables
ON visits.fullvisitorid = transactions.fullvisitorid

WITH visits AS (
SELECT fullvisitorid, SUM(totals.visits) AS visits
FROM `bigquery-public-data.google_analytics_sample.ga_sessions_2016*`
GROUP BY fullvisitorid),
transactions AS (
SELECT fullvisitorid, COUNT(DISTINCT hits.transaction.transactionId) AS transactions
FROM `bigquery-public-data.google_analytics_sample.ga_sessions_2016*`AS ga, 
UNNEST(ga.hits) AS hits WHERE  hits.transaction.transactionId IS NOT NULL
GROUP BY fullvisitorid)
SELECT visits.fullvisitorid, visits.visits, transactions.transactions  FROM visits
LEFT JOIN  transactions #LEFT OUTER JOIN #jointure la 1ère table
USING (fullvisitorid)

WITH visits AS (
SELECT fullvisitorid, SUM(totals.visits) AS visits
FROM `bigquery-public-data.google_analytics_sample.ga_sessions_2016*`
GROUP BY fullvisitorid),
transactions AS (
SELECT fullvisitorid, COUNT(DISTINCT hits.transaction.transactionId) AS transactions
FROM `bigquery-public-data.google_analytics_sample.ga_sessions_2016*`AS ga, 
UNNEST(ga.hits) AS hits WHERE  hits.transaction.transactionId IS NOT NULL
GROUP BY fullvisitorid)
SELECT visits.fullvisitorid, visits.visits, transactions.transactions  FROM visits
RIGHT JOIN  transactions #RIGHT OUTER JOIN #jointure sur la 2ème table
USING (fullvisitorid) 

WITH visits AS (
SELECT fullvisitorid, SUM(totals.visits) AS visits
FROM `bigquery-public-data.google_analytics_sample.ga_sessions_2016*`
GROUP BY fullvisitorid),
transactions AS (
SELECT fullvisitorid, COUNT(DISTINCT hits.transaction.transactionId) AS transactions
FROM `bigquery-public-data.google_analytics_sample.ga_sessions_2016*`AS ga, 
UNNEST(ga.hits) AS hits WHERE  hits.transaction.transactionId IS NOT NULL
GROUP BY fullvisitorid)
SELECT visits.fullvisitorid, visits.visits, transactions.transactions  FROM visits
FULL JOIN  transactions #FULL OUTER JOIN #jointure complete sur les 2 tables
USING (fullvisitorid)

WITH 
visitors AS (SELECT DISTINCT fullvisitorid FROM `bigquery-public-data.google_analytics_sample.ga_sessions_20161201`),
products AS (SELECT DISTINCT hp.v2ProductName AS product FROM `bigquery-public-data.google_analytics_sample.ga_sessions_*`
             AS ga, UNNEST(ga.hits) AS hits, UNNEST(hits.product) AS hp )

SELECT fullvisitorid, product FROM visitors CROSS JOIN products #FROM visitors, products #developpement factoriel

################################################# chaine de caractère ###################################################

SELECT DISTINCT CONCAT("ID",fullvisitorid) AS fullvisitorid, device.deviceCategory,
LENGTH(device.deviceCategory), #nombre de caractère
LEFT(device.deviceCategory,1), #x caratères depuis la gauche
RIGHT(device.deviceCategory,1), #x caratères depuis la droite
UPPER(device.deviceCategory), #majuscule
LOWER(UPPER(device.deviceCategory)), #minuscule
LPAD(device.deviceCategory,10,"0"), #ajoute x caratères depuis la gauche jusqu'à qu'il y en ai 10
RPAD(device.deviceCategory,10,"0"), #ajoute x caratères depuis la droite jusqu'à qu'il y en ai 10
LTRIM(LPAD(device.deviceCategory,10,"0"),"0"), #supprimer la chaine de caratères "..." depuis la gauche 
RTRIM(RPAD(device.deviceCategory,10,"0"),"0"), #supprimer la chaine de caratères "..." depuis la droite
TRIM(device.deviceCategory,"e"), #supprimer la chaine de caratères "..."
REPLACE(device.deviceCategory,"1","2"), #remplacer 1 par 2
REVERSE(device.deviceCategory), #inverser la chaine de caractère
SUBSTR(device.deviceCategory,1, 2), #2 caratères depuis la position 1
SUBSTR(device.deviceCategory,3), #tous les caracteres depuis la position 3
FROM `bigquery-public-data.google_analytics_sample.ga_sessions_20161201`

####################################################### date ############################################################

#DATE
SELECT 
CURRENT_DATE(), CURRENT_TIME(), #2021-09-02, 09:04:47.005603 
EXTRACT(DAYOFWEEK FROM CURRENT_DATE()), #[0,7] le dimanche étant considéré comme le premier jour de la semaine
EXTRACT(DAY FROM CURRENT_DATE()),
EXTRACT(DAYOFYEAR FROM CURRENT_DATE()),
EXTRACT(WEEK  FROM CURRENT_DATE()), 
#[0,53] commence le dimanche et les dates antérieures au premier dimanche de l'année correspondent à la semaine 0
EXTRACT(WEEK(MONDAY) FROM CURRENT_DATE()),
#[0,53] commence le jour spécifié par WEEKDAY (SUNDAY, MONDAY, TUESDAY, WEDNESDAY, THURSDAY, FRIDAY, SATURDAY)
#les dates antérieures au premier WEEKDAY de l'année correspondent à la semaine 0
EXTRACT(MONTH FROM CURRENT_DATE()),
EXTRACT(QUARTER FROM CURRENT_DATE()), #[1,4]
EXTRACT(YEAR FROM CURRENT_DATE()),
DATE_ADD(CURRENT_DATE(), INTERVAL 1 DAY),
DATE_SUB(CURRENT_DATE(), INTERVAL 1 DAY),
DATE_DIFF(DATE_ADD(CURRENT_DATE(), INTERVAL 1 DAY),DATE_SUB(CURRENT_DATE(), INTERVAL 1 DAY), DAY),
DATE_DIFF(DATE_ADD(CURRENT_DATE(), INTERVAL 1 DAY),DATE_SUB(CURRENT_DATE(), INTERVAL 1 DAY), WEEK),
DATE_DIFF(DATE_ADD(CURRENT_DATE(), INTERVAL 1 DAY),DATE_SUB(CURRENT_DATE(), INTERVAL 1 DAY), WEEK(MONDAY)),
DATE_DIFF(DATE_ADD(CURRENT_DATE(), INTERVAL 1 DAY),DATE_SUB(CURRENT_DATE(), INTERVAL 1 DAY), MONTH),
DATE_DIFF(DATE_ADD(CURRENT_DATE(), INTERVAL 1 DAY),DATE_SUB(CURRENT_DATE(), INTERVAL 1 DAY), QUARTER),
DATE_DIFF(DATE_ADD(CURRENT_DATE(), INTERVAL 1 DAY),DATE_SUB(CURRENT_DATE(), INTERVAL 1 DAY), YEAR),
DATE_FROM_UNIX_DATE(14238), #nombre de jours écoulés depuis le 1970-01-01
FORMAT_DATE("%x", CURRENT_DATE()), #string "mm/dd/yy"
#https://cloud.google.com/bigquery/docs/reference/standard-sql/date_functions?hl=fr#supported_format_elements_for_date
LAST_DAY(CURRENT_DATE(), YEAR) AS last_day,
LAST_DAY(CURRENT_DATE(), QUARTER) AS last_day,
LAST_DAY(CURRENT_DATE(), MONTH) AS last_day,
LAST_DAY(CURRENT_DATE(), WEEK) AS last_day, #egal à sept jours (DAY)
LAST_DAY(CURRENT_DATE(), WEEK(MONDAY)) AS last_day,
PARSE_DATE("%Y%m%d", date)
FROM `bigquery-public-data.google_analytics_sample.ga_sessions_20161201`limit 1

#DATETIME
SELECT 
CURRENT_DATETIME(),
DATETIME(EXTRACT(YEAR FROM PARSE_DATE("%Y%m%d", date)), EXTRACT(MONTH FROM PARSE_DATE("%Y%m%d", date)),
         EXTRACT(DAY FROM PARSE_DATE("%Y%m%d", date)), hits.hour, hits.minute,00),
DATE(CURRENT_DATETIME()),
EXTRACT(MICROSECOND FROM CURRENT_DATETIME()),
EXTRACT(MILLISECOND FROM CURRENT_DATETIME()),
EXTRACT(SECOND FROM CURRENT_DATETIME()),
EXTRACT(MINUTE FROM CURRENT_DATETIME()),
EXTRACT(HOUR FROM CURRENT_DATETIME()),
EXTRACT(DAYOFWEEK FROM CURRENT_DATETIME()),
EXTRACT(DAY FROM CURRENT_DATETIME()),
EXTRACT(DAYOFYEAR FROM CURRENT_DATETIME()),
EXTRACT(WEEK FROM CURRENT_DATETIME()),
EXTRACT(WEEK(MONDAY) FROM CURRENT_DATETIME()),
EXTRACT(ISOWEEK FROM CURRENT_DATETIME()),
EXTRACT(MONTH FROM CURRENT_DATETIME()),
EXTRACT(QUARTER FROM CURRENT_DATETIME()),
EXTRACT(YEAR FROM CURRENT_DATETIME()),
EXTRACT(ISOYEAR FROM CURRENT_DATETIME()),
EXTRACT(DATE FROM CURRENT_DATETIME()),
EXTRACT(TIME FROM CURRENT_DATETIME()),
DATETIME_ADD(CURRENT_DATETIME(), INTERVAL 10 MICROSECOND),
DATETIME_ADD(CURRENT_DATETIME(), INTERVAL 10 MILLISECOND),
DATETIME_ADD(CURRENT_DATETIME(), INTERVAL 10 SECOND),
DATETIME_ADD(CURRENT_DATETIME(), INTERVAL 10 MINUTE),
DATETIME_ADD(CURRENT_DATETIME(), INTERVAL 10 HOUR),
DATETIME_ADD(CURRENT_DATETIME(), INTERVAL 10 DAY),
DATETIME_ADD(CURRENT_DATETIME(), INTERVAL 10 WEEK),
DATETIME_ADD(CURRENT_DATETIME(), INTERVAL 10 QUARTER),
DATETIME_ADD(CURRENT_DATETIME(), INTERVAL 10 YEAR),
DATETIME_SUB(CURRENT_DATETIME(), INTERVAL 10 MICROSECOND),
DATETIME_SUB(CURRENT_DATETIME(), INTERVAL 10 MILLISECOND),
DATETIME_SUB(CURRENT_DATETIME(), INTERVAL 10 SECOND),
DATETIME_SUB(CURRENT_DATETIME(), INTERVAL 10 MINUTE),
DATETIME_SUB(CURRENT_DATETIME(), INTERVAL 10 HOUR),
DATETIME_SUB(CURRENT_DATETIME(), INTERVAL 10 DAY),
DATETIME_SUB(CURRENT_DATETIME(), INTERVAL 10 WEEK),
DATETIME_SUB(CURRENT_DATETIME(), INTERVAL 10 QUARTER),
DATETIME_SUB(CURRENT_DATETIME(), INTERVAL 10 YEAR),
DATETIME_DIFF(CURRENT_DATETIME(),DATETIME_SUB(CURRENT_DATETIME(), INTERVAL 10 MINUTE), MICROSECOND),
DATETIME_DIFF(CURRENT_DATETIME(),DATETIME_SUB(CURRENT_DATETIME(), INTERVAL 10 MINUTE), MILLISECOND),
DATETIME_DIFF(CURRENT_DATETIME(),DATETIME_SUB(CURRENT_DATETIME(), INTERVAL 10 MINUTE), SECOND),
DATETIME_DIFF(CURRENT_DATETIME(),DATETIME_SUB(CURRENT_DATETIME(), INTERVAL 10 MINUTE), MINUTE),
DATETIME_DIFF(CURRENT_DATETIME(),DATETIME_SUB(CURRENT_DATETIME(), INTERVAL 10 MINUTE), HOUR),
DATETIME_DIFF(CURRENT_DATETIME(),DATETIME_SUB(CURRENT_DATETIME(), INTERVAL 10 MINUTE), DAY),
DATETIME_DIFF(CURRENT_DATETIME(),DATETIME_SUB(CURRENT_DATETIME(), INTERVAL 10 MINUTE), WEEK),
DATETIME_DIFF(CURRENT_DATETIME(),DATETIME_SUB(CURRENT_DATETIME(), INTERVAL 10 MINUTE), WEEK(MONDAY)),
DATETIME_DIFF(CURRENT_DATETIME(),DATETIME_SUB(CURRENT_DATETIME(), INTERVAL 10 MINUTE), MONTH),
DATETIME_DIFF(CURRENT_DATETIME(),DATETIME_SUB(CURRENT_DATETIME(), INTERVAL 10 MINUTE), QUARTER),
DATETIME_DIFF(CURRENT_DATETIME(),DATETIME_SUB(CURRENT_DATETIME(), INTERVAL 10 MINUTE), YEAR),
FROM `bigquery-public-data.google_analytics_sample.ga_sessions_20161201` AS ga, 
UNNEST(ga.hits) AS hits limit 1

#TIMESTAMP
SELECT 
CURRENT_TIMESTAMP(),
EXTRACT(MICROSECOND FROM CURRENT_TIMESTAMP()),
EXTRACT(MILLISECOND FROM CURRENT_TIMESTAMP()),
EXTRACT(SECOND FROM CURRENT_TIMESTAMP()),
EXTRACT(MINUTE FROM CURRENT_TIMESTAMP()),
EXTRACT(HOUR FROM CURRENT_TIMESTAMP()),
EXTRACT(DAYOFWEEK FROM CURRENT_TIMESTAMP()),
EXTRACT(DAY FROM CURRENT_TIMESTAMP()),
EXTRACT(DAYOFYEAR FROM CURRENT_TIMESTAMP()),
EXTRACT(WEEK FROM CURRENT_TIMESTAMP()),
EXTRACT(WEEK(MONDAY) FROM CURRENT_TIMESTAMP()),
EXTRACT(ISOWEEK FROM CURRENT_TIMESTAMP()),
EXTRACT(MONTH FROM CURRENT_TIMESTAMP()),
EXTRACT(QUARTER FROM CURRENT_TIMESTAMP()),
EXTRACT(YEAR FROM CURRENT_TIMESTAMP()),
EXTRACT(ISOYEAR FROM CURRENT_TIMESTAMP()),
EXTRACT(DATE FROM CURRENT_TIMESTAMP()),
EXTRACT(DATETIME FROM CURRENT_TIMESTAMP()),
EXTRACT(TIME FROM CURRENT_TIMESTAMP()),
STRING(CURRENT_TIMESTAMP()),
TIMESTAMP_ADD(CURRENT_TIMESTAMP(), INTERVAL 10 MICROSECOND),
TIMESTAMP_ADD(CURRENT_TIMESTAMP(), INTERVAL 10 MILLISECOND),
TIMESTAMP_ADD(CURRENT_TIMESTAMP(), INTERVAL 10 SECOND),
TIMESTAMP_ADD(CURRENT_TIMESTAMP(), INTERVAL 10 MINUTE),
TIMESTAMP_ADD(CURRENT_TIMESTAMP(), INTERVAL 10 HOUR),
TIMESTAMP_ADD(CURRENT_TIMESTAMP(), INTERVAL 10 DAY),
TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL 10 MICROSECOND),
TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL 10 MILLISECOND),
TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL 10 SECOND),
TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL 10 MINUTE),
TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL 10 HOUR),
TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL 10 DAY),
TIMESTAMP_DIFF(CURRENT_TIMESTAMP(), TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL 10 MINUTE), MICROSECOND),
TIMESTAMP_DIFF(CURRENT_TIMESTAMP(), TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL 10 MINUTE), MILLISECOND),
TIMESTAMP_DIFF(CURRENT_TIMESTAMP(), TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL 10 MINUTE), SECOND),
TIMESTAMP_DIFF(CURRENT_TIMESTAMP(), TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL 10 MINUTE), MINUTE),
TIMESTAMP_DIFF(CURRENT_TIMESTAMP(), TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL 10 MINUTE), HOUR),
TIMESTAMP_DIFF(CURRENT_TIMESTAMP(), TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL 10 MINUTE), DAY),
TIMESTAMP_SECONDS(visitStartTime),
TIMESTAMP_MILLIS(1230219000000),
TIMESTAMP_MICROS(1230219000000000),
FROM `bigquery-public-data.google_analytics_sample.ga_sessions_20161201` AS ga, 
UNNEST(ga.hits) AS hits limit 1

###################################################### ARRAY ###########################################################

SELECT fullvisitorid,ARRAY_AGG(DISTINCT hp.v2ProductName) 
FROM `bigquery-public-data.google_analytics_sample.ga_sessions_20161201` AS ga, 
UNNEST(ga.hits) AS hits, UNNEST(hits.product) AS hp 
WHERE hits.transaction.transactionId IS NOT NULL GROUP BY fullvisitorid

###################################################### OVER ############################################################

WITH products AS (
SELECT fullvisitorid,COUNT(DISTINCT hp.v2ProductName) AS products 
FROM `bigquery-public-data.google_analytics_sample.ga_sessions_20161201` AS ga, 
UNNEST(ga.hits) AS hits, UNNEST(hits.product) AS hp GROUP BY fullvisitorid ),

classe AS (
SELECT fullvisitorid, products.products, NTILE(2) OVER (ORDER BY products.products DESC) AS classe
FROM products ORDER BY products DESC )

SELECT MAX(products) AS median_poduct FROM classe WHERE classe.classe = 2


WITH products AS (
SELECT  hp.v2ProductName, geoNetwork.continent, COUNT(DISTINCT fullvisitorid) AS visits
FROM `bigquery-public-data.google_analytics_sample.ga_sessions_20161201` AS ga, 
UNNEST(ga.hits) AS hits, UNNEST(hits.product) AS hp GROUP BY hp.v2ProductName, geoNetwork.continent)

SELECT v2ProductName,continent, visits, 
SUM(visits) OVER(PARTITION BY v2ProductName), 
SUM(visits) OVER(PARTITION BY v2ProductName ORDER BY visits ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW), 
SUM(visits) OVER(PARTITION BY v2ProductName ORDER BY visits ROWS BETWEEN CURRENT ROW AND UNBOUNDED FOLLOWING),
SUM(visits) OVER(PARTITION BY v2ProductName ORDER BY visits DESC ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW), 
SUM(visits) OVER(PARTITION BY v2ProductName ORDER BY visits DESC ROWS BETWEEN CURRENT ROW AND UNBOUNDED FOLLOWING), 
SUM(visits) OVER(PARTITION BY v2ProductName ORDER BY visits DESC ROWS BETWEEN 0 PRECEDING AND 2 FOLLOWING ),
SUM(visits) OVER(PARTITION BY v2ProductName ORDER BY visits DESC ROWS BETWEEN 1 PRECEDING AND 2 FOLLOWING ),
SUM(visits) OVER(PARTITION BY v2ProductName ORDER BY visits DESC ROWS BETWEEN 2 PRECEDING AND 2 FOLLOWING ),
ROW_NUMBER() OVER(PARTITION BY v2ProductName),
FROM products ORDER BY v2ProductName, visits DESC 

###################################### BigQuery SQL Code & Google Analytics #############################################

#Google Analytics 360 : https://support.google.com/analytics/answer/3437719?hl=fr

##################################################### SELECT ############################################################

SELECT 
     DISTINCT fullvisitorid, 
     device.deviceCategory, 
     10 AS dix,
FROM 
     `bigquery-public-data.google_analytics_sample.ga_sessions_20161201` 

##################################################### WHERE #############################################################

SELECT 
     DISTINCT fullvisitorid, 
     device.deviceCategory
FROM 
     `bigquery-public-data.google_analytics_sample.ga_sessions_20161201` 
WHERE device.deviceCategory = "desktop" #egale

SELECT 
     DISTINCT fullvisitorid, 
     device.deviceCategory
FROM 
     `bigquery-public-data.google_analytics_sample.ga_sessions_20161201` 
WHERE device.deviceCategory <> "desktop" #different

SELECT 
     DISTINCT fullvisitorid, 
     device.deviceCategory
FROM 
     `bigquery-public-data.google_analytics_sample.ga_sessions_20161201` 
WHERE device.deviceCategory != "desktop" #different

SELECT 
     DISTINCT fullvisitorid, 
     device.deviceCategory
FROM 
     `bigquery-public-data.google_analytics_sample.ga_sessions_20161201` 
WHERE device.deviceCategory IN ("mobile", "tablet") #parmis

SELECT 
     DISTINCT fullvisitorid, 
     device.deviceCategory
FROM 
     `bigquery-public-data.google_analytics_sample.ga_sessions_20161201` 
WHERE 
     (device.deviceCategory LIKE "le%" 
          OR device.deviceCategory LIKE "%le" 
          OR device.deviceCategory LIKE "%le%") 
      AND device.deviceCategory NOT LIKE "d%k" #commence par ... ou fini par ... ou contient ... et ne commence et fini par ...&...

SELECT 
     DISTINCT fullvisitorid, 
     date
FROM 
     `bigquery-public-data.google_analytics_sample.ga_sessions_*` 
WHERE _TABLE_SUFFIX > '20161201' 
ORDER BY 2 #superieur à

SELECT 
     DISTINCT fullvisitorid, 
     date
FROM 
     `bigquery-public-data.google_analytics_sample.ga_sessions_*` 
WHERE _TABLE_SUFFIX >= '20161201' 
ORDER BY 2 #superieur ou egal à

SELECT 
     DISTINCT fullvisitorid, 
     date
FROM 
     `bigquery-public-data.google_analytics_sample.ga_sessions_*` 
WHERE _TABLE_SUFFIX < '20161201' 
ORDER BY 2 DESC #inferieur à

SELECT 
     DISTINCT fullvisitorid, 
     date
FROM 
     `bigquery-public-data.google_analytics_sample.ga_sessions_*` 
WHERE _TABLE_SUFFIX <= '20161201' 
ORDER BY 2 
DESC #inferieur ou egal à

SELECT 
     DISTINCT fullvisitorid, 
     date
FROM `bigquery-public-data.google_analytics_sample.ga_sessions_*` 
WHERE _TABLE_SUFFIX BETWEEN '20161201' AND '20161231' 
ORDER BY 2 #entre

SELECT 
     DISTINCT fullvisitorid
FROM 
     `bigquery-public-data.google_analytics_sample.ga_sessions_20161201`AS ga, 
     UNNEST(ga.hits) AS hits 
WHERE hits.transaction.transactionId IS NULL #est vide

SELECT 
     DISTINCT fullvisitorid
FROM 
     `bigquery-public-data.google_analytics_sample.ga_sessions_20161201`AS ga, 
     UNNEST(ga.hits) AS hits 
WHERE hits.transaction.transactionId IS NOT NULL #est non vide

###################################################### GROUP BY ########################################################

SELECT 
     fullvisitorid, 
     SUM(totals.visits) #somme
     ROUND(AVG(totals.visits),2) #moyenne arrondie
     COUNT(DISTINCT device.deviceCategory) #compte
     MIN(date), #minimum
     MAX(date), #maximum
FROM 
     `bigquery-public-data.google_analytics_sample.ga_sessions_201612*` 
GROUP BY 1

###################################################### HAVING ##########################################################

SELECT 
     fullvisitorid, 
     SUM(totals.visits) AS visits
FROM 
     `bigquery-public-data.google_analytics_sample.ga_sessions_201612*` 
GROUP BY 1
HAVING SUM(totals.visits) >= 2
ORDER BY 2 DESC

###################################################### CASE WHEN ##########################################################

WITH transactions AS (
    SELECT 
        hp.v2ProductName AS products,
        device.deviceCategory
    FROM 
        `bigquery-public-data.google_analytics_sample.ga_sessions_*` AS ga, 
        UNNEST(ga.hits) AS hits, 
        UNNEST(hits.product) AS hp 
    WHERE 
          hits.transaction.transactionId IS NOT NULL 
)

SELECT
    products,
    CASE 
        WHEN
            SUM(CASE WHEN deviceCategory = "mobile" THEN 1 ELSE 0 END) > 0 
            AND SUM(CASE WHEN deviceCategory = "tablet" THEN 1 ELSE 0 END) = 0  
            AND SUM(CASE WHEN deviceCategory = "desktop" THEN 1 ELSE 0 END) = 0  THEN "mobile" 
       WHEN
            SUM(CASE WHEN deviceCategory = "mobile" THEN 1 ELSE 0 END) = 0 
            AND SUM(CASE WHEN deviceCategory = "tablet" THEN 1 ELSE 0 END) > 0  
            AND SUM(CASE WHEN deviceCategory = "desktop" THEN 1 ELSE 0 END) = 0  THEN "tablet" 
        WHEN
            SUM(CASE WHEN deviceCategory = "mobile" THEN 1 ELSE 0 END) = 0 
            AND SUM(CASE WHEN deviceCategory = "tablet" THEN 1 ELSE 0 END) = 0  
            AND SUM(CASE WHEN deviceCategory = "desktop" THEN 1 ELSE 0 END) > 0  THEN "desktop" 
        WHEN
            SUM(CASE WHEN deviceCategory = "mobile" THEN 1 ELSE 0 END) > 0 
            AND SUM(CASE WHEN deviceCategory = "tablet" THEN 1 ELSE 0 END) > 0  
            AND SUM(CASE WHEN deviceCategory = "desktop" THEN 1 ELSE 0 END) = 0  THEN "mobile & tablet" 
        WHEN
            SUM(CASE WHEN deviceCategory = "mobile" THEN 1 ELSE 0 END) > 0 
            AND SUM(CASE WHEN deviceCategory = "tablet" THEN 1 ELSE 0 END) = 0  
            AND SUM(CASE WHEN deviceCategory = "desktop" THEN 1 ELSE 0 END) > 0  THEN "mobile & desktop" 
        WHEN
            SUM(CASE WHEN deviceCategory = "mobile" THEN 1 ELSE 0 END) = 0 
            AND SUM(CASE WHEN deviceCategory = "tablet" THEN 1 ELSE 0 END) > 0  
            AND SUM(CASE WHEN deviceCategory = "desktop" THEN 1 ELSE 0 END) > 0  THEN "tablet & desktop" 
        WHEN
            SUM(CASE WHEN deviceCategory = "mobile" THEN 1 ELSE 0 END) > 0 
            AND SUM(CASE WHEN deviceCategory = "tablet" THEN 1 ELSE 0 END) > 0  
            AND SUM(CASE WHEN deviceCategory = "desktop" THEN 1 ELSE 0 END) > 0  THEN "mobile & tablet & desktop" END AS device
FROM 
    transactions
GROUP BY 1

################################################### ARRAY & UNNEST #####################################################

WITH transactions AS (
    SELECT 
        DISTINCT fullvisitorid,
        hp.v2ProductName AS products
    FROM 
        `bigquery-public-data.google_analytics_sample.ga_sessions_20161201` AS ga, 
        UNNEST(ga.hits) AS hits, 
        UNNEST(hits.product) AS hp 
    WHERE 
          hits.transaction.transactionId IS NOT NULL 
          AND hp.v2ProductName like "%/%"
)

, array_table AS (
     SELECT 
          fullvisitorid,
          SPLIT(products,'/') AS products
     FROM
          transactions
)
    
SELECT 
    fullvisitorid,
    products_sold,
FROM
    array_table
CROSS JOIN UNNEST(array_table.products) AS products_sold

###################################################### OVER #############################################################

WITH products AS (
     SELECT
          fullvisitorid,
          COUNT(DISTINCT hp.v2ProductName) AS products 
     FROM 
          `bigquery-public-data.google_analytics_sample.ga_sessions_20161201` AS ga, 
          UNNEST(ga.hits) AS hits, 
          UNNEST(hits.product) AS hp 
     GROUP BY 1 )
     
, classe AS (
     SELECT 
          fullvisitorid, 
          products.products, 
          NTILE(2) OVER (ORDER BY products.products DESC) AS classe
     FROM 
          products 
     ORDER BY 2 DESC)

SELECT 
     MAX(products) AS median_poduct 
FROM 
     classe 
WHERE classe.classe = 2

WITH products AS (
     SELECT  
          hp.v2ProductName, 
          geoNetwork.continent, 
          COUNT(DISTINCT fullvisitorid) AS visits
     FROM 
          `bigquery-public-data.google_analytics_sample.ga_sessions_20161201` AS ga, 
          UNNEST(ga.hits) AS hits, 
          UNNEST(hits.product) AS hp 
     GROUP BY 1, 2)

SELECT 
     v2ProductName,
     continent, 
     visits, 
     SUM(visits) OVER(PARTITION BY v2ProductName), 
     SUM(visits) OVER(PARTITION BY v2ProductName ORDER BY visits DESC ROWS BETWEEN CURRENT ROW AND UNBOUNDED FOLLOWING),
     SUM(visits) OVER(PARTITION BY v2ProductName ORDER BY visits DESC ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW), 
     SUM(visits) OVER(PARTITION BY v2ProductName ORDER BY visits DESC ROWS BETWEEN CURRENT ROW AND 2 FOLLOWING), 
     SUM(visits) OVER(PARTITION BY v2ProductName ORDER BY visits DESC ROWS BETWEEN 2 PRECEDING AND CURRENT ROW),
     ROW_NUMBER() OVER(PARTITION BY v2ProductName),
     RANK() OVER(PARTITION BY v2ProductName ORDER BY visits DESC)
FROM 
     products 
ORDER BY 1, 3 DESC 

##################################################### UNION ############################################################

#fusion
SELECT 
     DISTINCT fullvisitorid, 
FROM 
     `bigquery-public-data.google_analytics_sample.ga_sessions_20161224`
 
UNION ALL 

SELECT 
     DISTINCT fullvisitorid, 
FROM 
     `bigquery-public-data.google_analytics_sample.ga_sessions_20161225`

#intersection
SELECT 
     DISTINCT fullvisitorid, 
FROM 
     `bigquery-public-data.google_analytics_sample.ga_sessions_20161224` 

INTERSECT DISTINCT 

SELECT 
     DISTINCT fullvisitorid, 
FROM 
     `bigquery-public-data.google_analytics_sample.ga_sessions_20161225`

#extraction
SELECT 
     DISTINCT fullvisitorid, 
FROM 
     `bigquery-public-data.google_analytics_sample.ga_sessions_20161224` 

EXCEPT DISTINCT 

SELECT 
     DISTINCT fullvisitorid, 
FROM 
     `bigquery-public-data.google_analytics_sample.ga_sessions_20161225` 

###################################################### JOIN #############################################################

WITH visits AS (
     SELECT 
          fullvisitorid, 
          SUM(totals.visits) AS visits
     FROM 
          `bigquery-public-data.google_analytics_sample.ga_sessions_2016*`
     GROUP BY 1
)

, transactions AS (
     SELECT 
          fullvisitorid, 
          COUNT(DISTINCT hits.transaction.transactionId) AS transactions
     FROM 
          `bigquery-public-data.google_analytics_sample.ga_sessions_2016*`AS ga, 
          UNNEST(ga.hits) AS hits 
     WHERE  hits.transaction.transactionId IS NOT NULL
     GROUP BY 1
)
     
SELECT 
     visits.fullvisitorid, 
     visits.visits, transactions.transactions  
FROM 
     visits
INNER JOIN  
     transactions #jointure unique sur les 2 tables
     ON visits.fullvisitorid = transactions.fullvisitorid

WITH visits AS (
     SELECT 
          fullvisitorid, 
          SUM(totals.visits) AS visits
     FROM 
          `bigquery-public-data.google_analytics_sample.ga_sessions_2016*`
     GROUP BY 1
)
     
, transactions AS (
     SELECT 
          fullvisitorid, 
          COUNT(DISTINCT hits.transaction.transactionId) AS transactions
     FROM 
          `bigquery-public-data.google_analytics_sample.ga_sessions_2016*`AS ga, 
     UNNEST(ga.hits) AS hits 
     WHERE  hits.transaction.transactionId IS NOT NULL
     GROUP BY 1
)

SELECT 
     visits.fullvisitorid, 
     visits.visits, transactions.transactions  
FROM 
     visits
LEFT JOIN  
     transactions #LEFT OUTER JOIN #jointure la 1ère table
     USING (fullvisitorid)

WITH visits AS (
     SELECT 
          fullvisitorid, 
          SUM(totals.visits) AS visits
     FROM 
          `bigquery-public-data.google_analytics_sample.ga_sessions_2016*`
     GROUP BY 1
)
     
, transactions AS (
     SELECT 
          fullvisitorid, 
          COUNT(DISTINCT hits.transaction.transactionId) AS transactions
     FROM 
          `bigquery-public-data.google_analytics_sample.ga_sessions_2016*`AS ga, 
     UNNEST(ga.hits) AS hits 
     WHERE hits.transaction.transactionId IS NOT NULL
     GROUP BY 1
)
     
SELECT 
     visits.fullvisitorid, 
     visits.visits, transactions.transactions  
FROM 
     visits
RIGHT JOIN  
     transactions #RIGHT OUTER JOIN #jointure sur la 2ème table
USING (fullvisitorid) 

WITH visits AS (
     SELECT 
          fullvisitorid, 
          SUM(totals.visits) AS visits
     FROM 
          `bigquery-public-data.google_analytics_sample.ga_sessions_2016*`
     GROUP BY 1
)
     
, transactions AS (
     SELECT 
          fullvisitorid, 
          COUNT(DISTINCT hits.transaction.transactionId) AS transactions
     FROM 
          `bigquery-public-data.google_analytics_sample.ga_sessions_2016*`AS ga, 
     UNNEST(ga.hits) AS hits WHERE  hits.transaction.transactionId IS NOT NULL
     GROUP BY 1
)

SELECT 
     visits.fullvisitorid, 
     visits.visits, 
     transactions.transactions  
FROM
     visits
FULL JOIN 
     transactions #FULL OUTER JOIN #jointure complete sur les 2 tables
     USING (fullvisitorid)

WITH visitors AS (
     SELECT 
          DISTINCT fullvisitorid 
     FROM 
          `bigquery-public-data.google_analytics_sample.ga_sessions_20161201`
)
          
, products AS (
     SELECT 
          DISTINCT hp.v2ProductName AS product 
     FROM 
          `bigquery-public-data.google_analytics_sample.ga_sessions_*` AS ga, 
     UNNEST(ga.hits) AS hits, 
     UNNEST(hits.product) AS hp 
)

SELECT 
     fullvisitorid, 
     product 
FROM 
     visitors 
CROSS JOIN products #FROM visitors, products #developpement factoriel

###################################################### STRING ##########################################################

SELECT 
     DISTINCT CONCAT("ID",fullvisitorid) AS fullvisitorid, 
     device.deviceCategory,
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
FROM 
     `bigquery-public-data.google_analytics_sample.ga_sessions_20161201`

#################################################### DATA & TIME ######################################################

#DATE
SELECT 
     CURRENT_DATE(), #2021-09-02,
     EXTRACT(DAYOFWEEK FROM CURRENT_DATE()), #[0,7] le dimanche étant considéré comme le premier jour de la semaine
     CASE 
          WHEN EXTRACT(DAYOFWEEK FROM CURRENT_DATE()) = 2 THEN "1.Lundi"
          WHEN EXTRACT(DAYOFWEEK FROM CURRENT_DATE()) = 3 THEN "2.Mardi"
          WHEN EXTRACT(DAYOFWEEK FROM CURRENT_DATE()) = 4 THEN "3.Mercredi"
          WHEN EXTRACT(DAYOFWEEK FROM CURRENT_DATE()) = 5 THEN "4.Jeudi"
          WHEN EXTRACT(DAYOFWEEK FROM CURRENT_DATE()) = 6 THEN "5.Vendredi"
          WHEN EXTRACT(DAYOFWEEK FROM CURRENT_DATE()) = 7 THEN "6.Samedi" 
          WHEN EXTRACT(DAYOFWEEK FROM CURRENT_DATE()) = 1 THEN "7.Dimanche" END AS day_of_week,
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
     DATE_ADD(CURRENT_DATE(), INTERVAL 1 WEEK), # 1 WEEK = 7 DAY
     DATE_ADD(CURRENT_DATE(), INTERVAL 1 MONTH), 
     DATE_ADD(CURRENT_DATE(), INTERVAL 1 QUARTER), 
     DATE_ADD(CURRENT_DATE(), INTERVAL 1 YEAR),
     DATE_SUB(CURRENT_DATE(), INTERVAL 1 DAY),
     DATE_SUB(CURRENT_DATE(), INTERVAL 1 WEEK), #1 WEEK = 7 DAY
     DATE_SUB(CURRENT_DATE(), INTERVAL 1 MONTH),
     DATE_SUB(CURRENT_DATE(), INTERVAL 1 QUARTER),
     DATE_SUB(CURRENT_DATE(), INTERVAL 1 YEAR),
     DATE_DIFF(DATE_ADD(CURRENT_DATE(), INTERVAL 1 DAY),DATE_SUB(CURRENT_DATE(), INTERVAL 1 DAY), DAY),
     DATE_DIFF(DATE_ADD(CURRENT_DATE(), INTERVAL 1 DAY),DATE_SUB(CURRENT_DATE(), INTERVAL 1 DAY), WEEK),
     DATE_DIFF(DATE_ADD(CURRENT_DATE(), INTERVAL 1 DAY),DATE_SUB(CURRENT_DATE(), INTERVAL 1 DAY), WEEK(MONDAY)),
     DATE_DIFF(DATE_ADD(CURRENT_DATE(), INTERVAL 1 DAY),DATE_SUB(CURRENT_DATE(), INTERVAL 1 DAY), MONTH),
     DATE_DIFF(DATE_ADD(CURRENT_DATE(), INTERVAL 1 DAY),DATE_SUB(CURRENT_DATE(), INTERVAL 1 DAY), QUARTER),
     DATE_DIFF(DATE_ADD(CURRENT_DATE(), INTERVAL 1 DAY),DATE_SUB(CURRENT_DATE(), INTERVAL 1 DAY), YEAR),
     DATE_FROM_UNIX_DATE(CAST(visitStartTime/36400 AS INT)), #nombre de jours écoulés depuis le 1970-01-01 
     FORMAT_DATE("%x", CURRENT_DATE()), #string "mm/dd/yy" 
     LAST_DAY(CURRENT_DATE(), YEAR) AS last_day,
     LAST_DAY(CURRENT_DATE(), QUARTER) AS last_day,
     LAST_DAY(CURRENT_DATE(), MONTH) AS last_day,
     LAST_DAY(CURRENT_DATE(), WEEK) AS last_day, #1 WEEK = 7 DAY
     LAST_DAY(CURRENT_DATE(), WEEK(MONDAY)) AS last_day,
     PARSE_DATE("%Y%m%d", date) #convertit STRING en DATE
     #https://cloud.google.com/bigquery/docs/reference/standard-sql/date_functions?hl=fr#supported_format_elements_for_date

#DATE_TRUNC
SELECT 
    DATE_TRUNC(DATE(CURRENT_TIMESTAMP()), WEEK(MONDAY)),
    DATE_TRUNC(DATE(CURRENT_TIMESTAMP()), MONTH),
    DATE_TRUNC(DATE(CURRENT_TIMESTAMP()), QUARTER),
    DATE_TRUNC(DATE(CURRENT_TIMESTAMP()), YEAR)

#TIME
SELECT 
     CURRENT_TIME(), #21:53:41.384727 
     TIME(hits.hour, hits.minute, 00),
     EXTRACT(MICROSECOND FROM CURRENT_TIME()),
     EXTRACT(MILLISECOND FROM CURRENT_TIME()),
     EXTRACT(SECOND FROM CURRENT_TIME()),
     EXTRACT(MINUTE FROM CURRENT_TIME()),
     EXTRACT(HOUR FROM CURRENT_TIME()),
     TIME_ADD(CURRENT_TIME(), INTERVAL 10 MICROSECOND),
     TIME_ADD(CURRENT_TIME(), INTERVAL 10 MILLISECOND),
     TIME_ADD(CURRENT_TIME(), INTERVAL 10 SECOND),
     TIME_ADD(CURRENT_TIME(), INTERVAL 10 MINUTE),
     TIME_ADD(CURRENT_TIME(), INTERVAL 10 HOUR),
     TIME_SUB(CURRENT_TIME(), INTERVAL 10 MICROSECOND),
     TIME_SUB(CURRENT_TIME(), INTERVAL 10 MILLISECOND),
     TIME_SUB(CURRENT_TIME(), INTERVAL 10 SECOND),
     TIME_SUB(CURRENT_TIME(), INTERVAL 10 MINUTE),
     TIME_SUB(CURRENT_TIME(), INTERVAL 10 HOUR),
     TIME_DIFF(CURRENT_TIME(), TIME_SUB(CURRENT_TIME(), INTERVAL 10 MINUTE), MICROSECOND),
     TIME_DIFF(CURRENT_TIME(), TIME_SUB(CURRENT_TIME(), INTERVAL 10 MINUTE), MILLISECOND),
     TIME_DIFF(CURRENT_TIME(), TIME_SUB(CURRENT_TIME(), INTERVAL 10 MINUTE), SECOND),
     TIME_DIFF(CURRENT_TIME(), TIME_SUB(CURRENT_TIME(), INTERVAL 10 MINUTE), MINUTE),
     TIME_DIFF(CURRENT_TIME(), TIME_SUB(CURRENT_TIME(), INTERVAL 10 MINUTE), HOUR),
     TIME_TRUNC(CURRENT_TIME(), MICROSECOND),
     TIME_TRUNC(CURRENT_TIME(), MILLISECOND),
     TIME_TRUNC(CURRENT_TIME(), SECOND),
     TIME_TRUNC(CURRENT_TIME(), MINUTE),
     TIME_TRUNC(CURRENT_TIME(), HOUR),
     FORMAT_TIME("%R", CURRENT_TIME())
     PARSE_TIME("%H", CONCAT(hits.hour,":",hits.minute,":","00"))

#DATETIME
SELECT 
     CURRENT_DATETIME(), #2021-09-02T21:53:41.384727 
     DATETIME(EXTRACT(YEAR FROM PARSE_DATE("%Y%m%d", date)), 
               EXTRACT(MONTH FROM PARSE_DATE("%Y%m%d", date)),
               EXTRACT(DAY FROM PARSE_DATE("%Y%m%d", date)), 
               hits.hour, hits.minute, 00),
     DATE(CURRENT_DATETIME()), #2021-09-02
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
     EXTRACT(MONTH FROM CURRENT_DATETIME()),
     EXTRACT(QUARTER FROM CURRENT_DATETIME()),
     EXTRACT(YEAR FROM CURRENT_DATETIME()),
     EXTRACT(DATE FROM CURRENT_DATETIME()),
     EXTRACT(TIME FROM CURRENT_DATETIME()),
     DATETIME_ADD(CURRENT_DATETIME(), INTERVAL 10 MICROSECOND),
     DATETIME_ADD(CURRENT_DATETIME(), INTERVAL 10 MILLISECOND),
     DATETIME_ADD(CURRENT_DATETIME(), INTERVAL 10 SECOND),
     DATETIME_ADD(CURRENT_DATETIME(), INTERVAL 10 MINUTE),
     DATETIME_ADD(CURRENT_DATETIME(), INTERVAL 10 HOUR),
     DATETIME_ADD(CURRENT_DATETIME(), INTERVAL 10 DAY),
     DATETIME_ADD(CURRENT_DATETIME(), INTERVAL 10 WEEK), #1 WEEK = 7 DAY
     DATETIME_ADD(CURRENT_DATETIME(), INTERVAL 10 QUARTER),
     DATETIME_ADD(CURRENT_DATETIME(), INTERVAL 10 YEAR),
     DATETIME_SUB(CURRENT_DATETIME(), INTERVAL 10 MICROSECOND),
     DATETIME_SUB(CURRENT_DATETIME(), INTERVAL 10 MILLISECOND),
     DATETIME_SUB(CURRENT_DATETIME(), INTERVAL 10 SECOND),
     DATETIME_SUB(CURRENT_DATETIME(), INTERVAL 10 MINUTE),
     DATETIME_SUB(CURRENT_DATETIME(), INTERVAL 10 HOUR),
     DATETIME_SUB(CURRENT_DATETIME(), INTERVAL 10 DAY),
     DATETIME_SUB(CURRENT_DATETIME(), INTERVAL 10 WEEK), #1 WEEK = 7 DAY
     DATETIME_SUB(CURRENT_DATETIME(), INTERVAL 10 QUARTER),
     DATETIME_SUB(CURRENT_DATETIME(), INTERVAL 10 YEAR),
     DATETIME_DIFF(CURRENT_DATETIME(), DATETIME_SUB(CURRENT_DATETIME(), INTERVAL 10 MINUTE), MICROSECOND),
     DATETIME_DIFF(CURRENT_DATETIME(), DATETIME_SUB(CURRENT_DATETIME(), INTERVAL 10 MINUTE), MILLISECOND),
     DATETIME_DIFF(CURRENT_DATETIME(), DATETIME_SUB(CURRENT_DATETIME(), INTERVAL 10 MINUTE), SECOND),
     DATETIME_DIFF(CURRENT_DATETIME(), DATETIME_SUB(CURRENT_DATETIME(), INTERVAL 10 MINUTE), MINUTE),
     DATETIME_DIFF(CURRENT_DATETIME(), DATETIME_SUB(CURRENT_DATETIME(), INTERVAL 10 MINUTE), HOUR),
     DATETIME_DIFF(CURRENT_DATETIME(), DATETIME_SUB(CURRENT_DATETIME(), INTERVAL 10 MINUTE), DAY),
     DATETIME_DIFF(CURRENT_DATETIME(), DATETIME_SUB(CURRENT_DATETIME(), INTERVAL 10 MINUTE), WEEK),
     DATETIME_DIFF(CURRENT_DATETIME(), DATETIME_SUB(CURRENT_DATETIME(), INTERVAL 10 MINUTE), WEEK(MONDAY)),
     DATETIME_DIFF(CURRENT_DATETIME(), DATETIME_SUB(CURRENT_DATETIME(), INTERVAL 10 MINUTE), MONTH),
     DATETIME_DIFF(CURRENT_DATETIME(), DATETIME_SUB(CURRENT_DATETIME(), INTERVAL 10 MINUTE), QUARTER),
     DATETIME_DIFF(CURRENT_DATETIME(), DATETIME_SUB(CURRENT_DATETIME(), INTERVAL 10 MINUTE), YEAR),

#TIMESTAMP
SELECT 
     CURRENT_TIMESTAMP(), #2021-09-02 21:58:20.832894 UTC
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
     EXTRACT(MONTH FROM CURRENT_TIMESTAMP()),
     EXTRACT(QUARTER FROM CURRENT_TIMESTAMP()),
     EXTRACT(YEAR FROM CURRENT_TIMESTAMP()),
     EXTRACT(DATE FROM CURRENT_TIMESTAMP()),
     EXTRACT(DATETIME FROM CURRENT_TIMESTAMP()),
     EXTRACT(TIME FROM CURRENT_TIMESTAMP()),
     STRING(CURRENT_TIMESTAMP()),
     TIMESTAMP_ADD(CURRENT_TIMESTAMP(), INTERVAL 10 MICROSECOND),
     TIMESTAMP_ADD(CURRENT_TIMESTAMP(), INTERVAL 10 MILLISECOND),
     TIMESTAMP_ADD(CURRENT_TIMESTAMP(), INTERVAL 10 SECOND),
     TIMESTAMP_ADD(CURRENT_TIMESTAMP(), INTERVAL 10 MINUTE),
     TIMESTAMP_ADD(CURRENT_TIMESTAMP(), INTERVAL 10 HOUR), #1 HOUR = 60 MINUTE
     TIMESTAMP_ADD(CURRENT_TIMESTAMP(), INTERVAL 10 DAY), #1 DAY = 24 HOUR
     TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL 10 MICROSECOND),
     TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL 10 MILLISECOND),
     TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL 10 SECOND),
     TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL 10 MINUTE),
     TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL 10 HOUR), #1 HOUR = 60 MINUTE
     TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL 10 DAY), #1 DAY = 24 HOUR
     TIMESTAMP_DIFF(CURRENT_TIMESTAMP(), TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL 10 MINUTE), MICROSECOND),
     TIMESTAMP_DIFF(CURRENT_TIMESTAMP(), TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL 10 MINUTE), MILLISECOND),
     TIMESTAMP_DIFF(CURRENT_TIMESTAMP(), TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL 10 MINUTE), SECOND),
     TIMESTAMP_DIFF(CURRENT_TIMESTAMP(), TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL 10 MINUTE), MINUTE),
     TIMESTAMP_DIFF(CURRENT_TIMESTAMP(), TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL 10 MINUTE), HOUR), #1 HOUR = 60 MINUTE
     TIMESTAMP_DIFF(CURRENT_TIMESTAMP(), TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL 10 MINUTE), DAY), #1 DAY = 24 HOUR
     TIMESTAMP_SECONDS(visitStartTime),
     TIMESTAMP_MILLIS(1230219000000),
     TIMESTAMP_MICROS(1230219000000000),
 
 
SELECT 
    DATE_ADD(CURRENT_DATE(), INTERVAL delta DAY) AS un_ans
FROM 
  UNNEST(GENERATE_ARRAY(0,365)) AS delta

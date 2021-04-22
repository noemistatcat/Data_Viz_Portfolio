 DROP TABLE IF EXISTS `practice-datasets-310713.online_retail_datasets.online_retail_cohort_analysis`;
 CREATE TABLE `practice-datasets-310713.online_retail_datasets.online_retail_cohort_analysis`
 AS 
    WITH t_first_purchase AS (
    SELECT DISTINCT
    FORMAT_DATETIME('%Y%m', first_purchase_date) AS first_purchase,
    DATE_DIFF(date, first_purchase_date, MONTH) AS month_order,
    SUM(Sales) AS Sales,
    COUNT(DISTINCT CustomerID) AS Customers
    FROM (
      SELECT 
      DATE(TIMESTAMP(InvoiceDate)) AS date,
      UnitPrice * Quantity AS Sales,
      CustomerID,
      FIRST_VALUE(DATE(TIMESTAMP(InvoiceDate))) OVER (PARTITION BY CustomerID ORDER BY DATE(TIMESTAMP(InvoiceDate))) AS first_purchase_date
      FROM `practice-datasets-310713.online_retail_datasets.online_retail`
      WHERE Quantity IS NOT NULL DROP TABLE IF EXISTS `practice-datasets-310713.online_retail_datasets.online_retail_cohort_analysis`;
 CREATE TABLE `practice-datasets-310713.online_retail_datasets.online_retail_cohort_analysis`
 AS 
    WITH t_first_purchase AS (
    SELECT DISTINCT
    date,
    DATE_DIFF(date, first_purchase_date, MONTH) AS month_order,
    FORMAT_DATETIME('%Y%m', first_purchase_date) AS first_purchase,
    Sales,
    CustomerID
    FROM (
      SELECT 
      DATE(TIMESTAMP(InvoiceDate)) AS date,
      UnitPrice * Quantity AS Sales,
      CustomerID,
      FIRST_VALUE(DATE(TIMESTAMP(InvoiceDate))) OVER (PARTITION BY CustomerID ORDER BY DATE(TIMESTAMP(InvoiceDate))) AS first_purchase_date
      FROM `practice-datasets-310713.online_retail_datasets.online_retail`
      WHERE Quantity IS NOT NULL 
      AND UnitPrice IS NOT NULL
      AND CustomerID IS NOT NULL 
      AND DATE(TIMESTAMP(InvoiceDate)) BETWEEN '2011-01-01' AND '2011-11-30'
    )
   ),

t_agg AS (
    SELECT 
    first_purchase,
    month_order,
    SUM(Sales) AS Sales,
    COUNT(DISTINCT CustomerID) AS Customers
    FROM 
    t_first_purchase
    GROUP BY first_purchase, month_order
),

 t_cohort AS (
     SELECT *,
     SAFE_DIVIDE(Sales, CohortSales) AS CohortSalesPerc,
     SAFE_DIVIDE(Customers, CohortCustomers) AS CohortCustomersPerc  
     FROM (
         SELECT *,
         FIRST_VALUE(Sales) OVER (PARTITION BY first_purchase ORDER BY month_order) AS CohortSales,
         FIRST_VALUE(Customers) OVER (PARTITION BY first_purchase ORDER BY month_order) AS CohortCustomers
         FROM t_agg
     )
 )

SELECT * FROM t_cohort 
ORDER BY first_purchase, month_order
      AND UnitPrice IS NOT NULL
      AND CustomerID IS NOT NULL 
    )
    GROUP BY first_purchase, month_order
   ),

 t_cohort AS (
     SELECT *,
     SAFE_DIVIDE(Sales, CohortSales) AS CohortSalesPerc,
     SAFE_DIVIDE(Customers, CohortCustomers) AS CohortCustomersPerc  
     FROM (
         SELECT *,
         FIRST_VALUE(Sales) OVER (PARTITION BY first_purchase ORDER BY month_order) AS CohortSales,
         FIRST_VALUE(Customers) OVER (PARTITION BY first_purchase ORDER BY month_order) AS CohortCustomers
         FROM t_first_purchase
     )
 )

SELECT * FROM t_cohort 
ORDER BY first_purchase, month_order
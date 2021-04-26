 DROP TABLE IF EXISTS `practice-datasets-310713.online_retail_datasets.online_retail_preprocessed`;
 CREATE TABLE `practice-datasets-310713.online_retail_datasets.online_retail_preprocessed`
 PARTITION BY date
 AS 
   WITH t_repeat AS (
   SELECT *,
   CASE WHEN customer_seq > 1 THEN 'Repeat Customer'
   ELSE 'New Customer'
   END AS RepeatPurchase 
   FROM ( 
      SELECT  *,
      DATE(TIMESTAMP(InvoiceDate)) AS date,
      UnitPrice * Quantity AS Sales,
      RANK() OVER (PARTITION BY CustomerID ORDER BY DATE(TIMESTAMP(InvoiceDate))) AS customer_seq
      FROM `practice-datasets-310713.online_retail_datasets.online_retail`
      WHERE Quantity IS NOT NULL 
      AND UnitPrice IS NOT NULL
      AND CustomerID IS NOT NULL
   )
 ),

 t_repurchase AS (
    SELECT *, 
    DATE_DIFF(date, first_purchase, MONTH) AS month_order
    FROM (
        SELECT *,
        FIRST_VALUE(date) OVER (PARTITION BY CustomerID ORDER BY DATE) AS first_purchase
        FROM t_repeat
    )
 ),

  t_previous_purchase AS (
    SELECT *, 
    DATE_DIFF(next_purchase, date, DAY) AS days_bet_purchase
    FROM (
        SELECT *,
        LEAD(date) OVER (PARTITION BY CustomerID ORDER BY DATE) AS next_purchase
        FROM t_repurchase
    )
   )


SELECT * FROM t_previous_purchase

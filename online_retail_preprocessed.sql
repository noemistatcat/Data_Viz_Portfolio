 DROP TABLE IF EXISTS `practice-datasets-310713.online_retail_datasets.online_retail_preprocessed`;
 CREATE TABLE `practice-datasets-310713.online_retail_datasets.online_retail_preprocessed`
 PARTITION BY date
 AS 
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
 )
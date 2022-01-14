DROP TABLE IF EXISTS `practice-datasets-310713.online_retail_datasets.online_retail_full_rfm`;
CREATE TABLE `practice-datasets-310713.online_retail_datasets.online_retail_full_rfm`
PARTITION BY InvoiceDate
AS 
SELECT  a.*,
rfm_segment
FROM `practice-datasets-310713.online_retail_datasets.online_retail_preprocessed_nooutliers` a

LEFT JOIN
(SELECT CustomerID, rfm_segment
FROM `practice-datasets-310713.online_retail_datasets.online_retail_rfm`) b
ON a.CustomerID = b.CustomerID 
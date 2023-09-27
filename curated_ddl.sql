CREATE EXTERNAL TABLE `list_books_curated`(
  `url` string, 
  `rating2` double, 
  `isbn10` string, 
  `description` string, 
  `reviews_count` bigint, 
  `title` string, 
  `author` string, 
  `final_price` double)
PARTITIONED BY ( 
  `published_date` string)
ROW FORMAT SERDE 
  'org.apache.hadoop.hive.ql.io.parquet.serde.ParquetHiveSerDe' 
STORED AS INPUTFORMAT 
  'org.apache.hadoop.hive.ql.io.parquet.MapredParquetInputFormat' 
OUTPUTFORMAT 
  'org.apache.hadoop.hive.ql.io.parquet.MapredParquetOutputFormat'
LOCATION
  's3://how-finalproject-curated/'
TBLPROPERTIES (
  'CreatedByJob'='how_final_curated', 
  'CreatedByJobRun'='jr_fabfcb0869faf6a6b79119b74e2d1ab818cecf12b236e05fcf312bab8d649c77', 
  'UpdatedByJob'='how_final_curated', 
  'UpdatedByJobRun'='jr_70f04a723b9c24fcd260251e0b58335c7380d7d017fcab1a229fdfd5b0231966', 
  'classification'='parquet', 
  'useGlueParquetWriter'='true')
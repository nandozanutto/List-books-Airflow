CREATE EXTERNAL TABLE `list_books_cleaned`(
  `isbn10_left` string, 
  `final_price` float, 
  `rating` string, 
  `reviews_count` bigint, 
  `list_id` string, 
  `list_name` string, 
  `list_updated` string, 
  `title` string, 
  `author` string, 
  `description` string, 
  `url` string, 
  `isbn10` string, 
  `rating2` string)
PARTITIONED BY ( 
  `published_date` date)
ROW FORMAT SERDE 
  'org.apache.hadoop.hive.ql.io.parquet.serde.ParquetHiveSerDe' 
STORED AS INPUTFORMAT 
  'org.apache.hadoop.hive.ql.io.parquet.MapredParquetInputFormat' 
OUTPUTFORMAT 
  'org.apache.hadoop.hive.ql.io.parquet.MapredParquetOutputFormat'
LOCATION
  's3://how-finalproject-cleaned/'
TBLPROPERTIES (
  'CreatedByJob'='how_final', 
  'CreatedByJobRun'='jr_a5b664b95031c9966839299abf56eb91a356b87674b1c3ab190c9619cf872b61', 
  'UpdatedByJob'='how_final', 
  'UpdatedByJobRun'='jr_f31f8b3dea5aa6520705dabc4d76ee52d0467f40061b96923b4946ac4466b881', 
  'classification'='parquet', 
  'useGlueParquetWriter'='true')
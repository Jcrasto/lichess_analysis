CREATE EXTERNAL TABLE `lichess.running_gamestrings`(
  `id_key` string,
  `move_number` string,
  `pgn_string` string)
PARTITIONED BY (
  `date` string)
ROW FORMAT SERDE
  'org.apache.hadoop.hive.ql.io.parquet.serde.ParquetHiveSerDe'
STORED AS INPUTFORMAT
  'org.apache.hadoop.hive.ql.io.parquet.MapredParquetInputFormat'
OUTPUTFORMAT
  'org.apache.hadoop.hive.ql.io.parquet.MapredParquetOutputFormat'
LOCATION
  's3://jcrasto-chess-analysis/running_gamestrings/'
TBLPROPERTIES (
  'CrawlerSchemaDeserializerVersion'='1.0',
  'CrawlerSchemaSerializerVersion'='1.0',
  'UPDATED_BY_CRAWLER'='running_gamestrings',
  'averageRecordSize'='90',
  'classification'='parquet',
  'compressionType'='none',
  'objectCount'='904',
  'partition_filtering.enabled'='true',
  'recordCount'='297804',
  'sizeKey'='8529447',
  'typeOfData'='file')
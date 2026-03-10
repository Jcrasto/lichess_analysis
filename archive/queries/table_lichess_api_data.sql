CREATE EXTERNAL TABLE `lichess.lichess_api_data`(
  `event` string,
  `site` string,
  `white` string,
  `black` string,
  `result` string,
  `utcdate` string,
  `utctime` string,
  `whiteelo` string,
  `blackelo` string,
  `whiteratingdiff` string,
  `blackratingdiff` string,
  `variant` string,
  `timecontrol` string,
  `eco` string,
  `termination` string,
  `gamestring` string,
  `fen` string,
  `setup` string,
  `id_key` string)
PARTITIONED BY (
  `date` string)
ROW FORMAT SERDE
  'org.apache.hadoop.hive.ql.io.parquet.serde.ParquetHiveSerDe'
STORED AS INPUTFORMAT
  'org.apache.hadoop.hive.ql.io.parquet.MapredParquetInputFormat'
OUTPUTFORMAT
  'org.apache.hadoop.hive.ql.io.parquet.MapredParquetOutputFormat'
LOCATION
  's3://jcrasto-chess-analysis/lichess_api_data/'
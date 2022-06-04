CREATE EXTERNAL TABLE `lichess.luckleland_results`(
  `white` string,
  `black` string,
  `count` integer,
  `win` integer,
  `loss` integer,
  `draw` integer,
  `id_key` string,
  `game_json` string)
PARTITIONED BY (
  `date` string)
ROW FORMAT SERDE
  'org.apache.hadoop.hive.ql.io.parquet.serde.ParquetHiveSerDe'
STORED AS INPUTFORMAT
  'org.apache.hadoop.hive.ql.io.parquet.MapredParquetInputFormat'
OUTPUTFORMAT
  'org.apache.hadoop.hive.ql.io.parquet.MapredParquetOutputFormat'
LOCATION
  's3://jcrasto-chess-analysis/luckleland_results/'
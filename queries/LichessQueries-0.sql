select * from lichess.lichess_api_data
where white = 'luckleland' and result = '1-0'
or black = 'luckleland' and result = '0-1'
limit 100

select count(*) from lichess.lichess_api_data 

select * from lichess.lichess_api_data order by date desc 


show create table lichess.lichess_api_data 

MSCK REPAIR TABLE lichess.lichess_api_data


--alter table lichess.lichess_api_data 
--drop partition (date='2021-11-09')

show partitions lichess.lichess_api_data

--drop table lichess.lichess_api_data 

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

select max(gamestring) from lichess.lichess_api_data 


select * from lichess.lichess_api_data order by date desc, utctime desc 

select utcdate || ' ' || utctime "date_time",
case
	when white = 'luckleland' then whiteelo 
	when black = 'luckleland' then blackelo 
end as "elo"
from lichess.lichess_api_data 
order by date desc, utctime desc 


select "date", white, black, "result", 1 as count,
case
	when white = 'luckleland' and result = '1-0' then 1
	when black = 'luckleland' and result = '0-1' then 1
	else 0 
end as "win",
case
	when black = 'luckleland' and result = '1-0' then 1
	when white = 'luckleland' and result = '0-1' then 1
	else 0
end as "loss",
case
	when result = '1/2-1/2' then 1
	else 0
end as "draw",
gamestring, id_key
from lichess.lichess_api_data
order by date desc


MSCK REPAIR TABLE lichess.luckleland_results

drop table lichess.luckleland_results

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

 ALTER TABLE lichess.luckleland_results ADD IF NOT EXISTS PARTITION (date = '2022-06-04') LOCATION 's3://jcrasto-chess-analysis/luckleland_results/date=2022-06-04'



 

select json_extract(game_json, '$.1a') "1a", json_extract(game_json, '$.1b') "1b" from lichess.luckleland_results



select json_extract(game_json, '$.1a') "1a", json_extract(game_json, '$.1b') "1b" , 
json_extract(game_json, '$.2a') "2a" , json_extract(game_json, '$.2b') "2b",
json_extract(game_json, '$.3a') "3a" , json_extract(game_json, '$.3b') "3b",
json_extract(game_json, '$.4a') "4a" , json_extract(game_json, '$.4b') "4b",
json_extract(game_json, '$.5a') "5a" , json_extract(game_json, '$.5b') "5b",
win,loss, count(*) "count"
from lichess.luckleland_results
--where black = 'luckleland'
--and "date" >= '2022-04-01'
group by json_extract(game_json, '$.1a') , json_extract(game_json, '$.1b') ,
json_extract(game_json, '$.2a') , json_extract(game_json, '$.2b'),
json_extract(game_json, '$.3a'), json_extract(game_json, '$.3b'),
json_extract(game_json, '$.4a') , json_extract(game_json, '$.4b'),
json_extract(game_json, '$.5a'), json_extract(game_json, '$.5b'),
win, loss
order by count(*) desc



select "date", sum(win), sum(loss), sum("count")
from lichess.luckleland_results 
group by "date" 
having sum("count") > 10
order by "date" desc


select date
, case when white='luckleland' then whiteelo else blackelo end "elo"
, count(*) over (partition by date)
, avg(cast(whiteelo as double)) over (partition by date)
, avg(cast(blackelo as double)) over (partition by date)
from lichess.lichess_api_data  
order by date desc

select date, count(*)
from lichess.lichess_api_data 
group by "date" 
having count(*) > 10
order by date desc


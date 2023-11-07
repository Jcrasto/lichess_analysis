select * from lichess.lichess_api_data 
where "date" >= '2022-07-01'
order by gamestring desc 


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


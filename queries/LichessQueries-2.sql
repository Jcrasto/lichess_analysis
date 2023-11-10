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


create view lichess.luckleland_results  as( 
select "date", white, black, "result",
case
	when white = 'luckleland' then 'white'
	when black = 'luckleland' then 'black'
	else null
end as "luckleland",
1 as count,
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
end as "draw", id_key
from lichess.lichess_api_data
)


select *  
from lichess.luckleland_results 
where date >= '2023-06-01'
order by date desc

select date, sum(count) "count", sum(win)"win", sum(loss)"loss", sum(draw) "draw"
from lichess.luckleland_results 
where date >= '2023-06-01'
group by date
order by date desc



select
	a.pgn_string,
    b.luckleland,
    SUM(b.count) AS total_games,
    cast(SUM(b.win) as double)/ cast(SUM(b.count)as double) AS win,
    cast(SUM(b.loss) as double) / cast(SUM(b.count) as double) AS loss,
    cast(SUM(b.draw) as double) / cast(SUM(b.count) as double) AS draw
FROM
    lichess.running_gamestrings a
LEFT JOIN
    lichess.luckleland_results b
ON
    a.id_key = b.id_key
WHERE
    a.date >= '2023-01-01'
GROUP by
	a.move_number,
	a.move,
	a.pgn_string,
    b.luckleland
having SUM(b.count) >= 5
and cast(SUM(b.loss) as double) / cast(SUM(b.count) as double) >= 0.75
ORDER BY
a.move_number ,
a.move,
loss DESC;
   
 
   
 select move_number, move, count(distinct pgn_string) "pgn_strings"
 from lichess.running_gamestrings 
 group by move_number, move
 order by move_number, move
 
 
show create table lichess.running_gamestrings 


select * 
from lichess.luckleland_results 
order by date desc



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



create view lichess.gamestring_results as(
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
GROUP by
	a.move_number,
	a.move,
	a.pgn_string,
    b.luckleland
having SUM(b.count) >= 5
ORDER BY
a.move_number ,
a.move,
loss DESC
) 



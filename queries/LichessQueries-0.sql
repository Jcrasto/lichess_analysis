SELECT *
FROM lichess.gamestring_results 
where loss >= .6
and pgn_string like '1. d4%' 

select pgn_string  
from lichess.running_gamestrings 
where move_number =20
and move = 1


select "date", gamestring, id_key
from lichess.lichess_api_data
where date >= '2023-11-01'
and date <= '2023-11-03'
order by "date" desc


SELECT *
FROM lichess.gamestring_results 
where loss >= .6
and pgn_string like '1. d4%' 

select pgn_string  
from lichess.running_gamestrings 
where move_number =20
and move = 1

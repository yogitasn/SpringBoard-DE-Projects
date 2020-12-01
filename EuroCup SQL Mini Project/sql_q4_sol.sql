/*Write a SQL query to compute a list showing the number of substitutions that
happened in various stages of play for the entire tournament.*/

SELECT 
    p.match_no,
    m.play_stage,
    count(*) as "no_of_substitutions"
FROM player_in_out as p
    JOIN match_mast as m
WHERE in_out='O'
GROUP BY 1,2;

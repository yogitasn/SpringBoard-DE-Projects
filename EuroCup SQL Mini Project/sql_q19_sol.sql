/*. Write a SQL query to find the number of captains who were also goalkeepers*/

SELECT mt_cpt.team_id,
	   count(mt_cpt.player_captain) 
       FROM match_captain as mt_cpt
	   JOIN player_mast as pl_mt
	   ON mt_cpt.team_id=pl_mt.team_id
WHERE pl_mt.posi_to_play='GK'
GROUP BY 1
ORDER BY 2 DESC LIMIT 1;

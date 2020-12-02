USE euro_cup_2016;
/*Write a SQL query that returns the total number of goals scored by each position on
each country’s team. Do not include positions which scored no goals.*/

SELECT 
     p.team_id,
     p.posi_to_play,
     count(*)
FROM player_mast as p
     JOIN goal_details as g
	 ON p.team_id=g.team_id
GROUP BY 1,2
HAVING count(*) > 0
ORDER BY 3 DESC;


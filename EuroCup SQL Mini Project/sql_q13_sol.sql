/*Write a SQL query to find all the defenders who scored a goal for their teams.*/

select p.team_id,
       p.posi_to_play,
       count(*)
from player_mast as p
     JOIN goal_details as g
     ON p.team_id=g.team_id
WHERE p.posi_to_play='DF' or p.posi_to_play='FD'
GROUP BY 1,2
ORDER BY 3 DESC;
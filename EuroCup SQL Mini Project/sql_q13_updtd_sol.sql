USE euro_cup_2016;
/*Write a SQL query to find all the defenders who scored a goal for their teams.*/

/*select p.player_id,
       p.posi_to_play,
       count(*)
from player_mast as p
     JOIN goal_details as g
     ON p.player_id=g.player_id
WHERE p.posi_to_play='DF'
UNION 
select p.player_id,
       p.posi_to_play,
       count(*)
from player_mast as p
     JOIN goal_details as g
     ON p.player_id=g.player_id
WHERE p.posi_to_play='FD'
GROUP BY 1,2
ORDER BY 3 DESC;
*/

/* Removed Union and included results of Defenders who scored a goal for their teams */
SELECT p.player_id,
       p.age,
       p.player_name,
       p.jersey_no,
       count(*) as "no_of_goals"
FROM player_mast as p
     JOIN goal_details as g
      ON p.player_id=g.player_id
WHERE p.posi_to_play IN('DF','FD')
GROUP BY 1
HAVING count(*)=1;


USE euro_cup_2016;
/*. Write a SQL query to find the number of captains who were also goalkeepers*/

SELECT count(*) as "No_Of_GK_Captains"
FROM player_mast as pm
JOIN match_captain as mc
ON pm.player_id=mc.player_captain
WHERE pm.posi_to_play='GK'
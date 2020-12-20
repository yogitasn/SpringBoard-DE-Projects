USE euro_cup_2016;
/*Write a SQL query to find the substitute players who came into the field in the first
half of play, within a normal play schedule.*/

-- SELECT player_id as "substitute_players"
	--   FROM player_in_out
-- WHERE in_out='I'
-- AND time_in_out<=45;

SELECT pm.* 
       FROM player_mast as pm
       JOIN player_in_out as p
       ON pm.player_id=p.player_id
       WHERE p.in_out='I'
	   AND p.play_schedule='NT'
	   AND p.play_half=1;

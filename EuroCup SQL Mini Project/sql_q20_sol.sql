USE euro_cup_2016;
/*Write a SQL query to find the substitute players who came into the field in the first
half of play, within a normal play schedule.*/

SELECT player_id as "substitute_players"
	   FROM player_in_out
WHERE in_out='I'
AND time_in_out<=45;

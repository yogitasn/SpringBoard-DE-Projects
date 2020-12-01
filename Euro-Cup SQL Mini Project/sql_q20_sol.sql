/*Write a SQL query to find the substitute players who came into the field in the first
half of play, within a normal play schedule.*/

SELECT player_id
FROM player_in_out
WHERE in_out='I'
AND time_in_out<=30;
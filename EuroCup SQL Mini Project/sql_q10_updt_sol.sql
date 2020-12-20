USE euro_cup_2016;
/* Write a SQL query to find all available information about the players under contract to
Liverpool F.C. playing for England in EURO Cup 2016. */

/* Removed redundant columns player_id,team_id and used = instead of LIKE */
SELECT p.player_name,
       p.jersey_no,
       p.posi_to_play,
       p.dt_of_bir,
       p.age,
       p.playing_club,
	   sc.country_name 
FROM player_mast as p
	 JOIN soccer_country as sc
	 ON p.team_id=sc.country_id
WHERE playing_club ='Liverpool'
AND sc.country_name='England';
       
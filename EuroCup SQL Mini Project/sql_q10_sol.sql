USE euro_cup_2016;
/* Write a SQL query to find all available information about the players under contract to
Liverpool F.C. playing for England in EURO Cup 2016. */

SELECT p.player_id,
	   p.team_id,
	   p.jersey_no,
       p.player_name,
       p.posi_to_play,
       p.dt_of_bir,
       p.age,
       p.playing_club,
	   sc.country_name 
FROM player_mast as p
	 JOIN soccer_country as sc
	 ON p.team_id=sc.country_id
WHERE playing_club LIKE '%Liverpool%'
AND sc.country_name='England';
       
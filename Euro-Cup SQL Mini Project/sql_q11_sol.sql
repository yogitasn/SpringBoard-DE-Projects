/*Write a SQL query to find the players, their jersey number, and playing club who
were the goalkeepers for England in EURO Cup 2016*/

SELECT p.player_name,
       p.jersey_no,
       p.playing_club
FROM player_mast as p
     JOIN soccer_country as sc
	 ON p.team_id=sc.country_id
WHERE sc.country_name='England' 
AND p.posi_to_play ='GK';
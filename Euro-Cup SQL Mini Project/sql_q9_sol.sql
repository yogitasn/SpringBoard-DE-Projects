USE euro_cup_2016;

/*Write a SQL query to find the goalkeeper’s name and jersey number, playing for
Germany, who played in Germany’s group stage matches*/

SELECT DISTINCT GERM.player_name,
                GERM.jersey_no
       FROM match_details m 
            JOIN (SELECT p.team_id,
				         p.jersey_no as jersey_no,
						 p.player_name as player_name,
						 p.posi_to_play
				         FROM player_mast as p
					          JOIN soccer_country as sc
							  ON p.team_id=sc.country_id
						WHERE sc.country_name='Germany'
				        AND p.posi_to_play='GK') as GERM
			ON GERM.team_id=m.team_id
	   WHERE m.play_stage='G';
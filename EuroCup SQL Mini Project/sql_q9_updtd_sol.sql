USE euro_cup_2016;

/*Write a SQL query to find the goalkeeper’s name and jersey number, playing for
Germany, who played in Germany’s group stage matches*/

-- SELECT DISTINCT gm.player_name,
   --             gm.jersey_no
-- FROM match_details m 
	-- JOIN 
     -- (SELECT p.team_id,
	   --      p.jersey_no as jersey_no,
		-- 	   p.player_name as player_name,
		-- p.posi_to_play
	  -- FROM player_mast as p
	  -- JOIN soccer_country as sc
	  -- ON p.team_id=sc.country_id
	  -- WHERE sc.country_name='Germany'
	  -- AND p.posi_to_play='GK') as gm
	  -- ON gm.team_id=m.team_id
-- WHERE m.play_stage='G';

-- New query as per review comment: There are redundant columns in subquery. 
-- The query can be simplified by selecting from 'player_mast' and having info from 'match_details' in subquery
SELECT  DISTINCT gm.player_name,
        gm.jersey_no
FROM (SELECT p.team_id, 
            p.jersey_no as jersey_no,
			p.player_name as player_name
	  FROM player_mast as p
	  JOIN match_details as md
	  WHERE p.posi_to_play='GK'
	  AND md.play_stage='G') gm
JOIN soccer_country as sc
ON gm.team_id=sc.country_id
WHERE sc.country_name='Germany';
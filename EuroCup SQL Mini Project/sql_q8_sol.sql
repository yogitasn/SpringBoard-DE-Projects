use euro_cup_2016;

/* Write a SQL query to find the match number for the game with the highest number of
penalty shots, and which countries played that match.*/
SELECT
	DISTINCT sc.country_name 
	FROM soccer_country as sc
	JOIN
	(SELECT m.match_no,
            p.team_id as team_id,
            count(*) 
	        FROM penalty_shootout as p
				 JOIN match_details as m
				 ON p.team_id=m.team_id 
			WHERE p.score_goal='Y'
			AND m.win_lose='W'
			GROUP by 1,2
			ORDER BY 3 DESC) as penalty_scores
			ON sc.country_id=penalty_scores.team_id;

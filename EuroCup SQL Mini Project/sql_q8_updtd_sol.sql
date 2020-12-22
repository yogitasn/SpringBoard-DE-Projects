use euro_cup_2016;

/* Write a SQL query to find the match number for the game with the highest number of
penalty shots, and which countries played that match.*/

/*SELECT s.match_no,
       s.count,
       sc.country_name
FROM
	(SELECT p.match_no,
			count(*) as count
			FROM penalty_shootout as p
			GROUP by 1
			ORDER BY 2 DESC 
			LIMIT 1) as s
	JOIN match_details as md
	ON s.match_no=md.match_no
	JOIN soccer_country as sc
	ON sc.country_id=md.team_id;
*/

-- Modified the above query by removing Limit 1 and using max in a subquery
SELECT md.match_no,
       sc.country_name
 FROM match_details as md
 JOIN soccer_country as sc
   ON md.team_id=sc.country_id
WHERE md.match_no=(SELECT match_no
					FROM penalty_shootout
					GROUP BY match_no
					HAVING COUNT(*)=
						(SELECT MAX(match_count) 
                          FROM (SELECT match_no,
                                       COUNT(*) AS match_count
								  FROM penalty_shootout
								  GROUP BY match_no) 
								  t1) ) 

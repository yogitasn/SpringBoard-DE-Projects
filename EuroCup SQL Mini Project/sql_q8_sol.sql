use euro_cup_2016;

/* Write a SQL query to find the match number for the game with the highest number of
penalty shots, and which countries played that match.*/

SELECT s.match_no,
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

SELECT md.match_no,
       sc.country_name
from match_details as md
JOIN soccer_country as sc
ON sc.country_id=md.team_id
WHERE md.match_no = (SELECT tab.match_no,MAX(count) 
           FROM (SELECT p.match_no,
			count(*) as count
			FROM penalty_shootout as p
            GROUP by 1) tab)
            
SELECT tab.match_no 
           FROM (SELECT p.match_no,
			count(*) as count
			FROM penalty_shootout as p
            GROUP by 1) tab
           FROM (SELECT p.match_no,
			count(*) as count
			FROM pensalty_shootout as p
            GROUP by 1) s
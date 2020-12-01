/*Write a SQL query to find the referees who booked the most number of players.*/

SELECT referee_id
	   FROM
       (SELECT referee_id,
               count(*) as count 
		FROM  match_mast as m
		JOIN player_booked as pb
		ON m.match_no=pb.match_no
		GROUP BY m.referee_id
		ORDER BY count DESC
		LIMIT 1) as s;
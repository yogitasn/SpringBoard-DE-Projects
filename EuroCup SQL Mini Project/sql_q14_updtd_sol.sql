USE euro_cup_2016;
/*Write a SQL query to find referees and the number of bookings they made for the
entire tournament. Sort your answer by the number of bookings in descending order.*/

/*SELECT m.referee_id,
       count(*) 
       FROM match_mast as m
JOIN player_booked as pb
     ON m.match_no=pb.match_no
GROUP BY 1
ORDER BY 2 DESC;
*/

-- Included the referee name instead of id
SELECT rm.referee_name,
       count(*) 
 FROM referee_mast as rm
  JOIN  match_mast as m
    ON m.referee_id=rm.referee_id
  JOIN player_booked as pb
	ON m.match_no=pb.match_no
GROUP BY 1
ORDER BY 2 DESC;
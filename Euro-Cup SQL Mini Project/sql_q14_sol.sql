/*Write a SQL query to find referees and the number of bookings they made for the
entire tournament. Sort your answer by the number of bookings in descending order.*/

SELECT m.referee_id,
       count(*) 
       FROM match_mast as m
JOIN player_booked as pb
     ON m.match_no=pb.match_no
GROUP BY 1
ORDER BY 2 DESC;

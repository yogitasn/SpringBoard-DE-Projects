use euro_cup_2016;

/* Write a SQL query to find the number of bookings that happened in stoppage time*/
SELECT 
     p.match_no,
     COUNT(*)
FROM player_booked as p
     JOIN match_mast as m
	 ON p.match_no=m.match_no
WHERE p.booking_time=m.stop1_sec or p.booking_time=m.stop2_sec
GROUP BY 1;
/*.Write a SQL query to find the highest number of foul cards given in one match.
*/

SELECT pb.match_no,
       count(*) 
FROM player_booked as pb
     JOIN match_mast as mm
     ON pb.match_no=mm.match_no
WHERE pb.sent_off='Y'
GROUP BY pb.match_no
ORDER BY 2 DESC LIMIT 1;

/*.Write a SQL query to find referees and the number of matches they worked in each
venue*/

SELECT referee_id,
       venue_id,
       count(match_no)
FROM match_mast
GROUP BY 1,2;
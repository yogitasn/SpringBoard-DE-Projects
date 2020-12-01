use euro_cup_2016;

/*Write a SQL query to find all the venues where matches with penalty shootouts were
played.*/

SELECT 
     sv.venue_name
FROM soccer_venue as sv
     JOIN match_mast as m
     ON m.venue_id=sv.venue_id
WHERE m.decided_by='P';
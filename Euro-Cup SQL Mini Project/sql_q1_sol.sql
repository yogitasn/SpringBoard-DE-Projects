USE euro_cup_2016;

/*Write a SQL query to find the date EURO Cup 2016 started on */
SELECT 
     play_date as "Euro_Cup_Start_Date" 
FROM match_mast
ORDER BY play_date ASC
LIMIT 1;


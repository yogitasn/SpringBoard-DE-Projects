USE euro_cup_2016;

/* Write a SQL query to find the number of matches that were won by a single point, but
do not include matches decided by penalty shootout.*/
-- SELECT 
   --  count(*) as "matches_won_by_single_point"
-- FROM match_mast
-- WHERE decided_by='N'
-- AND goal_score LIKE '%1%'
-- AND results='WIN';

/* Simplified solution using table 'match_details'*/

SELECT 
     count(*) as "matches_won_by_single_point"
FROM match_details
WHERE decided_by='N'
AND goal_score=1
AND win_lose='W';
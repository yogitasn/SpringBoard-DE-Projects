USE euro_cup_2016;

/* Number of matches won by penalty shootout */
SELECT 
	COUNT(p.match_no) as "no_of_matches_won_by_penalty"
FROM penalty_shootout p
	JOIN match_details m
    ON p.match_no=m.match_no
WHERE m.win_lose='W';
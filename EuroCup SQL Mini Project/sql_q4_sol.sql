USE euro_cup_2016;

/*Write a SQL query to compute a list showing the number of substitutions that
happened in various stages of play for the entire tournament.*/
  
SELECT mm.match_no,
       mm.play_stage,
	   s.count as "no_of_substitutions"
FROM match_mast as mm
     JOIN (SELECT
			p.match_no,
			count(*) as count
			FROM player_in_out as p
				JOIN match_mast as m
				ON p.match_no=m.match_no
			WHERE p.in_out='O'
			GROUP BY 1) as s
	  ON s.match_no=mm.match_no;
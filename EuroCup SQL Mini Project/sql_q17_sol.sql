USE euro_cup_2016;

/*Write a SQL query to find the country where the most assistant referees come from,
and the count of the assistant referees.*/

SELECT country_name,
       count_asst_referees 
FROM soccer_country as sc
	 JOIN (SELECT country_id,
				  count(ass_ref_id) as count_asst_referees 
			FROM asst_referee_mast
			GROUP BY 1
			ORDER BY 2 DESC 
			LIMIT 1) as s
       ON sc.country_id=s.country_id;

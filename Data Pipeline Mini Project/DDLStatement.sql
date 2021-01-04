create database event_ticket;

use event_ticket;

CREATE TABLE ticket_sales(
		ticket_id INT,
		trans_date INT,
		event_id INT,
		event_name VARCHAR(50),
		event_date DATE,
		event_type VARCHAR(10),
		event_city VARCHAR(20),
		event_addr VARCHAR(100),
		customer_id INT,
		price DECIMAL,
		num_tickets INT
        )
        
SELECT * FROM ticket_sales;

WITH top_selling_tickets
AS (SELECT 
       event_name,
       ROW_NUMBER() OVER (
          ORDER BY num_tickets DESC) row_num
    FROM 
       ticket_sales
   )
SELECT 
   event_name
FROM 
   top_selling_tickets
WHERE 
   row_num <= 3;
USE springboardopt;

-- -------------------------------------
SET @v1 = 1612521;
SET @v2 = 1145072;
SET @v3 = 1828467;
SET @v4 = 'MGT382';
SET @v5 = 'Amber Hill';
SET @v6 = 'MGT';
SET @v7 = 'EE';			  
SET @v8 = 'MAT';

-- 1. List the name of the student with id equal to v1 (id).
-- Bottleneck: For the below query a Full Table scan takes place which is very costly for large tables. This is indicated in Execution plan in red
	-- Query Cost:41.00
    -- FULL TABLE SCAN
-- To resolve the bottleneck, creating index id 
DROP INDEX id_idx on Student;
CREATE INDEX id_idx ON  Student(id);
SELECT name FROM Student WHERE id = @v1;
  -- After creating index, Query Cost=0.35

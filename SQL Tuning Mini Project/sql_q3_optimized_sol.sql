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

-- 3. List the names of students who have taken course v4 (crsCode).
-- Old Query
-- SELECT name FROM Student WHERE id IN (SELECT studId FROM Transcript WHERE crsCode = @v4);

-- Using Joins lowered the execution time and the Query cost from 3.55 to 2.15 as it eliminated the Full Table scan for the 'Transcript' Table
SELECT Student.name FROM Student
      INNER JOIN Transcript
         ON Student.id=Transcript.studId
WHERE crsCode=@v4;

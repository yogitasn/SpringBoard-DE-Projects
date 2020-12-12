/* Primary and Foreign key setup for the tables */

ALTER TABLE Student ADD PRIMARY KEY (id);

ALTER TABLE professor ADD PRIMARY KEY (id);

ALTER TABLE Transcript ADD PRIMARY KEY(crsCode,semester);

ALTER TABLE teaching
ADD FOREIGN KEY (profId) REFERENCES professor(id);


ALTER TABLE Transcript
ADD FOREIGN KEY (studId) REFERENCES Student(id);



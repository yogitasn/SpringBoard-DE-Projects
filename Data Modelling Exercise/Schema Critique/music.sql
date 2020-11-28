-- from the terminal run:
-- psql < music.sql

DROP DATABASE IF EXISTS music;

CREATE DATABASE music;

USE music;

CREATE TABLE artists
( 
    artist_id SERIAL PRIMARY KEY,
	name VARCHAR(100)
);

CREATE TABLE producers
(
    producer_id SERIAL PRIMARY KEY,
    name VARCHAR(100)
);

CREATE TABLE songs
(
  song_id SERIAL PRIMARY KEY,
  title VARCHAR(50),
  duration_in_seconds INT NOT NULL,
  release_date VARCHAR(20)
);


CREATE TABLE album
(   
	album_id SERIAL PRIMARY KEY,
    album_name VARCHAR(50),
    song_id BIGINT UNSIGNED,
    producer_id BIGINT UNSIGNED,
    FOREIGN KEY(song_id) REFERENCES songs(song_id),
    FOREIGN KEY(producer_id) REFERENCES producers(producer_id)
    
);



INSERT INTO artists
	(name)
VALUES
('Hanson'),
('Queen'),
('Mariah Cary'),
('Boyz II Men'),
('Lady Gaga'),
('Bradley Cooper'),
('Nickelback'),
('Jay Z'),
('Alicia Keys'),
('Katy Perry'),
('Juicy J'),
('Maroon 5'),
('Christina Aguilera'),
('Avril Lavigne'),
('Destiny''s Child');

INSERT INTO producers
    (name)
VALUES
('Dust Brothers'),
('Stephen Lironi'),
('Roy Thomas Baker'),
('Walter Afanasieff'),
('Benjamin Rice'),
('Rick Parashar'),
('Al Shux'),
('Max Martin'),
('Cirkut'),
('Shellback'),
('Benny Blanco'),
('The Matrix'),
('Darkchild');

/*INSERT INTO songs
  (title, duration_in_seconds, release_date, artist_id)
VALUES
  ('MMMBop', 238, '04-15-1997', ((SELECT artist_id from artists where name='Hanson'))),
  ('Bohemian Rhapsody', 355, '10-31-1975', ((SELECT artist_id from artists where name='Queen'))),
  ('One Sweet Day', 282, '11-14-1995', ((SELECT artist_id from artists where name='Mariah Cary'))),
  ('One Sweet Day', 282, '11-14-1995', ((SELECT artist_id from artists where name='Boyz II Men'))),
  ('Shallow', 216, '09-27-2018', ((SELECT artist_id from artists where name='Lady Gaga'))),
  ('Shallow', 216, '09-27-2018', ((SELECT artist_id from artists where name='Bradley Cooper'))),
  ('How You Remind Me', 223, '08-21-2001', ((SELECT artist_id from artists where name='Nickelback'))),
  ('New York State of Mind', 276, '10-20-2009', ((SELECT artist_id from artists where name='Jay Z'))),
  ('New York State of Mind', 276, '10-20-2009', ((SELECT artist_id from artists where name='Alicia Keys'))),
  ('Dark Horse', 215, '12-17-2013', ((SELECT artist_id from artists where name='Katy Perry'))),
  ('Dark Horse', 215, '12-17-2013', ((SELECT artist_id from artists where name='Juicy J'))),
  ('Moves Like Jagger', 201, '06-21-2011', ((SELECT artist_id from artists where name='Maroon 5'))),
  ('Moves Like Jagger', 201, '06-21-2011', ((SELECT artist_id from artists where name='Christina Aguilera'))),
  ('Complicated', 244, '05-14-2002', ((SELECT artist_id from artists where name='Avril Lavigne'))),
  ('Say My Name', 240, '11-07-1999', ((SELECT artist_id from artists where name='Destiny''s Child')));
*/

INSERT INTO songs
  (title, duration_in_seconds, release_date)
VALUES
  ('MMMBop', 238, '04-15-1997'),
  ('Bohemian Rhapsody', 355, '10-31-1975'),
  ('One Sweet Day', 282, '11-14-1995'),
  ('Shallow', 216, '09-27-2018'),
  ('How You Remind Me', 223, '08-21-2001'),
  ('New York State of Mind', 276, '10-20-2009'),
  ('Dark Horse', 215, '12-17-2013'),
  ('Moves Like Jagger', 201, '06-21-2011'),
  ('Complicated', 244, '05-14-2002'),
  ('Say My Name', 240, '11-07-1999');

INSERT INTO album
    (album_name,song_id,producer_id)
VALUES
('Middle of Nowhere',((SELECT song_id from songs where title='MMMBop')),((SELECT producer_id from producers where name='Dust Brothers'))),
('Middle of Nowhere',((SELECT song_id from songs where title='MMMBop')),((SELECT producer_id from producers where name='Stephen Lironi'))),
('A Night at the Opera',((SELECT song_id from songs where title='Queen')),((SELECT producer_id from producers where name='Roy Thomas Baker'))),
('Daydream',((SELECT song_id from songs where title='One Sweet Day')),((SELECT producer_id from producers where name='Walter Afanasieff'))),
('A Star Is Born',((SELECT song_id from songs where title='Shallow')),((SELECT producer_id from producers where name='Benjamin Rice'))),
('Silver Side Up',((SELECT song_id from songs where title='How You Remind Me')),((SELECT producer_id from producers where name='Rick Parashar'))),
('The Blueprint 3',((SELECT song_id from songs where title='New York State of Mind')),((SELECT producer_id from producers where name='Al Shux'))),
('Prism',((SELECT song_id from songs where title='Dark Horse')),((SELECT producer_id from producers where name='Max Martin'))),
('Prism',((SELECT song_id from songs where title='Dark Horse')),((SELECT producer_id from producers where name='Cirkut'))),
('Hands All Over',((SELECT song_id from songs where title='Moves Like Jagger')),((SELECT producer_id from producers where name='Shellback'))),
('Hands All Over',((SELECT song_id from songs where title='Moves Like Jagger')),((SELECT producer_id from producers where name='Benny Blanco'))),
('Let Go',((SELECT song_id from songs where title='Complicated')),((SELECT producer_id from producers where name='The Matrix'))),
('The Writing''s on the Wall',((SELECT song_id from songs where title='Say My Name')),((SELECT producer_id from producers where name='Darkchild')));



select * from artists;
select * from songs;
select * from album;
select * from producers;


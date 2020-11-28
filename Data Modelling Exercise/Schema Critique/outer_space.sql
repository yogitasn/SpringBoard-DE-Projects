-- from the terminal run:
-- psql < outer_space.sql

DROP DATABASE IF EXISTS outer_space;

CREATE DATABASE outer_space;
use outer_space;

CREATE TABLE moons
( 
   moon_id SERIAL PRIMARY KEY,
   name VARCHAR(100)
);

CREATE TABLE planets
(
  id SERIAL PRIMARY KEY,
  name VARCHAR(50),
  orbital_period_in_years FLOAT NOT NULL,
  orbits_around VARCHAR(50),
  galaxy VARCHAR(10),
  moon_id BIGINT UNSIGNED,
  FOREIGN KEY(moon_id) REFERENCES moons(moon_id)
);

INSERT INTO moons
    (name)
VALUES
    ('The Moon'),
    ('Phobos'),
    ('Deimos'),
    ('Naiad'),
    ('Thalassa'),
    ('Despina'),
    ('Galatea'),
    ('Larissa'),
    ('S/2004 N 1'),
    ('Proteus'),
    ('Triton'),
    ('Nereid'),
    ('Halimede'),
    ('Sao'),
    ('Laomedeia'),
    ('Psamathe');
    

INSERT INTO planets
  (name, orbital_period_in_years, orbits_around, galaxy, moon_id)
VALUES
  ('Earth', 1.00, 'The Sun', 'Milky Way', (select moon_id from moons where name='The Moon')),
  ('Mars', 1.88, 'The Sun', 'Milky Way', (select moon_id from moons where name='Phobos')),
  ('Mars', 1.88, 'The Sun', 'Milky Way', (select moon_id from moons where name='Deimos')),
  ('Venus', 0.62, 'The Sun', 'Milky Way', NULL),
  ('Neptune', 164.8, 'The Sun', 'Milky Way', (select moon_id from moons where name='Naiad')), 
  ('Neptune', 164.8, 'The Sun', 'Milky Way', (select moon_id from moons where name='Thalassa')), 
  ('Neptune', 164.8, 'The Sun', 'Milky Way', (select moon_id from moons where name='Despina')), 
  ('Neptune', 164.8, 'The Sun', 'Milky Way', (select moon_id from moons where name='Galatea')), 
  ('Neptune', 164.8, 'The Sun', 'Milky Way', (select moon_id from moons where name='Larissa')), 
  ('Neptune', 164.8, 'The Sun', 'Milky Way', (select moon_id from moons where name='S/2004 N 1')), 
  ('Neptune', 164.8, 'The Sun', 'Milky Way', (select moon_id from moons where name='Proteus')), 
  ('Neptune', 164.8, 'The Sun', 'Milky Way', (select moon_id from moons where name='Triton')), 
  ('Neptune', 164.8, 'The Sun', 'Milky Way', (select moon_id from moons where name='Nereid')),
  ('Neptune', 164.8, 'The Sun', 'Milky Way', (select moon_id from moons where name='Halimede')),
  ('Neptune', 164.8, 'The Sun', 'Milky Way', (select moon_id from moons where name='Sao')), 
  ('Neptune', 164.8, 'The Sun', 'Milky Way', (select moon_id from moons where name='Laomedeia')), 
  ('Neptune', 164.8, 'The Sun', 'Milky Way', (select moon_id from moons where name='Psamathe')), 
  ('Neptune', 164.8, 'The Sun', 'Milky Way', (select moon_id from moons where name='Neso')),
  ('Proxima Centauri b', 0.03, 'Proxima Centauri', 'Milky Way', NULL),
  ('Gliese 876 b', 0.23, 'Gliese 876', 'Milky Way', NULL);

select * from planets;
select * from moons;

select m.name
from moons m
JOIN planets p
ON m.moon_id=p.moon_id
where p.name='Earth'
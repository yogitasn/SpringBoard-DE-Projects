drop database if exists soccergame;
create database soccergame;
use soccergame;

CREATE TABLE IF NOT EXISTS teams(
		team_id INT PRIMARY KEY NOT NULL,
		home_ground VARCHAR(20),
		home_wins INT,
		home_draws INT, 
		home_defeats INT,
		home_for INT,
		home_against INT,
		away_wins INT,
		away_draws INT,
		away_defeats INT,
		away_for INT,
		away_against INT);

create table IF NOT EXISTS players(
		player_id int PRIMARY KEY NOT NULL,
		team_id int,
		first_name VARCHAR(20),
		last_name VARCHAR(20),
		position VARCHAR(20),
		goals_scored INT,
		goals_received INT,
		assists INT,
		minutes_played INT,
		yellow_cards_received INT,
		red_cards_received INT,
		FOREIGN KEY (team_id) REFERENCES teams(team_id)
        );

create table IF NOT EXISTS goals_scored(
		goal_id INT PRIMARY KEY NOT NULL,
		game_week INT,
		player_id INT,
		goal_time INT,
		assisted_by INT,
		FOREIGN KEY(player_id) REFERENCES players(player_id));

create table if not EXISTS fixtures(
        season_id INT PRIMARY KEY NOT NULL,
		game_week INT,
		home_team VARCHAR(45),
		away_team VARCHAR(45),
		home_team_score INT,
		away_team_score INT,
		fixture_date DATETIME
);

create table if not EXISTS player_match(
		match_id INT PRIMARY KEY NOT NULL,
		game_week INT,
		player_id INT,
		goals_conceeded INT,
		assists INT,
		mins_played INT,
		yellow_card INT,
		red_card INT,
        referee_id INT,
        FOREIGN KEY (referee_id) REFERENCES referee(referee_id),
        FOREIGN KEY (player_id) REFERENCES players(player_id),
        FOREIGN KEY(season_id) REFERENCES fixtures(season_id)
        );

CREATE TABLE referee(
        referee_id INT PRIMARY KEY NOT NULL,
        name VARCHAR(30));
	
CREATE TABLE results(
        id INT PRIMARY KEY NOT NULL,
        team_id INT,
        match_id INT,
        result VARCHAR(30),
        FOREIGN KEY(team_id) REFERENCES teams(team_id),
        FOREIGN KEY(match_id) REFERENCES player_match(match_id));
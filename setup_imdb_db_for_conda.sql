-- Set up the DB in testing with conda

-- Drop existing database if exists
DROP DATABASE IF EXISTS imdb_db;

-- Create user and database
CREATE DATABASE imdb_db;
-- GRANT ALL PRIVILEGES ON DATABASE imdb_db TO imdb_user;
-- \c imdb_db imdb_user
\c imdb_db

-- Create the tables
CREATE TABLE title_basics (
    tconst VARCHAR PRIMARY KEY,
    titleType VARCHAR,
    primaryTitle VARCHAR,
    originalTitle VARCHAR,
    isAdult BOOLEAN,
    startYear INTEGER,
    endYear INTEGER,
    runtimeMinutes INTEGER,
    genres VARCHAR
);

CREATE TABLE title_ratings (
    tconst VARCHAR PRIMARY KEY,
    averageRating FLOAT,
    numVotes INTEGER
);

-- Copy data from files
\copy title_basics FROM '/raw_data/title.basics.tsv_sample' WITH (FORMAT 'csv', DELIMITER E'\t', HEADER true, NULL '\N');
\copy title_ratings FROM '/raw_data/title.ratings.tsv' WITH (FORMAT 'csv', DELIMITER E'\t', HEADER true, NULL '\N');

-- Verification
SELECT COUNT(*) FROM title_basics;
SELECT COUNT(*) FROM title_ratings;
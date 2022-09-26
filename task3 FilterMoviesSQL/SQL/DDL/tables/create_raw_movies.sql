CREATE TABLE IF NOT EXISTS stg_moviesdb.raw_movies (
    movie_id INT NOT NULL PRIMARY KEY,
    title TEXT,
    genres TEXT
    );
TRUNCATE TABLE stg_moviesdb.raw_movies;
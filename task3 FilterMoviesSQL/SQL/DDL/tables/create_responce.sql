CREATE TABLE IF NOT EXISTS moviesdb.responce (
    genre text,
    title text,
    movie_year int DEFAULT NULL,
    rating float DEFAULT NULL
    );
TRUNCATE TABLE moviesdb.responce;
CREATE TABLE IF NOT EXISTS stg_moviesdb.raw_ratings (
    user_id INT,
    movie_id INT,
    rating FLOAT,
    time_stamp INT
    );
TRUNCATE TABLE stg_moviesdb.raw_ratings;

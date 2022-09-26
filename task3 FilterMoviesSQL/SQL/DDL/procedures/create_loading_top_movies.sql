DROP PROCEDURE IF EXISTS moviesdb.loading_top_movies;
CREATE PROCEDURE moviesdb.loading_top_movies()
INSERT INTO moviesdb.top_movies(genre, title, movie_year, rating)
SELECT genres, title, movie_year, rating
FROM stg_moviesdb.movies
JOIN stg_moviesdb.ratings
ON movies.movie_Id = ratings.movie_Id;

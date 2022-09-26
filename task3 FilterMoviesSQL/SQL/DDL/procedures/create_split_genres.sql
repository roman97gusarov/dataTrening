DROP PROCEDURE IF EXISTS moviesdb.split_genres;
CREATE PROCEDURE moviesdb.split_genres()
INSERT INTO moviesdb.movies (title, genre, movie_year,rating)
SELECT
  moviesdb.top_movies.title,
  SUBSTRING_INDEX(SUBSTRING_INDEX(moviesdb.top_movies.genre, '|', numbers.n), '|', -1) genre, movie_year, rating
FROM
  (SELECT 1 n UNION ALL SELECT 2
   UNION ALL SELECT 3 UNION ALL SELECT 4 UNION ALL SELECT 5) numbers INNER JOIN moviesdb.top_movies
  ON CHAR_LENGTH(moviesdb.top_movies.genre)
     -CHAR_LENGTH(REPLACE(moviesdb.top_movies.genre, '|', ''))>=numbers.n-1

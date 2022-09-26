DROP PROCEDURE IF EXISTS moviesdb.filter_movies;
CREATE PROCEDURE moviesdb.filter_movies(
			  IN titles TEXT
            , IN year_min INT
            , IN year_max INT
            , IN genres TEXT
            , IN limit_films INT
)

INSERT INTO moviesdb.responce (genre,title,movie_year,rating)
SELECT genre, title, movie_year, rating
FROM moviesdb.movies
WHERE TITLE REGEXP titles
AND MOVIE_YEAR >= year_min
AND MOVIE_YEAR <= year_max
AND GENRE REGEXP genres
ORDER BY  rating DESC , movie_year DESC , title ASC, genre LIMIT limit_films


CREATE OR REPLACE VIEW stg_moviesdb.movies AS
SELECT movie_id
       , REGEXP_SUBSTR(title, '(.+)(?= \\(\\d{4}\\)$)') AS title
       , genres
       , CAST(REGEXP_SUBSTR(REGEXP_SUBSTR(title, '(?=.+)( \\(\\d{4}\\)$)'), '\\d{4}') AS SIGNED) AS movie_year
FROM stg_moviesdb.raw_movies;


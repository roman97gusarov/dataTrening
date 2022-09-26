CREATE OR REPLACE VIEW stg_moviesdb.ratings AS
SELECT movie_id, AVG(rating) AS rating
FROM stg_moviesdb.raw_ratings
GROUP BY movie_id
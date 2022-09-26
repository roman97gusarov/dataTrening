LOAD DATA LOCAL INFILE "./data/movies.csv"
INTO TABLE stg_moviesdb.raw_movies LINES TERMINATED BY "\n" IGNORE 1 LINES (@var1)
SET movie_id = substring_index(@var1,',',1),
genres = REPLACE(substring_index(@var1,',',-1),'\r',''),
title = REPLACE(REPLACE(REPLACE(@var1,concat(movie_id,','),''),concat(',',genres),''),'\r','');

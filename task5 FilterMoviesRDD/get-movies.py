import datetime
import re
from pyspark import SparkConf, SparkContext


def read_data(path: str):
    try:
        raw_data = sc.textFile(path)
        header = raw_data.first()
        rdd_data = raw_data.filter(lambda row: row != header)
    except Exception as e:
        print(e)

    return rdd_data


def normalize_title(movie_title):
    try:
        match = re.match(r'(.*)[ ]*\((\d{4}(\-\d{4})?)\)[ ]*$', movie_title)
        movie_title = match.group(1).strip()
        release_year = int(match.group(2)[-4:])
    except AttributeError:
        release_year = None

    return movie_title, release_year


def normalize_movie(movie):
    match = re.match('(\d+),"?(.+[^"])"?,(.+)', movie)
    movieId, raw_title, movie_genres = match.groups()
    movie_genres = movie_genres.split('|')

    title, year = normalize_title(raw_title)
    return [(int(movieId), (title, year, genre)) for genre in movie_genres]


def get_filtered_movies(movie, regexp, genres, year_from, year_to):
    regexp = '.*' if regexp is None else regexp
    genres = '.*' if genres is None else genres
    year_from = 1895 if (year_from is None or year_from < 1895) else year_from
    year_to = datetime.now().year if (year_to is None or year_to < year_from) else year_to

    movieId, (title, year, movie_genre) = movie
    if re.match(rf'\b{regexp}\b', title) and \
            year and year_from <= year <= year_to and \
            re.match(genres, movie_genre):
        return True


def normalize_rating(rating_line):
    if rating_line == "userId,movieId,rating,timestamp":
        return(None,None)
    else:
        splited_rating_line = rating_line.split(',')
        movieId, rating =  splited_rating_line[1:3]
        return int(movieId), (float(rating), 1)


def movie_mapper(movieId, movie):
    (movie_title, release_year, genre), avg_rating = movie
    return genre, (movie_title, release_year, avg_rating)


def rating_reducer(sumRatings, countRatings):
    return [sumRatings + countRatings for sumRatings, countRatings in zip(sumRatings, countRatings)]


def createCombiner(movie):
    return [movie]


def margeValue(movieCombiner, movie):
    movieCombiner.append(movie)
    return movieCombiner


def margeCombiner(movieCombiner, movie):
    movieCombiner.extend(movie)
    return movieCombiner


def get_top_rated_movies(movies, N):
    N = 99999999 if (N is None or N < 1) else N
    return sorted(movies, key=lambda movies: (movies[2], movies[1], movies[0]), reverse=True)[:N]


def main():
    rdd_movies = read_data("data/movies.csv")
    rdd_ratings = read_data("data/ratings.csv")
    rdd_filtered_movies = rdd_movies \
        .flatMap(lambda movie: normalize_movie(movie)) \
        .filter(lambda movie: get_filtered_movies(movie, regexp, genres, year_from, year_to))
    rdd_avg_rating = rdd_ratings \
        .map(normalize_rating) \
        .reduceByKey(lambda sumRatings, countRatings: rating_reducer(sumRatings, countRatings)) \
        .mapValues(lambda sumCount: sumCount[0] / sumCount[1])
    rdd_result = rdd_filtered_movies.join(rdd_avg_rating) \
        .map(lambda line: movie_mapper(*line)) \
        .combineByKey(createCombiner, margeValue, margeCombiner) \
        .mapValues(lambda movies: get_top_rated_movies(movies, N))
    rdd_result.saveAsTextFile("tmp.result")
    

if __name__ == '__main__':
    sconf = SparkConf()
    sconf.setMaster("local[*]") \
        .setAppName("get-movies")

    sc = SparkContext(conf=sconf)

    regexp = 'Scooby'
    genres = ''
    year_from = 1895
    year_to = 2017
    N = 10

    main()

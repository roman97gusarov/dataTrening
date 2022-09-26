import argparse
import re
from pyspark.sql import SparkSession, Window
from pyspark.sql.functions import *


def parse_args():
    parser = argparse.ArgumentParser(description='movies filter')
    parser.add_argument('-n', '--number', type=int, help='number of films', default=10)
    parser.add_argument('-g', '--genres', nargs='*', required=False, help='enter the genres you want', default="Drama")
    parser.add_argument('-yf', '--year_from', type=int, help='enter the year from', default=0)
    parser.add_argument('-yt', '--year_to', type=int, help='enter the year to', default=2030)
    parser.add_argument('-rg', '--regexp', type=str, help='name of the ".+name"', default='(?i)')

    return parser.parse_args()


def normalize_title(title):
    exp = r'\(\d{4}\)'
    matches = re.findall(exp, title)
    if len(matches) != 0:
        title = title.replace(matches[-1], "")
    title = title.replace('"', "").strip()

    return title


def get_year_from_title(title):
    exp = r'\(\d{4}\)'
    year = -1
    matches = re.findall(exp, title)
    if len(matches) != 0:
        year = int(matches[-1][1:5])

    return year


def main():
    spark = SparkSession.builder \
        .master("local") \
        .config("spark.executor.memory", "500mb") \
        .appName("get-movies") \
        .getOrCreate()

    movies_path = "data/movies.csv"
    ratings_path = "data/ratings.csv"

    movies_df = spark.read.format('csv').option('header', 'true').load(movies_path)
    ratings_df = spark.read.format('csv').option('header', 'true').load(ratings_path)

    get_title = spark.udf.register("get_title", normalize_title)
    get_year = spark.udf.register("get_year", get_year_from_title)

    normalized_movies = movies_df.select('movieId', get_title('title').alias('title'), get_year('title').alias('year'),
                                         explode(split('genres', '\\|')).alias('genre'))

    filtered_movies = normalized_movies.filter(normalized_movies.genre.isin(args.genres)) \
        .filter(normalized_movies.title.rlike(args.regexp)) \
        .filter(normalized_movies.year.between(args.year_from, args.year_to))

    movies_with_rating_df = ratings_df.join(filtered_movies, ratings_df.movieId == filtered_movies.movieId, "inner") \
        .groupBy("genre", "title", "year").agg(avg("rating").alias("rate"))

    result_df = movies_with_rating_df.sort(movies_with_rating_df.rate.desc(), movies_with_rating_df.year.desc(),
                                           movies_with_rating_df.title.asc())
    windowDept = Window.partitionBy("genre").orderBy(col("title").desc())
    result_df = result_df.withColumn("row", row_number().over(windowDept))
    result_df = result_df.filter(col("row") <= args.number)
    result_df = result_df.drop("row")

    result_df.write.mode("overwrite").format('csv').option("header", "true").save('data/results')


if __name__ == "__main__":
    args = parse_args()
    main()

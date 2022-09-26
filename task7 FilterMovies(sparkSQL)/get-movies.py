import argparse

from pyspark.sql import SparkSession


def parse_args():
    parser = argparse.ArgumentParser(description='movies filter')
    parser.add_argument('-n', '--number', type=int, help='number of films', default=10)
    parser.add_argument('-g', '--genres', nargs='*', required=False, help='enter the genres you want', default="Drama")
    parser.add_argument('-yf', '--year_from', type=int, help='enter the year from', default=0)
    parser.add_argument('-yt', '--year_to', type=int, help='enter the year to', default=2030)
    parser.add_argument('-rg', '--regexp', type=str, help='name of the ".+name"', default='')

    return parser.parse_args()


def main():
    genres = args.genres
    number = args.number
    year_from = args.year_from
    year_to = args.year_to
    regexp = args.regexp

    spark = SparkSession.builder \
        .master("local") \
        .config("spark.executor.memory", "500mb") \
        .config("spark.sql.catalogImplementation", "hive") \
        .appName("get-movies") \
        .getOrCreate()

    movies_df = spark.read.format("csv").option("header", "true").option("inferSchema", "true").load("data/movies.csv")
    ratings_df = spark.read.format("csv").option("header", "true").option("inferSchema", "true").load(
        "data/ratings.csv")

    movies_df.createOrReplaceTempView('movies')
    ratings_df.createOrReplaceTempView('ratings')

    filtered_movies_with_ratings_df = spark.sql("""
        with
            cte_avg_ratings as (
                select
                    movieId as movie_id,
                    avg(rating) as avg_rating
                from
                    ratings
                group by
                    movieId
            ),

            cte_normalized_movies as (
                select
                    movieId as movie_id,
                    regexp_extract(title, '(.+)[ ]+\\((\\d{4})\\)', 1) as title,
                    regexp_extract(title, '(.+)[ ]+\\((\\d{4})\\)', 2) as year,
                    explode(split(genres, '\\|')) as genre
                from
                    movies
            ),

            cte_cleaned_movies as (
                select
                    movie_id,
                    title,
                    year,
                    genre
                from
                    cte_normalized_movies
                where
                    year is not null
                    and genre <> '(no genres listed)'
                    and ifnull(trim(genre), '') <> ''
            ),

            cte_filtered_movies as (
                select
                    movie_id,
                    title,
                    year,
                    genre
                from
                    cte_cleaned_movies
                where
                     year >= """ + year_from + """
                    and (""" + year_to + """ = 0 or year <= """ + year_to + """)
                    and ('""" + genres + """' = '' or genre rlike('""" + genres + """'))
                    and ('""" + regexp + """' = '' or title rlike('""" + regexp + """'))
                    
            ),

            cte_movies_with_ratings as (
                select
                    m.genre,
                    m.title,
                    m.year,
                    r.avg_rating,
                    row_number() over(
                        partition by
                            m.genre
                        order by
                            r.avg_rating desc,
                            m.year desc,
                            m.title
                    ) as row_number
                from
                    cte_filtered_movies m
                    join cte_avg_ratings r on r.movie_id = m.movie_id
            ),

            cte_top_movies as (
                select
                    genre,
                    title,
                    year,
                    avg_rating,
                    row_number
                from
                    cte_movies_with_ratings
                where
                    number = 0 or row_number <= number
            )

            select
                genre,
                title,
                year,
                rating
            from
                cte_top_movies
            order by
                genre,
                row_number
    """)

    print(filtered_movies_with_ratings_df.collect())


if __name__ == "__main__":
    args = parse_args()
    main()

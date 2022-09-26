import argparse
import csv
import re
from operator import itemgetter


def parse_args():
    parser = argparse.ArgumentParser(description='movies filter')
    parser.add_argument('-n', '--number', type=int, help='number of films')
    parser.add_argument('-g', '--genres', nargs='*', required=False, help='enter the genres you want')
    parser.add_argument('-yf', '--year_from', type=int, help='enter the year from')
    parser.add_argument('-yt', '--year_to', type=int, help='enter the year to')
    parser.add_argument('-rg', '--regexp', type=str, help='name of the ".+name"')

    return parser.parse_args()


def read_input_ratings(file_name):
    try:
        with open(file_name) as file:
            reader = csv.DictReader(file)
            for line in reader:
                yield line
    except FileNotFoundError:
        print('File not found')


def read_input_movies(file_name):
    try:
        with open(file_name, newline='') as csv_file:
            reader = csv.DictReader(csv_file)
            output_list = [row for row in reader]
        return output_list
    except FileNotFoundError:
        print('File not found')
    except MemoryError:
        print('not enough memory')


def normalize_ratings(ratings):
    userId_to_be_deleted = 'userId'
    timestamp_to_be_deleted = 'timestamp'

    if userId_to_be_deleted in ratings:
        del ratings[userId_to_be_deleted]
    if timestamp_to_be_deleted in ratings:
        del ratings[timestamp_to_be_deleted]
    ratings.update(rating=float(ratings['rating']), movieId=int(ratings['movieId']))
    return ratings


def normalize_movies(movies):
    output_list = []
    exp = r'.+\((\d{4})\)$'
    for i in movies:
        cur_title = i['title']
        if re.match(exp, cur_title):
            year = int(re.findall(exp, cur_title)[0])
            new_title = re.sub(r'(\(\d{4}\))', '', cur_title).strip()
            i.update(year=year, title=new_title)
        else:
            i.update(year=-1)
        i.update(genres=i['genres'].split('|'), movieId=int(i['movieId']))
        output_list.append(i)
    return output_list


def sum_ratings_by_movie_id(rating, output_dict_ratings):
    movie_id = rating['movieId']
    rating = rating['rating']
    try:
        if movie_id in output_dict_ratings:
            val = output_dict_ratings[movie_id]
            val.append(rating)
        else:
            output_dict_ratings[movie_id] = [rating]
        return output_dict_ratings
    except MemoryError:
        print('not enough memory')


def average_rating_by_movie_id(output_dict_ratings):
    for k, v in output_dict_ratings.items():
        avg = round(sum(v) / len(v), 1)
        output_dict_ratings[k] = avg


def filter_genres(movies, genres):
    output_list = []
    for i in movies:
        for k in genres:
            if k in i["genres"]:
                output_list.append(i)
                break
    return output_list


def filter_movies_by_year_from(movies, year_from):
    return [r for r in movies if r['year'] >= year_from]


def filter_movies_by_year_to(movies, year_to):
    return [r for r in movies if r['year'] <= year_to]


def filter_movies_by_name(movies, regexp):
    return [r for r in movies if re.match(regexp, r['title'])]


def join_ratings_with_movies_by_movie_id(movies, ratings):
    output_list = []
    for i in movies:
        if i['movieId'] in ratings:
            i.update(rating=ratings[i['movieId']])
            output_list.append(i)
    return output_list


def sort(movies):
    movies = sorted(movies, key=itemgetter('rating', 'year', 'title'))
    movies = sorted(movies, key=lambda k: (-k['rating'], -k['year'], k['title']))
    return movies


def normalize_genres(movies):
    output_list = []
    for i in movies:
        for g in i['genres']:
            i.update(genres=g)
            tmp_dict = {}
            tmp_dict.update(i)
            output_list.append(tmp_dict)
    return output_list


def number_of_films_by_genres(movies):
    output_list = []
    if args.genres is None:
        movies = sorted(movies, key=lambda k: (k['genres']))
        genres_list = []
        count = 0
        for i in movies:
            if count < args.number and i['genres'] not in genres_list:
                output_list.append(i)
                count += 1
            elif count >= args.number:
                genres_list.append(i['genres'])
                count = 0
    else:
        for g in args.genres:
            count = 0
            for i in movies:
                if i['genres'] == g and count < args.number:
                    output_list.append(i)
                    count += 1
    return output_list


def print_movies_result(movies):
    print('genre;title;year;rating')
    if args.regexp is None \
            and args.year_from is None \
            and args.year_to is None \
            and args.genres is None \
            and args.number is None:
        movies = sorted(movies, key=lambda k: (k['genres'], -k['rating']))
    elif args.number is None and args.genres is not None:
        movies = sorted(movies, key=lambda k: (k['genres'], -k['rating']))
        output_list = []
        for i in movies:
            for g in args.genres:
                if i['genres'] == g:
                    output_list.append(i)
                    movies = output_list

    for i in movies:
        if i['year'] == -1 or i['genres'] == '(no genres listed)':
            continue
        print(i['genres'] + ';', i['title'] + ';', str(i['year']) + ';', i['rating'])


def main():
    movies_path = 'data/ml-latest-small/movies.csv'
    ratings_path = 'data/ml-latest-small/ratings.csv'
    movies = read_input_movies(movies_path)
    movies = normalize_movies(movies)
    ratings_dict = {}

    for rating in read_input_ratings(ratings_path):
        normalize_ratings(rating)
        sum_ratings_by_movie_id(rating, ratings_dict)
    average_rating_by_movie_id(ratings_dict)

    if args.regexp is not None:
        movies = filter_movies_by_name(movies, args.regexp)

    if args.year_from is not None:
        movies = filter_movies_by_year_from(movies, args.year_from)

    if args.year_to is not None:
        movies = filter_movies_by_year_to(movies, args.year_to)

    if args.genres is not None:
        args.genres = args.genres[0]
        args.genres = list(re.split(r'\|', args.genres))
        movies = filter_genres(movies, args.genres)

    movies = join_ratings_with_movies_by_movie_id(movies, ratings_dict)
    movies = sort(movies)
    movies = normalize_genres(movies)

    if args.number is not None:
        movies = number_of_films_by_genres(movies)

    print_movies_result(movies)


if __name__ == '__main__':
    args = parse_args()
    main()

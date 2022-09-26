import argparse
import csv
import re


def parse_args():
    parser = argparse.ArgumentParser(description='movies filter')
    parser.add_argument('-n', '--number', type=int, help='number of films')
    parser.add_argument('-g', '--genres', nargs='+', required=False, help='enter the genres you want')
    parser.add_argument('-yf', '--year_from', type=int, help='enter the year from')
    parser.add_argument('-yt', '--year_to', type=int, help='enter the year to')
    parser.add_argument('-rg', '--regexp', type=str, help='name of the ".+name"')
    parser.add_argument('-m', '--movies_file', type=str, help='Input moves fail')
    return parser.parse_args()


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


def normalize_movies(movies):
    output_list = []
    exp = r'.+\((\d{4})\)$'
    for i in movies:
        cur_title = i['title']
        if re.match(exp, cur_title):
            year = int(re.findall(exp, cur_title)[0])
            new_title = re.sub(r'(\(\d{4}\))$', '', cur_title).strip()
            i.update(year=year, title=new_title)
        else:
            i.update(year=-1)
        i.update(genres=i['genres'].split('|'), movieId=int(i['movieId']))
        output_list.append(i)

    return output_list


def read_input_file(file_name):
    with open(file_name, newline='') as csv_file:
        reader = csv.DictReader(csv_file)
        output_list = [row for row in reader]
    return output_list


def map(movie_id, movie):
    title, year, genres = movie
    if genres == args.genres:
        for genre in genres:
            yield genre, (title, year)


def main():
    movies = read_input_file(args.movies_file)
    movies = normalize_movies(movies)

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

    for movie in movies:
        for key, value in map(movie['movieId'], (movie['title'], movie['year'], movie['genres'])):
            print('{0}\t{1}'.format(key, value))


if __name__ == '__main__':
    args = parse_args()
    main()

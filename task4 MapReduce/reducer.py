from itertools import groupby
from operator import itemgetter
import sys


def read_mapper_output(file):
    for line in file:
        elements = line.split("\t")
        yield [i.strip() for i in elements]


def main(separator='\t'):
    data = read_mapper_output(sys.stdin)
    for key, group in groupby(data, itemgetter(0)):
        print('{"genre": "%s", "movies": [' % key,
              ','.join(['{"title": %s}' % (i[1]) for i in group]),
              ']}', sep='')


if __name__ == '__main__':
    main()

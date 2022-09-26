import argparse
import csv
import sys
import pathlib

import pandas as pd
import pyarrow.parquet as pq


def parse_args():
    parser = argparse.ArgumentParser(description='converter',
                                     formatter_class=argparse.RawTextHelpFormatter)
    parser.add_argument('-a', '--action', help='What to do '
                                               'with file? csv2parquet, parquet2csv, get_schema \n')
    parser.add_argument('-i', '--input_file', type=str, help='Input dir for file', required=False)
    parser.add_argument('-o', '--output_file', type=str, help='Output dir for file', required=False)

    if len(sys.argv) == 1:
        parser.print_help(sys.stderr)
        sys.exit(1)
    return parser.parse_args()


def convert_csv_to_parquet(input_file, output_file):
    try:
        df = pd.read_csv(input_file)
        df.to_parquet(output_file, index=False)
    except MemoryError:
        print('not enough memory')
    except TypeError:
        print('wrong file format')
    except FileNotFoundError:
        print('File not found')


def convert_parquet_to_csv(input_file, output_file):
    try:
        df = pd.read_parquet(input_file)
        df.to_csv(output_file, index=False)
    except MemoryError:
        print('not enough memory')
    except TypeError:
        print('wrong file format')
    except FileNotFoundError:
        print('File not found')


def get_schema(input_file, input_file_format):
    try:
        if input_file_format == '.parquet':
            pfile = pq.read_table(input_file)
            print("Column names: {}".format(pfile.column_names))
            print("Schema: {}".format(pfile.schema))

        elif input_file_format == '.csv':
            with open(input_file, encoding='utf-8') as r_file:
                file_reader = csv.reader(r_file, delimiter=",")
                count = 0
                for row in file_reader:
                    if count == 0:
                        print(f'Файл содержит столбцы: {", ".join(row)}')
                    count += 1
                print(f'Всего в файле {count} строк.')
                print(pd.read_csv(input_file).dtypes)
        else:
            print('%s - wrong file format csv or parquet only' % input_file_format)
    except TypeError:
        print('wrong file format')
    except FileNotFoundError:
        print('File not found')


def main(args):
    input_file_format = pathlib.Path(args.input_file).suffix
    action = args.action

    if action == 'csv2parquet':
        convert_csv_to_parquet(args.input_file, args.output_file)

    elif action == 'parquet2csv':
        convert_parquet_to_csv(args.input_file, args.output_file)

    elif action == 'get_schema':
        get_schema(args.input_file, input_file_format)

    else:
        print('%s action is not supported' % action)


if __name__ == '__main__':
    args = parse_args()
    main(args)


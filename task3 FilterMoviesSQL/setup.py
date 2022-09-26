import mysql.connector


def execute_query(query_path):
    connection = mysql.connector.connect(
        user='root',
        password='root',
        host='localhost',
        allow_local_infile=True
    )
    sql_file = open(query_path)
    sql_as_string = sql_file.read()
    cursor = connection.cursor()
    for result in cursor.execute(sql_as_string, multi=True):
        pass

    connection.commit()
    cursor.close()
    connection.close()
    connection.disconnect()


def main():
    create_moviesDb_path = 'SQL/DDL/databases/create_moviesDb.sql'
    create_stg_moviesDb_path = 'SQL/DDL/databases/create_stg_moviesDb.sql'
    create_top_movies_proc_path = 'SQL/DDL/tables/create_top_movies.sql'
    create_table_raw_movies_path = 'SQL/DDL/tables/create_raw_movies.sql'
    create_table_raw_ratings_path = 'SQL/DDL/tables/create_raw_ratings.sql'
    create_table_movies_path = 'SQL/DDL/tables/create_movies.sql'

    load_movies_path = 'SQL/DML/load/load_movies.sql'
    load_ratings_path = 'SQL/DML/load/load_ratings.sql'

    normalize_movies_path = 'SQL/DML/views/normalize_movies.sql'
    normalize_ratings_path = 'SQL/DML/views/normalize_ratings.sql'

    create_response_path = 'SQL/DDL/tables/create_responce.sql'

    create_loading_top_movies_path = 'SQL/DDL/procedures/create_loading_top_movies.sql'
    create_filter_movies_path = 'SQL/DDL/procedures/create_filter_movies.sql'
    create_split_genres_path = 'SQL/DDL/procedures/create_split_genres.sql'

    call_loading_top_movies_path = 'SQL/DML/procedures/loading_top_movies.sql'

    split_genres_path = 'SQL/DML/procedures/split_genres.sql'

    execute_query(create_moviesDb_path)
    execute_query(create_stg_moviesDb_path)
    execute_query(create_table_raw_movies_path)
    execute_query(create_table_raw_ratings_path)
    execute_query(create_table_movies_path)
    execute_query(create_top_movies_proc_path)

    execute_query(load_movies_path)
    execute_query(load_ratings_path)

    execute_query(normalize_movies_path)
    execute_query(normalize_ratings_path)
    execute_query(create_response_path)
    execute_query(create_loading_top_movies_path)
    execute_query(create_filter_movies_path)
    execute_query(create_split_genres_path)
    execute_query(call_loading_top_movies_path)
    execute_query(split_genres_path)


if __name__ == '__main__':
    main()

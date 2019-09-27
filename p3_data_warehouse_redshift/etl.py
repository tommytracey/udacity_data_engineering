import configparser
import psycopg2
from sql_queries import copy_table_queries, insert_table_queries


def load_staging_tables(cur, conn):
    print("Loading staging tables...")
    for query in copy_table_queries:
        print("Executing query '{}'...".format(query))
        cur.execute(query)
        conn.commit()
    print("--> Staging tables loaded")

def insert_tables(cur, conn):
    print("Loading dimension and fact tables...")
    for query in insert_table_queries:
        print("Executing query '{}'...".format(query))
        cur.execute(query)
        conn.commit()
    print("--> Fact and dimension tables loaded")


def main():
    print("Parsing config file...")
    config = configparser.ConfigParser()
    config.read('dwh.cfg')

    print("Connecting to cluster...")
    conn = psycopg2.connect("host={} dbname={} user={} password={} port={}".format(*config['CLUSTER'].values()))
    cur = conn.cursor()

    load_staging_tables(cur, conn)
    insert_tables(cur, conn)

    conn.close()


if __name__ == "__main__":
    main()

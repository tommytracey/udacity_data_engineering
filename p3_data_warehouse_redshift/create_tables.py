import configparser
import psycopg2
from sql_queries import create_table_queries, drop_table_queries


def drop_tables(cur, conn):
    for query in drop_table_queries:
        cur.execute(query)
        conn.commit()
    print("Existing tables dropped...")


def create_tables(cur, conn):
    print("Creating new tables...")
    for query in create_table_queries:
        # print("Executing query '{}'...".format(query))
        cur.execute(query)
        conn.commit()
    print("New tables created")

def verify_tables(cur, conn):
    '''Prints all tables and columns created in Redshift'''

    cur.execute("""
        SELECT *
        FROM pg_table_def
        WHERE schemaname ='public';
    """)
    rows = cur.fetchall()
    conn.commit()
    print("\nschemaname | tablename | column | type | encoding | distkey | sortkey | notnull\n")
    for row in rows:
        print(row)


def main():
    config = configparser.ConfigParser()
    config.read('dwh.cfg')

    print('Connecting to the cluster...')
    conn = psycopg2.connect("host={} dbname={} user={} password={} port={}".format(*config['CLUSTER'].values()))
    cur = conn.cursor()
    print('Connected to the cluster')

    drop_tables(cur, conn)
    create_tables(cur, conn)
    # verify_tables(cur, conn)

    conn.close()


if __name__ == "__main__":
    main()

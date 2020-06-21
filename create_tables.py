import configparser
import psycopg2
from sql_queries import create_table_queries, drop_table_queries


def drop_tables(cur, conn):
    """
    Delete all tables, if they are exist.
    
    deleting all pre-exist tables including staging ,fact and dims tables .
    
    Args:
    As DB is PostgreSQL, both arguments are for connection and execution in Postgre.
    
    Returns:
    Clean space for re-run other queries.
    
    """
    for query in drop_table_queries:
        cur.execute(query)
        conn.commit()
    print('All tables dropped.')


def create_tables(cur, conn): 
    """
    Create satging, fact and dims tables.
    Creating staging, fact and dims tables based on snowflake schema on sql_queries and fill them from staging tables.
    
    Args:
    As tables create in PostgreSQL, both arguments are related to connection to and execution in PostgreSql.
    
    Returns:
    Creating tables.
    
    """
    for query in create_table_queries:
        cur.execute(query)
        conn.commit()
    
    print('Tables created.')


def main():
    """
    Parsing database setup data, connect to database, 
    execute dropping and creating tables functions and finally close connection
    """
    
    config = configparser.ConfigParser()
    config.read('dwh.cfg')

    conn = psycopg2.connect("host={} dbname={} user={} password={} port={}".format(*config['CLUSTER'].values()))
    cur = conn.cursor()

    drop_tables(cur, conn)
    create_tables(cur, conn)

    conn.close()


if __name__ == "__main__":
    main()
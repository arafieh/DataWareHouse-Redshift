import configparser
import psycopg2
from sql_queries import copy_table_queries, insert_table_queries


def load_staging_tables(cur, conn):
    """
    Load data to staging tables:
    
    Copying raw data from predifine destination on S3 to the tables which declared on the sql_queries script.
    
    Args:
    As tables design and creat in PostgreSQL, 
    
    """
    for query in copy_table_queries:
        cur.execute(query)
        conn.commit()


def insert_tables(cur, conn):
    """
    Select and Transform data from staging tables to fact and dimensional tables.
    
    Args:
    As woking with PostgreSQL, both arguments are related to connection and execution query command.
    
    Returns:
    Filled fact and dims tables with satging tables data.
    
    """
    
    for query in insert_table_queries:
        cur.execute(query)
        conn.commit()
    
    print('Fact and dimensional tables filled.')


def main():
    """
    Extract songs metadata and user activity data from S3, 
    Transform them to staging tables, and Load into fact and dimensional tables.
    """
    
    config = configparser.ConfigParser()
    config.read('dwh.cfg')

    conn = psycopg2.connect("host={} dbname={} user={} password={} port={}".format(*config['CLUSTER'].values()))
    cur = conn.cursor()
    
    load_staging_tables(cur, conn)
    insert_tables(cur, conn)

    conn.close()


if __name__ == "__main__":
    main()
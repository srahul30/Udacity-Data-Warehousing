# Import Libraries for config setting, Creating Tables and calling List of Create and Drop Tables
import configparser
import psycopg2
from sql_queries import create_table_queries, drop_table_queries

def drop_tables(cur, conn): 
    """
    Summary - Function to Drop all tables
    
    Parameters - 
    cur - Cursor having the list of all table name which needs to be dropped
    conn - Connection to the Cluster
    
    Return - None
    """
    for query in drop_table_queries:
        cur.execute(query)
        conn.commit()


def create_tables(cur, conn):
    """
    Summary - Function to Create all tables
    
    Parameters - 
    cur - Cursor having the list of all table name which needs to be Created
    conn - Connection to the Cluster
    
    Return - None
    """
    for query in create_table_queries:
        cur.execute(query)
        conn.commit()


def main():
    """
    Summary - Main Call for Creating and Dropping the Staging tables and Star Schema tables 
    
    Parameters - None
    
    Return - None
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
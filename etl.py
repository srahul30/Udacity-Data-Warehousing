# Import Libraries for config setting, connecting Tables and calling List of Load and Insert Tables
import configparser
import psycopg2
from sql_queries import copy_table_queries, insert_table_queries

def load_staging_tables(cur, conn):
    """
    Summary - Function to Load staging tables
    
    Parameters - 
    cur - Cursor having the list of all table names which needs to be Loaded
    conn - Connection to the Cluster
    
    Return - None
    """
    for query in copy_table_queries:
        cur.execute(query)
        conn.commit()

def insert_tables(cur, conn):
    """
    Summary - Function to Insert into Dimensional Tables
    
    Parameters - 
    cur - Cursor having the list of all table names which needs to be Inserted
    conn - Connection to the Cluster
    
    Return - None
    """
    for query in insert_table_queries:
        cur.execute(query)
        conn.commit()

def main():
    """
    Summary - Main Call for Loading and Inserting the Staging tables and Star Schema tables
    
    Parameters - None
    
    Return - None
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
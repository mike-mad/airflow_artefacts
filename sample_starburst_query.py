import os
import sys
import trino
from trino import dbapi
from datetime import datetime
import argparse

parser=argparse.ArgParser(description='check data availability for a specific date and runs the query if data is available')
parser.add_argument('--check_date', type=str, help='Availability date in source table')

args = parser.arg_parse()

def run_insert(check_date):
  conn=dbapi.connect(
    host='starburst.host',
    port=443,
    http_scheme='https',
    auth.trion.auth.BasicAuthentication(os.environ['my_sb_username'],os.environ['my_sb_password']),
    catalog='system',
    schema='runtime',
    verify='False
  )
  cursor = conn.cursor()

  try:
    cursor.execute(f"SELECT MAX(snapshot_date) FROM starburst_catalogue.starburst_schema.starburst_table")
    max_update_date = cursor.fetchone()[0]
  
    if max_update_date >= check_date:
      print(f"Data is available in source, continuing")
    else:
      print(f"Data is not available, exiting")
      cursor.close()
      conn.close()
      sys.exit
  
    step1_query = f"""
      DROP TABLE IF EXISTS starburst_catalogue.starburst_schema.starburst_subset_table")
    """
    cursor.execute(step1_query)
    print("Query executed successfully. Query = ", step1_query)
    
    step2_query = f"""
      CREATE TABLE starburst_catalogue.starburst_schema.starburst_subset_table
      AS
      (
        SELECT * FROM starburst_catalogue.starburst_schema.starburst_table
        WHERE snapshot_date = date'{check_date}'
      ) WITH DATA
    """
    cursor.execute(step2_query)
    print("Query executed successfully. Query = ", step2_query)
  
  except Exception as e:
    print(f"An error occurred: {e}")
    cursor.close()
    conn.close()
    sys.exit(1)
  
  finally
    cursor.close()
    conn.close()

if __name__ = "__main__":
  check_date = datetime.strptime(args.check_date,'%Y-%m-%dT%H:%M:%S+00.00').date()
  run_insert(check_date)
 

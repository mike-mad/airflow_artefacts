import os
import teradatasql

def run_td_query():

  try:
    connect = teradatasql.connect(
      host="TDSERVER.UNIX.CO_NAME",
      user=os.environ['my_username'],
      password=os.environ['my_password'],
      logmech='TDNEGO'
    )

    cursor = connectoin.cursor()

    query = f""" 
      INSERT INTO target_db_schema.target_db_table
      SELECT * FROM source_db_schema.source_db_table
    """
    
    cursor.execute(query)
    
    result = cursor.fetchall()

  except Exception as e:
    print(f"An error occurred: {e}")

  finally:
    cursor.close()
    connection.close()
    return result

if __name__ == "__main__":
  run_teradata_query()

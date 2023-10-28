import io
import csv
import json
import re
from google.cloud import storage
 
def read_csv_header(bucket_name, csv_file_name):
  storage_client = storage.Client()
  bucket = storage_client.bucket(bucket_name)
  blob = bucket.blob(csv_file_name)
  
  stream = blob.open("r")
  first_line = stream.readline()
  x=re.sub(r'"','',first_line)
  header_row =x.strip().split(",")

  schema_fields = []
  for column_name in header_row:
    final_column = re.sub(r"[^\w\d]+", "_", column_name)
    #print(final_column)
    if final_column == 'date_time':
        schema_field = {"name": final_column,"type": "TIMESTAMP"}
    else:
        schema_field = {"name": final_column,"type": "STRING"}
  
    schema_fields.append(schema_field)
  
  stream.close()
  
  return json.dumps(schema_fields)
  
if __name__ == "__main__":
  schema= read_csv_header('bucket01', 'folder01/new_115.csv')
  print(schema)

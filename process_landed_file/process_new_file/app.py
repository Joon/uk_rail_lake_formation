import boto3
import json
import urllib
import awswrangler as wr
import pandas as pd
import numpy as np
from datetime import datetime

print('Loading function')

def load_file_into_frame(bucket, key):
    # Ensure that the correct region is used
    boto3.setup_default_session(region_name="us-east-1")
    s3 = boto3.client('s3')
    obj = s3.get_object(Bucket=bucket, Key=key)
    content = obj['Body'].read().decode('utf8')
    filedata = json.loads(content)
    print('loading file ' + key)
    rows = []
    for row in filedata:
        row_to_add = row['body']
        row_to_add['msg_type'] = row['header']['msg_type']
        rows.append(row_to_add)
    
    frame = pd.DataFrame(rows)        
    frame['segment_timestamp'] = np.nan
    if 'actual_timestamp' in frame.columns:
        frame['segment_timestamp'] = frame['segment_timestamp'].fillna(frame['actual_timestamp'])
    if 'creation_timestamp' in frame.columns:
        frame['segment_timestamp'] = frame['segment_timestamp'].fillna(frame['creation_timestamp'])
    if 'dep_timestamp' in frame.columns:
        frame['segment_timestamp'] = frame['segment_timestamp'].fillna(frame['dep_timestamp'])
    if 'event_timestamp' in frame.columns:
        frame['segment_timestamp'] = frame['segment_timestamp'].fillna(frame['event_timestamp'])
                
    frame['segment_date'] = frame['segment_timestamp'].apply(lambda x: datetime.fromtimestamp(int(x) / 1000.0).date())
    frame = frame.drop(columns=['segment_timestamp'])
    return frame

def save_frame_to_table(frame, db, table):
     wr.s3.to_parquet(
        df=frame,
        dataset=True,
        mode="append",
        database=db,
        table=table,
        catalog_versioning=True,  # Optional
        #schema_evolution=True,
        partition_cols=['segment_date']
    )
    
def move_file_to_processed(bucket, key):
    s3 = boto3.client('s3')
    copy_source = {
        'Bucket': bucket,
        'Key': key
    }
    s3.copy(copy_source, bucket, 'processed/' + key)
    s3.delete_object(Bucket=bucket, Key=key)
    
# --------------- Main handler ------------------

def lambda_handler(event, context):
    '''Demonstrates S3 trigger that uses
    Rekognition APIs to detect faces, labels and index faces in S3 Object.
    '''
    #print("Received event: " + json.dumps(event, indent=2))

    # Get the object from the event
    bucket = event['Records'][0]['s3']['bucket']['name']
    key = urllib.parse.unquote_plus(event['Records'][0]['s3']['object']['key'])
    try:
        frame = load_file_into_frame(bucket, key)
        save_frame_to_table(frame, 'train_bronze', 'train_movements_governed')
        move_file_to_processed(bucket, key)
    except Exception as e:
        print(e)
        print("Error processing object {} from bucket {}. ".format(key, bucket))
        raise e

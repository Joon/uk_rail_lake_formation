import sys
import awswrangler as wr
from datetime import datetime

df = wr.athena.read_sql_query('SELECT max(canx_timestamp) AS max_date FROM "AwsDataCatalog"."train_silver"."journey";', database="train_silver")
if df.count()[0] == 0:
    max_journey_timestamp = 0
else:
    max_journey_timestamp = df.head()['max_date'][0]

print('=======================')
print('  Processing Journeys')
print('=======================')
print('Reading latest journeys as of ' + str(datetime.fromtimestamp(max_journey_timestamp / 1000.0)))

journey_query = """SELECT train_id, loc_stanox as stanox, CAST(canx_timestamp as bigint) AS canx_timestamp, segment_date
FROM "AwsDataCatalog"."train_bronze"."train_movements_governed"
WHERE canx_type = 'AT ORIGIN'
AND cast(canx_timestamp AS bigint) > {}""".format(max_journey_timestamp)

journey_df = wr.athena.read_sql_query(journey_query, database="train_bronze")

journey_df["created"] = datetime.now()
journey_df["date"] = journey_df['canx_timestamp'].apply(lambda x: datetime.fromtimestamp(int(x) / 1000.0).date())
journey_df["start_timestamp"] = journey_df['canx_timestamp'].apply(lambda x: datetime.fromtimestamp(int(x) / 1000.0))

print('Saving ' + str(journey_df.count()[0])  + ' rows to the journey table')
wr.s3.to_parquet(
    df=journey_df,
    dataset=True,
    mode="append",
    database="train_silver",
    table="journey",
    partition_cols=['segment_date']
)

print('=======================')
print('  Processing Stops')
print('=======================')

df = wr.athena.read_sql_query('SELECT max(depart_timestamp) AS max_date FROM "AwsDataCatalog"."train_silver"."stop";', database="train_silver")
if df.count()[0] == 0:
    max_stop_timestamp = 0
else:
    max_stop_timestamp = df.head()['max_date'][0]

query = """SELECT a.train_id, a.loc_stanox as stanox_code, 
       a.planned_timestamp as planned_arrival_int, 
       a.actual_timestamp as actual_arrival_int,
       a.variation_status as time_status,
       b.planned_timestamp as planned_departure_int, 
       b.actual_timestamp as actual_departure_int,
       CASE a.planned_event_type 
           WHEN 'DESTINATION' THEN True
           ELSE False
       END AS is_terminus
FROM "train_bronze"."train_movements_governed" as a
     LEFT JOIN "train_bronze"."train_movements_governed" as b
     ON  a.train_id = b.train_id
     AND a.loc_stanox = b.loc_stanox
     AND a.event_type = 'ARRIVAL'  
     AND b.event_type = 'DEPARTURE'  
WHERE a.event_type = 'ARRIVAL'  
AND (b.event_type = 'DEPARTURE' or a.planned_event_type = 'DESTINATION') 
AND (CAST(a.actual_timestamp as bigint) > {} OR CAST(b.actual_timestamp as bigint) > {})""".format(max_stop_timestamp, max_stop_timestamp)

stop_df = wr.athena.read_sql_query(query, database="train_bronze")
stop_df['planned_departure_int'] = stop_df['planned_departure_int'].fillna('0')
stop_df['actual_departure_int'] = stop_df['actual_departure_int'].fillna('0')

def calc_date(planned, actual):
    if planned == 0:
        return datetime.fromtimestamp(int(actual) / 1000.0)
    return datetime.fromtimestamp(int(planned) / 1000.0)

def value_with_fallback(value, fallback):
    if value == 0:
        return fallback
    return value

def calc_date_none(actual):
    if actual == 0:
        return None
    return datetime.fromtimestamp(int(actual) / 1000.0)

def calc_delay(expected, actual):
    if expected == 0 or actual == 0:
        return 0
    return (actual - expected) / 1000.0

stop_df["arrival"] = stop_df['actual_arrival_int'].apply(lambda x: datetime.fromtimestamp(int(x) / 1000.0))
stop_df["planned_arrival_int"].replace({"": "0"}, inplace=True)
stop_df["delay_seconds"] = stop_df.apply(lambda x: calc_delay(int(x['planned_arrival_int']), int(x['actual_arrival_int'])), axis=1)
stop_df["planned_arrival"] = stop_df.apply(lambda x: calc_date(int(x['planned_arrival_int']), int(x['actual_arrival_int'])), axis=1)

stop_df["planned_departure_int"].replace({"": "0"}, inplace=True)
stop_df["departure"] = stop_df['actual_departure_int'].apply(lambda x: calc_date_none(x))
stop_df["planned_departure"] = stop_df['planned_departure_int'].apply(lambda x: calc_date_none(x))
stop_df["depart_timestamp"] = stop_df.apply(lambda x: value_with_fallback(int(x['actual_departure_int']), int(x['actual_arrival_int'])), axis=1)

stop_df.drop(['planned_arrival_int', 'actual_arrival_int', 'planned_departure_int', 'actual_departure_int'], inplace=True, axis=1)

print('Saving ' + str(stop_df.count()[0])  + ' rows to the stop table')
wr.s3.to_parquet(
    df=stop_df,
    dataset=True,
    mode="append",
    database="train_silver",
    table="stop"
)
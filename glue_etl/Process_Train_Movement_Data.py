import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
from pyspark.sql.functions import *
from awsglue.dynamicframe import DynamicFrame

## @params: [JOB_NAME]
args = getResolvedOptions(sys.argv, ['JOB_NAME'])

sc = SparkContext()
glueContext = GlueContext(sc)
logger = glueContext.get_logger()
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args['JOB_NAME'], args)

logger.info('About to start transaction')

tx_id = glueContext.start_transaction(False)

logger.info('About to read the bronze table')
bronze_table = glueContext.create_dynamic_frame.from_catalog(database = "train_bronze", table_name = "train_movements_governed", 
    transformation_ctx = "bronze", additional_options = { "transactionId": tx_id })
logger.info('About to save the bronze table to a view')
bronze_table.toDF().registerTempTable("train_movements")

logger.info('About to access the silver journey table')
silver_journeys = glueContext.create_dynamic_frame.from_catalog(database = "train_silver", table_name = "journey", 
    transformation_ctx = "silver_journey", additional_options = { "transactionId": tx_id })
silver_journeys.toDF().registerTempTable("journeys")

logger.info('About to access the silver stops table')
silver_stops = glueContext.create_dynamic_frame.from_catalog(database = "train_silver", table_name = "stop", transformation_ctx = "silver_stop", additional_options = { "transactionId": tx_id })
silver_stops.toDF().registerTempTable("stops")

logger.info('reading max timestamp from journeys')
max_timestamp_df = spark.sql("""
SELECT max(canx_timestamp) as max_timestamp
FROM journeys""")
max_journey_timestamp = max_timestamp_df.head().getLong(0)

logger.info('Max journey timestamp: ' + str(max_journey_timestamp))

logger.info('reading max timestamp from stops')
max_stop_timestamp_df = spark.sql("""
SELECT max(depart_timestamp) as max_timestamp
FROM stops""")
max_stop_timestamp = max_stop_timestamp_df.head().getLong(0)

logger.info('Max stop timestamp: ' + str(max_stop_timestamp))

logger.info('reading new journeys')
journey_df = spark.sql("""
SELECT train_id, loc_stanox, canx_timestamp 
FROM train_movements
WHERE canx_type = 'AT ORIGIN'
AND cast(canx_timestamp AS bigint) > {}""".format(max_journey_timestamp))

try:
    save_journey_frame = DynamicFrame.fromDF(journey_df, glueContext, "journey_df")
    logger.info('Saving ' + str(save_journey_frame.count()) + 'new journeys')
    journeySink = glueContext.write_dynamic_frame.from_catalog(frame = save_journey_frame, database = "train_silver", table_name = "journey", 
        additional_options = { "transactionId": tx_id })
    logger.info('Committing transaction')
    glueContext.commit_transaction(tx_id)
    logger.info('Transaction committed')
except Exception:
    glueContext.cancel_transaction(tx_id)
    raise
logger.info('Committing the job')
job.commit()
from pyspark.sql import SparkSession
from pyspark.sql.functions import *

from datetime import datetime

from common.logger import log_job, get_logger
from common.utils import get_batch_id
from config.customer_config import *

logger = get_logger(__name__)


def customer_bronze(batch_id, spark=None):

    should_stop_spark = False
    if spark is None:
        spark = (
            SparkSession.builder
            .appName("Customer Bronze Load")
            .config("spark.sql.artifact.dir", "/tmp/artifacts")  
            .getOrCreate()
        )
        should_stop_spark = True

    start_time = datetime.now()

    status = "SUCCESS"

    input_count = 0
    accepted_count = 0
    rejected_count = 0

    try:

        df = spark.read.csv(

            LANDING_PATH,

            header=True

        )

        input_count = df.count()

        accepted_count = input_count


        audit_df = df.withColumn(

            "source_file",

            input_file_name()

        ).withColumn(

            "load_timestamp",

            current_timestamp()

        ).withColumn(

            "batch_id",

            lit(batch_id)

        )


        audit_df.write.mode(

            "overwrite"

        ).parquet(

            BRONZE_PATH

        )


        logger.info("Bronze Load Completed")

    except Exception as e:

        status = "FAILED"

        logger.error("Failed to load Bronze layer.")


    finally:

        end_time = datetime.now()


        log_job(

            job_name=f"{TABLE_NAME}_bronze",

            batch_id=batch_id,

            input_count=input_count,

            accepted_count=accepted_count,

            rejected_count=rejected_count,

            status=status,

            start_time=start_time,

            end_time=end_time

        )


        if should_stop_spark:
            spark.stop()


if __name__ == "__main__":

    customer_bronze(

        get_batch_id()

    )
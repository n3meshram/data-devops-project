from pyspark.sql import SparkSession
from pyspark.sql.functions import *

from datetime import datetime

from common.logger import log_job, get_logger
from common.utils import get_batch_id

from config.customer_config import *

logger = get_logger(__name__)


def customer_mart(batch_id, spark=None):

    should_stop_spark = False
    if spark is None:
        spark = SparkSession.builder \
            .appName("Customer Mart") \
            .getOrCreate()
        should_stop_spark = True


    start_time = datetime.now()


    hub_df = spark.read.parquet(

        HUB_PATH

    )


    current_df = hub_df.filter(

        col("is_current") == "Y"

    )


    mart_df = current_df.withColumn(

        "email_domain",

        split(

            col("email"),

            "@"

        )[1]

    )


    mart_df = mart_df.withColumn(

        "full_name",

        concat_ws(

            " ",

            col("first_name"),

            col("last_name")

        )

    )


    mart_df = mart_df.withColumn(

        "phone_status",

        when(

            col("phone").isNull(),

            "MISSING"

        ).otherwise(

            "AVAILABLE"

        )

    )


    mart_df = mart_df.select(

        "customer_id",

        "full_name",

        "email",

        "email_domain",

        "phone",

        "phone_status",

        "effective_date"

    )


    mart_df.write.mode(

        "overwrite"

    ).parquet(

        MART_TMP_PATH

    )


    end_time = datetime.now()

    mart_count = mart_df.count()

    log_job(

        job_name=f"{TABLE_NAME}_mart",

        batch_id=batch_id,

        input_count=hub_df.count(),

        accepted_count=mart_count,

        rejected_count=0,

        status="SUCCESS",

        start_time=start_time,

        end_time=end_time

    )

    logger.info("Mart Load Completed")
    logger.info(f"Current Records : {mart_count}")


    if should_stop_spark:
        spark.stop()



if __name__ == "__main__":

    customer_mart(

        get_batch_id()

    )
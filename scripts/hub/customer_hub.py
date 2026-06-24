from pyspark.sql import SparkSession
from pyspark.sql.functions import *

from datetime import datetime

from common.logger import log_job
from common.utils import get_batch_id

from config.customer_config import *


def customer_hub(batch_id):

    spark = SparkSession.builder \
        .appName("Customer Hub Load") \
        .getOrCreate()

    start_time = datetime.now()

    bronze_df = spark.read.parquet(

        BRONZE_PATH

    )

    trim_df = bronze_df.select(

        trim(col("customer_id")).alias("customer_id"),

        trim(col("first_name")).alias("first_name"),

        trim(col("last_name")).alias("last_name"),

        trim(col("email")).alias("email"),

        trim(col("phone")).alias("phone"),

        trim(col("load_date")).alias("load_date"),

        "source_file",

        "load_timestamp",

        "batch_id"

    )

    dedup_df = trim_df.dropDuplicates()

    accepted_df = dedup_df.filter(

        col("customer_id").isNotNull()

        &

        col("first_name").isNotNull()

        &

        col("email").isNotNull()

    )

    rejected_df = dedup_df.filter(

        col("customer_id").isNull()

        |

        col("first_name").isNull()

        |

        col("email").isNull()

    )

    rejected_df = rejected_df.withColumn(

        "reject_reason",

        when(

            col("first_name").isNull(),

            "NAME_MISSING"

        )

        .when(

            col("email").isNull(),

            "EMAIL_MISSING"

        )

    )

    accepted_df = accepted_df.withColumn(

        "effective_date",

        current_date()

    ).withColumn(

        "expiry_date",

        to_date(

            lit("9999-12-31")

        )

    ).withColumn(

        "is_current",

        lit("Y")

    )

    accepted_df.write.mode(

        "overwrite"

    ).parquet(

        HUB_PATH

    )

    rejected_df.write.mode(

        "overwrite"

    ).parquet(

        REJECT_PATH

    )

    print("Hub Load Completed")

    print(

        "Accepted :",

        accepted_df.count()

    )

    print(

        "Rejected :",

        rejected_df.count()

    )

    end_time = datetime.now()

    log_job(

        job_name=f"{TABLE_NAME}_hub",

        batch_id=batch_id,

        input_count=bronze_df.count(),

        accepted_count=accepted_df.count(),

        rejected_count=rejected_df.count(),

        status="SUCCESS",

        start_time=start_time,

        end_time=end_time

    )

    spark.stop()


if __name__ == "__main__":

    customer_hub(

        get_batch_id()

    )
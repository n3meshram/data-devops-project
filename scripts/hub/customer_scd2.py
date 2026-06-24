from pyspark.sql import SparkSession
from pyspark.sql.functions import *

from datetime import datetime

from common.logger import log_job
from common.utils import get_batch_id

from config.customer_config import *


def customer_scd2(batch_id):

    spark = SparkSession.builder \
        .appName("Customer SCD2") \
        .getOrCreate()

    start_time = datetime.now()

    hub_df = spark.read.parquet(

        HUB_PATH

    )

    day2_df = spark.read.option(

        "header",

        True

    ).csv(

        DAY2_PATH

    )

    current_df = hub_df.filter(

        col("is_current") == "Y"

    )

    joined_df = current_df.alias(

        "old"

    ).join(

        day2_df.alias(

            "new"

        ),

        "customer_id",

        "inner"

    )

    changed_df = joined_df.filter(

        col("old.phone")

        !=

        col("new.phone")

    )

    expired_df = changed_df.select(

        col("old.customer_id"),

        col("old.first_name"),

        col("old.last_name"),

        col("old.email"),

        col("old.phone"),

        col("old.load_date"),

        col("old.source_file"),

        col("old.load_timestamp"),

        col("old.batch_id"),

        col("old.effective_date")

    ).withColumn(

        "expiry_date",

        current_date()

    ).withColumn(

        "is_current",

        lit("N")

    )

    new_version_df = changed_df.select(

        col("new.customer_id"),

        col("new.first_name"),

        col("new.last_name"),

        col("new.email"),

        col("new.phone"),

        col("new.load_date")

    ).withColumn(

        "source_file",

        lit("customer_day2.csv")

    ).withColumn(

        "load_timestamp",

        current_timestamp()

    ).withColumn(

        "batch_id",

        lit(batch_id)

    ).withColumn(

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

    new_customer_df = day2_df.join(

        hub_df,

        "customer_id",

        "left_anti"

    ).withColumn(

        "source_file",

        lit("customer_day2.csv")

    ).withColumn(

        "load_timestamp",

        current_timestamp()

    ).withColumn(

        "batch_id",

        lit(batch_id)

    ).withColumn(

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

    unchanged_df = current_df.join(

        changed_df.select(

            "customer_id"

        ),

        "customer_id",

        "left_anti"

    )

    final_df = (

        unchanged_df

        .unionByName(

            expired_df

        )

        .unionByName(

            new_version_df

        )

        .unionByName(

            new_customer_df

        )

    )

    final_df.cache()

    final_count = final_df.count()

    final_df.write.mode(

        "overwrite"

    ).parquet(

        HUB_PATH

    )

    end_time = datetime.now()

    log_job(

        job_name=f"{TABLE_NAME}_scd2",

        batch_id=batch_id,

        input_count=day2_df.count(),

        accepted_count=final_count,

        rejected_count=0,

        status="SUCCESS",

        start_time=start_time,

        end_time=end_time

    )

    spark.stop()


if __name__ == "__main__":

    customer_scd2(

        get_batch_id()

    )
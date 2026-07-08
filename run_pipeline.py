import sys

from pyspark.sql import SparkSession

from common.logger import get_logger
from common.utils import get_batch_id

from scripts.bronze.customer_bronze import customer_bronze
from scripts.hub.customer_hub import customer_hub
from scripts.hub.customer_scd2 import customer_scd2
from scripts.mart.customer_mart import customer_mart

logger = get_logger(__name__)

batch_id = get_batch_id()

logger.info("Pipeline Started: %s", batch_id)

spark = (
    SparkSession.builder
    .appName("Customer ETL Pipeline")
    .config("spark.sql.artifact.dir", "/tmp/artifacts")
    .getOrCreate()
)

spark.sparkContext.setLogLevel("WARN")

try:

    customer_bronze(batch_id, spark)

    customer_hub(batch_id, spark)

    customer_scd2(batch_id, spark)

    customer_mart(batch_id, spark)

    logger.info("Pipeline Completed: %s", batch_id)

except Exception:

    logger.exception("Pipeline Failed")

    sys.exit(1)

finally:

    spark.stop()
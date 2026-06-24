from common.utils import get_batch_id

from scripts.bronze.customer_bronze import customer_bronze
from scripts.hub.customer_hub import customer_hub
from scripts.hub.customer_scd2 import customer_scd2
from scripts.mart.customer_mart import customer_mart


batch_id = get_batch_id()

print(f"Pipeline Started : {batch_id}")

customer_bronze(batch_id)

customer_hub(batch_id)

customer_scd2(batch_id)

customer_mart(batch_id)

print(f"Pipeline Completed : {batch_id}")
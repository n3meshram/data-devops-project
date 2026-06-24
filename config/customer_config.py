from common.utils import get_batch_id


TABLE_NAME = "customer"


BUSINESS_KEY = "customer_id"


COMPARE_COLUMNS = [

    "first_name",

    "last_name",

    "email",

    "phone"

]


LANDING_PATH = (

    "data/landing/customer.csv"

)


DAY2_PATH = (

    "data/landing/customer_day2.csv"

)


BRONZE_PATH = (

    "data/bronze/customer"

)


HUB_PATH = (

    "data/hub/customer"

)


HUB_TMP_PATH = (

    "data/hub/customer_tmp"

)


REJECT_PATH = (

    "data/reject/customer"

)


MART_PATH = (

    "data/mart/customer"

)


MART_TMP_PATH = (

    "data/mart/customer_tmp"

)


BATCH_ID = get_batch_id()
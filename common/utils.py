from datetime import datetime


def get_batch_id():

    return (

        f"RUN_"

        f"{datetime.now().strftime('%Y%m%d_%H%M%S')}"

    )
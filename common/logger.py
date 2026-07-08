from datetime import datetime
import csv
import os
import logging
import sys


def log_job(job_name,
            batch_id,
            input_count,
            accepted_count,
            rejected_count,
            status,
            start_time,
            end_time):

    logfile = "logs/job_log.csv"

    file_exists = os.path.exists(logfile)

    with open(logfile,
              mode="a",
              newline="") as file:

        writer = csv.writer(file)

        if not file_exists:
            writer.writerow([
                "job_name",
                "batch_id",
                "input_count",
                "accepted_count",
                "rejected_count",
                "status",
                "start_time",
                "end_time"
            ])

        writer.writerow([

            job_name,

            batch_id,

            input_count,

            accepted_count,

            rejected_count,

            status,

            start_time,

            end_time

        ])


def get_logger(name: str) -> logging.Logger:
    """
    Create and return a configured logger.
    """

    logger = logging.getLogger(name)

    if logger.hasHandlers():
        return logger

    logger.setLevel(logging.INFO)

    formatter = logging.Formatter(
        "%(asctime)s | %(levelname)s | %(name)s | %(message)s"
    )

    console_handler = logging.StreamHandler(sys.stdout)
    console_handler.setFormatter(formatter)

    logger.addHandler(console_handler)

    return logger
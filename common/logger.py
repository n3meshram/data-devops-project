from datetime import datetime
import csv
import os


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
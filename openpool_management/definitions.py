# openpool_management/definitions.py
"""
Dagster definitions for the Open Pool event sourcing system.
"""
from dagster import Definitions
import os
from dotenv import load_dotenv

from openpool_management.assets.events import raw_events,processed_jobs
from openpool_management.assets.metrics import worker_performance_analytics, worker_summary_analytics, worker_summary_s3,worker_performance_s3
from openpool_management.assets.payment import worker_payments
from openpool_management.assets.worker import worker_connections,worker_fees
from openpool_management.configs import S3Config, PaymentConfig, PoolConfig
from openpool_management.io_manager import partitioned_filesystem_io_manager
from openpool_management.jobs import process_inbound_events_job, worker_performance_job, worker_payment_job, \
    worker_summary_job
from openpool_management.resources import S3Resource, PaymentResource, PoolResource
from openpool_management.sensors import s3_event_sensor, worker_payment_sensor, post_payment_summary_sensor, post_processing_summary_sensor

# Load environment variables from .env file
load_dotenv()

# -------------------- Definitions --------------------

# Define the Dagster repository
defs = Definitions(
    assets=[
        raw_events,
        processed_jobs,
        worker_connections,
        worker_fees,
        worker_payments,
        worker_performance_analytics,
        worker_summary_analytics,
        worker_summary_s3,worker_performance_s3
    ],
    jobs=[
        process_inbound_events_job,
        worker_payment_job,
        worker_summary_job,
        worker_performance_job,
    ],
    sensors=[s3_event_sensor, worker_payment_sensor, post_payment_summary_sensor, post_processing_summary_sensor],
    resources={
        "io_manager": partitioned_filesystem_io_manager,
        "s3": S3Resource(
            config=S3Config(
                bucket=os.environ.get("S3_BUCKET", "raw-events"),
                endpoint_url=os.environ.get("S3_ENDPOINT", "https://obj-store-console.xode.app/"),
                aws_access_key_id=os.environ.get("AWS_ACCESS_KEY_ID"),
                aws_secret_access_key=os.environ.get("AWS_SECRET_ACCESS_KEY"),
                aws_region=os.environ.get("AWS_REGION", "us-east-1"),
                path_style=True,
                archive_bucket=os.environ.get("S3_ARCHIVE_BUCKET"),
                archive_prefix=os.environ.get("S3_ARCHIVE_PREFIX", "archive/"),
            )
        ),
        "s3_metrics": S3Resource(
            config=S3Config(
                bucket=os.environ.get("S3_METRICS_BUCKET", "open-pool-metrics"),
                endpoint_url=os.environ.get("S3_ENDPOINT", "https://obj-store-console.xode.app/"),
                aws_access_key_id=os.environ.get("AWS_ACCESS_KEY_ID"),
                aws_secret_access_key=os.environ.get("AWS_SECRET_ACCESS_KEY"),
                aws_region=os.environ.get("AWS_REGION", "us-east-1"),
                path_style=True,
            )
        ),
        "payment_processor": PaymentResource(
            config=PaymentConfig()
        ),
        "pool": PoolResource(
            config=PoolConfig()
        ),
    },
)

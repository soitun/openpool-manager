import os

from dagster import AssetKey, define_asset_job, AssetSelection

_MAX_CONCURRENT = int(os.environ.get("MAX_CONCURRENT_PROCESSES", "5"))

# -------------------- Jobs --------------------
# Define the main job that runs the whole pipeline (excludes payments and payment-dependent assets)
process_inbound_events_job = define_asset_job(
    name="process_inbound_events",
    selection=(
        AssetSelection.all()
        - AssetSelection.keys(AssetKey(["worker", "worker_payments"]))
        - AssetSelection.keys(AssetKey(["summary", "worker_summary_analytics"]))
        - AssetSelection.keys(AssetKey(["export", "worker_summary_s3"]))
    ),
    description="Process OpenPool events (excludes payments and payment-dependent analytics)",
    config={"execution": {"config": {"multiprocess": {"max_concurrent": _MAX_CONCURRENT}}}}
)

# Define the worker payment job
worker_payment_job = define_asset_job(
    name="worker_payment_job",
    selection=AssetSelection.keys(
        AssetKey(["worker", "worker_payments"])
    ),
    description="Process worker payments when thresholds are reached"
)

# Define a job for worker summary analytics
worker_summary_job = define_asset_job(
    name="worker_summary_job",
    selection=AssetSelection.keys(
        AssetKey(["summary", "worker_summary_analytics"]),
        AssetKey(["export", "worker_summary_s3"])
    ),
    description="Generate comprehensive worker summaries and export to S3"
)

# Define a job for worker performance analytics
worker_performance_job = define_asset_job(
    name="worker_performance_job",
    selection=AssetSelection.keys(
        AssetKey(["worker_performance_analytics"]),
    ),
    description="Analyze worker performance metrics"
)

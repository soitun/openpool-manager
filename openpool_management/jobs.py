from dagster import AssetKey, define_asset_job, AssetSelection

# -------------------- Jobs --------------------
# Define the main job that runs the whole pipeline
process_inbound_events_job = define_asset_job(
    name="process_inbound_events",
    selection=AssetSelection.all(),
    description="Process OpenPool events by region, node type",
    # Add config to control partition handling if needed
    config={"execution": {"config": {"multiprocess": {"max_concurrent": 5}}}}
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
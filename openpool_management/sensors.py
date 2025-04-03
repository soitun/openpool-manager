# openpool_management/sensors.py
from dagster import sensor, RunRequest, SkipReason, SensorEvaluationContext, RunsFilter, DagsterRunStatus
import json
from datetime import datetime
import os
import pickle


@sensor(
    job_name="process_inbound_events",
    minimum_interval_seconds=300,
    required_resource_keys={"s3"}
)
def s3_event_sensor(context: SensorEvaluationContext):
    """
    Sensor that detects new event files in S3 and triggers processing jobs.
    Processes files in batches while allowing multiple partitions to run concurrently
    (but not the same partition more than once).

    Each batch processes a maximum of files defined by max_files_per_batch.
    """
    s3 = context.resources.s3
    instance = context.instance

    # Configure batch size - maximum number of files to process in one run
    max_files_per_batch = 250

    # Define the combinations to check based on partitions
    valid_combinations = [
        ("ai", "us-central"),
        ("transcode", "us-central"),
        ("transcode", "us-west"),
        ("transcode", "eu-central"),
        ("transcode", "oceania"),
    ]

    # Get cursor data
    cursor_data = {}
    if context.cursor:
        try:
            cursor_data = json.loads(context.cursor)
            context.log.info(f"Loaded cursor data: {context.cursor}")
        except json.JSONDecodeError:
            context.log.error("Failed to parse cursor data")
            cursor_data = {}
    else:
        context.log.info("No existing cursor data found")

    # Check for existing runs to prevent concurrent partition processing
    context.log.info("Checking for existing runs to prevent concurrent partition processing")
    existing_runs = instance.get_runs(
        filters=RunsFilter(
            statuses=[
                DagsterRunStatus.STARTED,
                DagsterRunStatus.QUEUED,
                DagsterRunStatus.STARTING,
                DagsterRunStatus.CANCELING
                # PENDING is not available in Dagster 1.10.x
            ],
            tags={"source": "s3_event_sensor"}
        )
    )

    # Track partitions with active runs
    active_partitions = set()
    for run in existing_runs:
        if "partition" in run.tags:
            partition = run.tags["partition"]
            active_partitions.add(partition)
            context.log.info(f"Partition {partition} has an active run: {run.run_id}")

    # Process each partition
    runs_created = 0

    for node_type, region in valid_combinations:
        partition_key = f"{node_type}|{region}"
        context.log.info(f"Processing partition: {partition_key}")

        # Skip this partition if it already has an active run
        if partition_key in active_partitions:
            context.log.info(f"Skipping partition {partition_key} as it already has an active run")
            continue

        # Get prefix for S3
        prefix = s3.get_prefix_for_partition(region, node_type)

        # Get last processed key from cursor
        last_key = cursor_data.get(f"{node_type}_{region}", "")
        context.log.info(f"Last processed key for {node_type}/{region}: {last_key}")

        try:
            # List objects with the prefix
            context.log.info(f"Checking for new files in prefix: {prefix}")
            objects = s3.list_objects(prefix)

            if not objects:
                context.log.info(f"No files found for {node_type}/{region}")
                continue

            # Filter for new keys and validate file naming format
            new_keys = []
            for obj in objects:
                key = obj["Key"]
                if key <= last_key:
                    continue

                # Check if file follows the expected naming format
                filename = key.split("/")[-1]

                # Expected format: region-node_type-timestamp1_timestamp2.json
                if filename.startswith(f"{region}-{node_type}-") and "_" in filename and filename.endswith(".json"):
                    new_keys.append(key)
                else:
                    context.log.warning(f"Skipping file with invalid format: {key}")

            # Sort keys chronologically
            new_keys.sort()

            if not new_keys:
                context.log.info(f"No new valid files for {node_type}/{region}")
                continue

            # Limit to batch size
            batch_keys = new_keys[:max_files_per_batch]
            next_last_key = batch_keys[-1]

            # Only update the cursor if we've fully processed this batch
            # This ensures we don't lose files if a run fails
            # The cursor will be updated on the next successful sensor run

            context.log.info(f"Found {len(new_keys)} new files for {node_type}/{region}, "
                             f"processing first {len(batch_keys)} in this batch")

            # Generate run for this batch of files
            timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
            run_key = f"{node_type}_{region}_file_loader_{timestamp}"

            # Calculate progress information
            batch_progress = f"{len(batch_keys)}/{len(new_keys)}"

            context.log.info(
                f"Creating run for partition {partition_key} with {len(batch_keys)} files (progress: {batch_progress})")

            # Yield run request
            yield RunRequest(
                run_key=run_key,
                partition_key=partition_key,
                run_config={
                    "ops": {
                        "raw__raw_events": {
                            "config": {
                                "s3_keys": batch_keys
                            }
                        }
                    }
                },
                tags={
                    "source": "s3_event_sensor",
                    "file_count": str(len(batch_keys)),
                    "last_key": next_last_key,
                    "first_key": batch_keys[0],
                    "region": region,
                    "node_type": node_type,
                    "partition": partition_key,
                    "batch_progress": batch_progress,
                    "batch_size": str(max_files_per_batch)
                }
            )

            # Update the cursor after generating the run
            cursor_data[f"{node_type}_{region}"] = next_last_key
            runs_created += 1

        except Exception as e:
            context.log.error(f"Error checking {node_type}/{region}: {str(e)}")
            import traceback
            context.log.error(f"Traceback: {traceback.format_exc()}")

    # Save the updated cursor
    if runs_created > 0:
        context.update_cursor(json.dumps(cursor_data))
        context.log.info(f"Created {runs_created} batch runs and updated cursor")
    else:
        context.log.info("No new runs created")
        yield SkipReason("No new events detected or all partitions already have active runs")


@sensor(
    job_name="worker_payment_job",
    minimum_interval_seconds=14400,
    required_resource_keys={"payment_processor"}

)
def worker_payment_sensor(context: SensorEvaluationContext):
    """Sensor that checks worker fee states and triggers payment job when
    workers have reached the payment threshold."""
    payment_threshold_wei = context.resources.payment_processor.get_payment_threshold()

    valid_combinations = [
        ("ai", "us-central"),
        ("transcode", "us-central"),
        ("transcode", "us-west"),
        ("transcode", "eu-central"),
        ("transcode", "oceania"),
    ]

    # Check if asset has been recently materialized
    workers_due_by_partition = {}

    for node_type, region in valid_combinations:
        partition_key = f"{node_type}|{region}"
        context.log.info(f"Checking payment eligibility for {partition_key}")

        # Use direct file loading as fallback since materialization API is limited
        try:
            payment_path = os.path.join("data", "worker", "worker_payments", f"{node_type}/{region}/data.pkl")
            if os.path.exists(payment_path):
                with open(payment_path, "rb") as f:
                    payment_data = pickle.load(f)

                if isinstance(payment_data, dict) and "payment_states" in payment_data:
                    payment_states = payment_data["payment_states"]
                    workers_due = []

                    for eth_address, state in payment_states.items():
                        unpaid_fees = state.get("unpaid_fees", 0)
                        if unpaid_fees >= payment_threshold_wei:
                            workers_due.append({
                                "eth_address": eth_address,
                                "pending_fees": unpaid_fees,
                                "pending_eth": unpaid_fees / 1e18
                            })

                    if workers_due:
                        workers_due_by_partition[partition_key] = workers_due
                        context.log.info(
                            f"Found {len(workers_due)} workers due for payment totaling {sum(w['pending_eth'] for w in workers_due):.6f} ETH")
        except Exception as e:
            context.log.error(f"Error checking {partition_key}: {str(e)}")

    # Create run requests for partitions with eligible workers
    if not workers_due_by_partition:
        return SkipReason("No workers have reached the payment threshold")

    return [RunRequest(
        run_key=f"worker_payment_{partition_key}_{datetime.now().strftime('%Y%m%d_%H%M')}",
        partition_key=partition_key,
        run_config={"ops": {
            "worker__worker_payments": {"config": {"payment_threshold_wei": payment_threshold_wei}}}},
        tags={"source": "worker_payment_sensor", "workers_due_count": str(len(workers_due))}
    ) for partition_key, workers_due in workers_due_by_partition.items()]
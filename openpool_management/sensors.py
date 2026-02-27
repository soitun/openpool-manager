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
    Two-phase sensor that detects new event files in S3 and triggers processing jobs.

    Phase 1 (Reconcile): Check pending runs — on success, promote cursor and archive;
    on failure, clear pending state so keys are re-scanned.

    Phase 2 (Scan): For partitions without pending runs, scan S3 for new files
    and yield RunRequests. Cursor only advances after confirmed success.
    """
    s3 = context.resources.s3
    instance = context.instance

    # Configure batch size
    max_files_per_batch = 250

    # Define the combinations to check based on partitions
    valid_combinations = [
        ("ai", "us-central"),
        ("transcode", "us-central"),
        ("transcode", "us-west"),
        ("transcode", "eu-central"),
        ("transcode", "oceania"),
    ]

    # Load cursor data (two-phase format)
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

    # Ensure all partitions have the two-phase structure
    for node_type, region in valid_combinations:
        key = f"{node_type}_{region}"
        if key not in cursor_data:
            cursor_data[key] = {
                "confirmed": "",
                "pending": None,
                "pending_run_id": None,
                "pending_keys": []
            }
        elif isinstance(cursor_data[key], str):
            # Should not happen on fresh install, but handle gracefully
            cursor_data[key] = {
                "confirmed": cursor_data[key],
                "pending": None,
                "pending_run_id": None,
                "pending_keys": []
            }

    # ============================================================
    # PHASE 1: Reconcile pending runs
    # ============================================================
    for node_type, region in valid_combinations:
        key = f"{node_type}_{region}"
        entry = cursor_data[key]

        if not entry.get("pending_run_id"):
            continue

        pending_run_id = entry["pending_run_id"]

        # Handle placeholder run IDs — look up actual run by run_key tag
        if pending_run_id.startswith("__pending_lookup_"):
            run_key = pending_run_id.replace("__pending_lookup_", "")
            context.log.info(f"[{key}] Looking up run by run_key: {run_key}")
            matching_runs = instance.get_runs(
                filters=RunsFilter(tags={"dagster/run_key": run_key}),
                limit=1
            )
            if matching_runs:
                pending_run_id = matching_runs[0].run_id
                entry["pending_run_id"] = pending_run_id
                context.log.info(f"[{key}] Resolved run_key to run_id: {pending_run_id}")
            else:
                context.log.info(f"[{key}] Run for run_key {run_key} not found yet, waiting...")
                continue

        # Query the actual run status
        runs = instance.get_runs(
            filters=RunsFilter(run_ids=[pending_run_id]),
            limit=1
        )

        if not runs:
            context.log.warning(f"[{key}] Pending run {pending_run_id} not found, clearing pending state")
            entry["pending"] = None
            entry["pending_run_id"] = None
            entry["pending_keys"] = []
            continue

        run = runs[0]
        run_status = run.status

        if run_status == DagsterRunStatus.SUCCESS:
            # Promote pending to confirmed
            context.log.info(f"[{key}] Run {pending_run_id} succeeded, advancing cursor")
            entry["confirmed"] = entry["pending"]

            # Archive processed S3 files (Fix 5)
            pending_keys = entry.get("pending_keys", [])
            if pending_keys and hasattr(s3, 'config') and getattr(s3.config, 'archive_bucket', None):
                try:
                    result = s3.archive_keys(
                        keys=pending_keys,
                        archive_bucket=s3.config.archive_bucket,
                        archive_prefix=getattr(s3.config, 'archive_prefix', 'archive/')
                    )
                    context.log.info(f"[{key}] Archived {result['archived']}/{len(pending_keys)} files")
                except Exception as e:
                    context.log.error(f"[{key}] Archival failed (non-blocking): {e}")

            # Clear pending state
            entry["pending"] = None
            entry["pending_run_id"] = None
            entry["pending_keys"] = []

        elif run_status in (DagsterRunStatus.FAILURE, DagsterRunStatus.CANCELED):
            # Run failed — clear pending state, keys will be re-scanned next tick
            context.log.warning(
                f"[{key}] Run {pending_run_id} {run_status.value}, clearing pending state. "
                f"Files will be re-processed on next scan."
            )
            entry["pending"] = None
            entry["pending_run_id"] = None
            entry["pending_keys"] = []

        else:
            # Still running (STARTED, QUEUED, STARTING, CANCELING)
            context.log.info(f"[{key}] Run {pending_run_id} still {run_status.value}, skipping partition")
            # Don't touch this partition in Phase 2

    # ============================================================
    # PHASE 2: Scan for new files (only for partitions without pending runs)
    # ============================================================
    runs_created = 0

    for node_type, region in valid_combinations:
        key = f"{node_type}_{region}"
        partition_key = f"{node_type}|{region}"
        entry = cursor_data[key]

        # Skip if there's still a pending run
        if entry.get("pending_run_id"):
            context.log.info(f"Skipping partition {partition_key} — pending run in progress")
            continue

        # Get prefix for S3
        prefix = s3.get_prefix_for_partition(region, node_type)

        # Use confirmed key as the starting point
        last_key = entry.get("confirmed", "")
        context.log.info(f"Last confirmed key for {node_type}/{region}: {last_key}")

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
                obj_key = obj["Key"]
                if obj_key <= last_key:
                    continue

                # Check if file follows the expected naming format
                filename = obj_key.split("/")[-1]

                # Expected format: region-node_type-timestamp1_timestamp2.json
                if filename.startswith(f"{region}-{node_type}-") and "_" in filename and filename.endswith(".json"):
                    new_keys.append(obj_key)
                else:
                    context.log.warning(f"Skipping file with invalid format: {obj_key}")

            # Sort keys chronologically
            new_keys.sort()

            if not new_keys:
                context.log.info(f"No new valid files for {node_type}/{region}")
                continue

            # Limit to batch size
            batch_keys = new_keys[:max_files_per_batch]
            next_last_key = batch_keys[-1]

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
                    "dagster/run_key": run_key,
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

            # Set pending state — cursor does NOT advance until run succeeds
            entry["pending"] = next_last_key
            entry["pending_run_id"] = f"__pending_lookup_{run_key}"
            entry["pending_keys"] = batch_keys
            runs_created += 1

        except Exception as e:
            context.log.error(f"Error checking {node_type}/{region}: {str(e)}")
            import traceback
            context.log.error(f"Traceback: {traceback.format_exc()}")

    # Always persist the cursor (Phase 1 may have updated confirmed keys)
    context.update_cursor(json.dumps(cursor_data))

    if runs_created > 0:
        context.log.info(f"Created {runs_created} batch runs and updated cursor")
    else:
        context.log.info("No new runs created")
        yield SkipReason("No new events detected or all partitions have pending/active runs")


@sensor(
    job_name="worker_payment_job",
    minimum_interval_seconds=14400,
    required_resource_keys={"payment_processor"}

)
def worker_payment_sensor(context: SensorEvaluationContext):
    """Sensor that checks worker fee states and triggers payment job when
    workers have reached the payment threshold."""
    instance = context.instance
    payment_threshold_wei = context.resources.payment_processor.get_payment_threshold()

    valid_combinations = [
        ("ai", "us-central"),
        ("transcode", "us-central"),
        ("transcode", "us-west"),
        ("transcode", "eu-central"),
        ("transcode", "oceania"),
    ]

    # Check for active payment runs to prevent overlapping payments
    existing_payment_runs = instance.get_runs(
        filters=RunsFilter(
            statuses=[DagsterRunStatus.STARTED, DagsterRunStatus.QUEUED, DagsterRunStatus.STARTING],
            tags={"source": "worker_payment_sensor"}
        )
    )
    active_payment_partitions = {
        run.tags.get("partition") for run in existing_payment_runs if "partition" in run.tags
    }
    if active_payment_partitions:
        context.log.info(f"Active payment partitions: {active_payment_partitions}")

    # Check if asset has been recently materialized
    workers_due_by_partition = {}

    base_dir = os.environ.get("IO_MANAGER_BASE_DIR", "data")

    for node_type, region in valid_combinations:
        partition_key = f"{node_type}|{region}"

        # Skip partitions with active payment runs
        if partition_key in active_payment_partitions:
            context.log.info(f"Skipping {partition_key} — payment run already active")
            continue

        context.log.info(f"Checking payment eligibility for {partition_key}")

        # Use IO_MANAGER_BASE_DIR instead of hardcoded "data"
        try:
            payment_path = os.path.join(base_dir, "worker", "worker_payments", f"{node_type}/{region}/data.pkl")
            if os.path.exists(payment_path):
                try:
                    with open(payment_path, "rb") as f:
                        payment_data = pickle.load(f)
                except Exception as e:
                    # Try .bak file on corrupt primary
                    bak_path = payment_path + ".bak"
                    if os.path.exists(bak_path):
                        context.log.warning(f"Primary pickle corrupt, trying backup: {bak_path}")
                        with open(bak_path, "rb") as f:
                            payment_data = pickle.load(f)
                    else:
                        raise

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
        tags={
            "source": "worker_payment_sensor",
            "workers_due_count": str(len(workers_due)),
            "partition": partition_key
        }
    ) for partition_key, workers_due in workers_due_by_partition.items()]

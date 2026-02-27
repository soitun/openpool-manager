# openpool_management/assets/analytics.py
from dagster import asset, AssetIn, MetadataValue
from typing import Dict
import pandas as pd
from datetime import datetime
import os
import json
import numpy as np

from openpool_management.models import WorkerState, parse_partition_key
from openpool_management.partitions import multi_partitions


@asset(
    partitions_def=multi_partitions,
    ins={
        "processed_jobs": AssetIn(
            key=["processed", "processed_jobs"],
        )
    },
    compute_kind="pandas",
    io_manager_key="io_manager",
    group_name="analytics"
)
def worker_performance_analytics(context, processed_jobs: dict):
    """
    Analyze worker performance from raw events.

    For transcode jobs: Calculate best real-time ratio (lower is better).
    For AI jobs: Group by model/pipeline and rank by response time.
    """
    processed_events = processed_jobs["processed_events"]

    node_type, region = parse_partition_key(context.partition_key)

    # Add debug logging about processed jobs
    context.log.info(f"Received {len(processed_events)} processed events for {node_type}/{region}")

    if not processed_events:
        context.log.warning(f"No processed events found for {node_type}/{region}")
        return {
            "transcode_performance": pd.DataFrame(),
            "ai_performance": pd.DataFrame(),
            "ai_model_pipeline_rankings": {},
            "processed_event_ids": set()  # Add tracking of processed event IDs
        }

    # Log the first few events to help debug
    if len(processed_events) > 0:
        context.log.info(f"First job event type: {processed_events[0].event_type}")
        context.log.info(f"First job payload: {processed_events[0].payload}")

    context.log.info(f"Analyzing worker performance for {node_type}/{region}")

    # Load previous results for incremental analytics
    previous_results = None
    try:
        previous_results = context.resources.io_manager.load_previous_output(context)
        if previous_results:
            context.log.info(f"Loaded previous performance analytics")
        else:
            context.log.info(f"No previous performance analytics found — starting fresh")
    except Exception as e:
        context.log.warning(f"Could not load previous performance analytics: {str(e)}")
        previous_results = None

    # Get previously processed event IDs to avoid double counting
    processed_event_ids = set()
    if previous_results and "processed_event_ids" in previous_results:
        processed_event_ids = previous_results["processed_event_ids"]
        context.log.info(f"Found {len(processed_event_ids)} previously processed event IDs")

    # Filter out previously processed events
    new_events = [event for event in processed_events if event.id not in processed_event_ids]
    context.log.info(f"After filtering, found {len(new_events)} new events to process")

    # Update our processed event ID set with new event IDs
    for event in new_events:
        processed_event_ids.add(event.id)

    # Process based on node type
    if node_type == "transcode":
        results = process_transcode_events(context, new_events, previous_results)
    elif node_type == "ai":
        results = process_ai_events(context, new_events, previous_results)
    else:
        context.log.error(f"Unknown node type: {node_type}")
        results = {
            "transcode_performance": pd.DataFrame(),
            "ai_performance": pd.DataFrame(),
            "ai_model_pipeline_rankings": {}
        }

    # Add the processed event IDs to the results
    results["processed_event_ids"] = processed_event_ids

    return results

def process_transcode_events(context, processed_events, previous_results=None):
    """Process transcode events and analyze real-time ratio performance."""
    # Extract necessary data from each event
    transcode_data = []

    for event in processed_events:
        # Make sure we have the required data
        if "ethAddress" not in event.payload or "realTimeRatio" not in event.payload:
            continue

        # Calculate computeUnitsPerSecond - convert responseTime from ms to seconds
        compute_units = event.payload.get("computeUnits", 0)
        response_time_ms = event.payload.get("responseTime", 0)

        # Handle cases where responseTime is missing or zero
        if response_time_ms is None or response_time_ms <= 0:
            context.log.warning(
                f"Invalid response time in event {event.id}: {response_time_ms}. Setting computeUnitsPerSecond to 0.")
            compute_units_per_second = 0
        else:
            # Convert from ms to seconds
            response_time_s = response_time_ms / 1000
            compute_units_per_second = compute_units / response_time_s

        transcode_data.append({
            "worker_address": event.payload["ethAddress"],
            "event_id": event.id,
            "timestamp": event.dt,
            "real_time_ratio": event.payload.get("realTimeRatio"),
            "response_time": response_time_ms,
            "compute_units": compute_units,
            "fees": event.payload.get("fees"),
            "compute_units_per_second": compute_units_per_second
        })

    if not transcode_data:
        context.log.warning("No valid transcode performance data found in current batch")
        # Return previous results if available, otherwise empty result
        if previous_results:
            context.log.info("Using previous performance results as no new data found")
            return previous_results
        return {
            "transcode_performance": pd.DataFrame(),
            "ai_performance": pd.DataFrame(),
            "ai_model_pipeline_rankings": {}
        }

    # Create DataFrame for transcode performance
    new_df_transcode = pd.DataFrame(transcode_data)

    # If we have previous results, we'll need to update them with the new data
    if previous_results and previous_results.get("transcode_performance") is not None and not previous_results.get("transcode_performance").empty:
        prev_df = previous_results.get("transcode_performance")

        # Create dictionaries to store aggregate values by worker
        worker_data = {}

        # First, initialize with previous worker data
        for _, row in prev_df.iterrows():
            worker_addr = row["worker_address"]
            worker_data[worker_addr] = {
                "worker_address": worker_addr,
                "job_count": row["job_count"],
                "total_real_time_ratio": row["mean_real_time_ratio"] * row["job_count"],
                "min_real_time_ratio": row["min_real_time_ratio"],
                "max_real_time_ratio": row["max_real_time_ratio"],
                "total_response_time": row["mean_response_time"] * row[
                    "job_count"] if "mean_response_time" in row else 0,
                "total_compute_units": row["total_compute_units"] if "total_compute_units" in row else 0,
                "total_fees": row["total_fees"] if "total_fees" in row else 0,
                "total_compute_units_per_second": row["mean_compute_units_per_second"] * row[
                    "job_count"] if "mean_compute_units_per_second" in row else 0
            }

        # Now add in the new data
        for _, row in new_df_transcode.iterrows():
            worker_addr = row["worker_address"]
            if worker_addr not in worker_data:
                worker_data[worker_addr] = {
                    "worker_address": worker_addr,
                    "job_count": 0,
                    "total_real_time_ratio": 0,
                    "min_real_time_ratio": float('inf'),
                    "max_real_time_ratio": 0,
                    "total_response_time": 0,
                    "total_compute_units": 0,
                    "total_fees": 0,
                    "total_compute_units_per_second": 0
                }

            # Update the aggregates
            worker_data[worker_addr]["job_count"] += 1
            worker_data[worker_addr]["total_real_time_ratio"] += row["real_time_ratio"]
            worker_data[worker_addr]["min_real_time_ratio"] = min(worker_data[worker_addr]["min_real_time_ratio"],
                                                                  row["real_time_ratio"])
            worker_data[worker_addr]["max_real_time_ratio"] = max(worker_data[worker_addr]["max_real_time_ratio"],
                                                                  row["real_time_ratio"])
            worker_data[worker_addr]["total_response_time"] += row["response_time"] if pd.notna(
                row["response_time"]) else 0
            worker_data[worker_addr]["total_compute_units"] += row["compute_units"] if pd.notna(
                row["compute_units"]) else 0
            worker_data[worker_addr]["total_fees"] += row["fees"] if pd.notna(row["fees"]) else 0
            worker_data[worker_addr]["total_compute_units_per_second"] += row["compute_units_per_second"] if pd.notna(
                row["compute_units_per_second"]) else 0

        # Convert back to DataFrame with computed aggregates
        worker_performance = pd.DataFrame(list(worker_data.values()))

        # Calculate means based on job count
        worker_performance["mean_real_time_ratio"] = worker_performance["total_real_time_ratio"] / worker_performance[
            "job_count"]
        worker_performance["median_real_time_ratio"] = worker_performance[
            "mean_real_time_ratio"]  # We can't recover true median, use mean as approximation
        worker_performance["mean_response_time"] = worker_performance["total_response_time"] / worker_performance[
            "job_count"]
        worker_performance["median_response_time"] = worker_performance[
            "mean_response_time"]  # Again, using mean as approximation
        worker_performance["mean_compute_units_per_second"] = worker_performance["total_compute_units_per_second"] / \
                                                              worker_performance[
                                                                  "job_count"]
        worker_performance["median_compute_units_per_second"] = worker_performance[
            "mean_compute_units_per_second"]  # Again, using mean as approximation

        # Replace infinite values with NaN, which may occur if no jobs were processed
        worker_performance.replace([float('inf'), -float('inf')], np.nan, inplace=True)

    else:
        # No previous data, just process the new data
        # Calculate median real-time ratio per worker
        worker_performance = new_df_transcode.groupby("worker_address").agg(
            median_real_time_ratio=("real_time_ratio", "median"),
            mean_real_time_ratio=("real_time_ratio", "mean"),
            min_real_time_ratio=("real_time_ratio", "min"),
            max_real_time_ratio=("real_time_ratio", "max"),
            job_count=("real_time_ratio", "count"),
            median_response_time=("response_time", "median"),
            mean_response_time=("response_time", "mean"),
            total_compute_units=("compute_units", "sum"),
            total_fees=("fees", "sum"),
            median_compute_units_per_second=("compute_units_per_second", "median"),
            mean_compute_units_per_second=("compute_units_per_second", "mean"),
            min_compute_units_per_second=("compute_units_per_second", "min"),
            max_compute_units_per_second=("compute_units_per_second", "max")
        ).reset_index()

    # Rank workers by median real-time ratio (lower is better)
    worker_performance = worker_performance.sort_values("median_real_time_ratio")
    worker_performance["rank"] = range(1, len(worker_performance) + 1)

    # Add percentile ranking (0-1 scale, lower is better)
    if len(worker_performance) > 1:
        worker_performance["percentile"] = (worker_performance["rank"] - 1) / (len(worker_performance) - 1)
    else:
        worker_performance["percentile"] = 0

    context.log.info(f"Analyzed performance for {len(worker_performance)} transcode workers")

    # Return the analysis results
    return {
        "transcode_performance": worker_performance,
        "ai_performance": pd.DataFrame(),  # Empty for transcode node_type
        "ai_model_pipeline_rankings": {}  # Empty for transcode node_type
    }

def process_ai_events(context, processed_events, previous_results=None):
    """Process AI events and analyze response time performance by model/pipeline."""
    # Extract necessary data from each event
    ai_data = []

    for event in processed_events:
        # Make sure we have the required data
        if "ethAddress" not in event.payload or "responseTime" not in event.payload:
            continue

        # Calculate computeUnitsPerSecond - convert responseTime from ms to seconds
        compute_units = event.payload.get("computeUnits", 0)
        response_time_ms = event.payload.get("responseTime", 0)

        # Handle cases where responseTime is missing or zero
        if response_time_ms is None or response_time_ms <= 0:
            context.log.warning(
                f"Invalid response time in event {event.id}: {response_time_ms}. Setting computeUnitsPerSecond to 0.")
            compute_units_per_second = 0
        else:
            # Convert from ms to seconds
            response_time_s = response_time_ms / 1000
            compute_units_per_second = compute_units / response_time_s

        ai_data.append({
            "worker_address": event.payload["ethAddress"],
            "event_id": event.id,
            "timestamp": event.dt,
            "response_time": response_time_ms,
            "compute_units": compute_units,
            "fees": event.payload.get("fees"),
            "model_id": event.payload.get("modelID"),
            "pipeline": event.payload.get("pipeline"),
            "compute_units_per_second": compute_units_per_second
        })

    if not ai_data:
        context.log.warning("No valid AI performance data found in current batch")
        # Return previous results if available, otherwise empty result
        if previous_results:
            context.log.info("Using previous performance results as no new data found")
            return previous_results
        return {
            "transcode_performance": pd.DataFrame(),
            "ai_performance": pd.DataFrame(),
            "ai_model_pipeline_rankings": {}
        }

    # Create DataFrame for AI performance
    new_df_ai = pd.DataFrame(ai_data)

    # Handle missing model_id or pipeline
    new_df_ai["model_id"] = new_df_ai["model_id"].fillna("unknown_model")
    new_df_ai["pipeline"] = new_df_ai["pipeline"].fillna("unknown_pipeline")
    new_df_ai["model_pipeline"] = new_df_ai["model_id"] + " | " + new_df_ai["pipeline"]

    # If we have previous results, we'll need to update them with the new data
    if previous_results and previous_results.get("ai_performance") is not None and not previous_results.get("ai_performance").empty:
        prev_df = previous_results.get("ai_performance")

        # Create a mapping for aggregating by worker, model, and pipeline
        worker_model_pipeline_data = {}

        # First, initialize with previous data
        for _, row in prev_df.iterrows():
            key = (row["worker_address"], row["model_id"], row["pipeline"], row["model_pipeline"])
            worker_model_pipeline_data[key] = {
                "worker_address": row["worker_address"],
                "model_id": row["model_id"],
                "pipeline": row["pipeline"],
                "model_pipeline": row["model_pipeline"],
                "job_count": row["job_count"],
                "total_response_time": row["mean_response_time"] * row["job_count"],
                "min_response_time": row["min_response_time"],
                "max_response_time": row["max_response_time"],
                "total_compute_units": row["total_compute_units"],
                "total_fees": row["total_fees"],
                "total_compute_units_per_second": row["mean_compute_units_per_second"] * row[
                    "job_count"] if "mean_compute_units_per_second" in row else 0
            }

        # Now add the new data
        for _, row in new_df_ai.iterrows():
            key = (row["worker_address"], row["model_id"], row["pipeline"], row["model_pipeline"])
            if key not in worker_model_pipeline_data:
                worker_model_pipeline_data[key] = {
                    "worker_address": row["worker_address"],
                    "model_id": row["model_id"],
                    "pipeline": row["pipeline"],
                    "model_pipeline": row["model_pipeline"],
                    "job_count": 0,
                    "total_response_time": 0,
                    "min_response_time": float('inf'),
                    "max_response_time": 0,
                    "total_compute_units": 0,
                    "total_fees": 0,
                    "total_compute_units_per_second": 0
                }

            # Update the aggregates
            worker_model_pipeline_data[key]["job_count"] += 1
            worker_model_pipeline_data[key]["total_response_time"] += row["response_time"] if pd.notna(
                row["response_time"]) else 0
            worker_model_pipeline_data[key]["min_response_time"] = min(
                worker_model_pipeline_data[key]["min_response_time"],
                row["response_time"] if pd.notna(row["response_time"]) else float('inf'))
            worker_model_pipeline_data[key]["max_response_time"] = max(
                worker_model_pipeline_data[key]["max_response_time"],
                row["response_time"] if pd.notna(row["response_time"]) else 0)
            worker_model_pipeline_data[key]["total_compute_units"] += row["compute_units"] if pd.notna(
                row["compute_units"]) else 0
            worker_model_pipeline_data[key]["total_fees"] += row["fees"] if pd.notna(row["fees"]) else 0
            worker_model_pipeline_data[key]["total_compute_units_per_second"] += row[
                "compute_units_per_second"] if pd.notna(
                row["compute_units_per_second"]) else 0

        # Convert back to DataFrame
        worker_performance = pd.DataFrame(list(worker_model_pipeline_data.values()))

        # Calculate means
        worker_performance["mean_response_time"] = worker_performance["total_response_time"] / worker_performance[
            "job_count"]
        worker_performance["median_response_time"] = worker_performance[
            "mean_response_time"]  # Using mean as approximation
        worker_performance["mean_compute_units_per_second"] = worker_performance["total_compute_units_per_second"] / \
                                                              worker_performance[
                                                                  "job_count"]
        worker_performance["median_compute_units_per_second"] = worker_performance[
            "mean_compute_units_per_second"]  # Using mean as approximation

        # Replace infinite values with NaN
        worker_performance.replace([float('inf'), -float('inf')], np.nan, inplace=True)

    else:
        # Just process the new data
        worker_performance = new_df_ai.groupby(["worker_address", "model_id", "pipeline", "model_pipeline"]).agg(
            median_response_time=("response_time", "median"),
            mean_response_time=("response_time", "mean"),
            min_response_time=("response_time", "min"),
            max_response_time=("response_time", "max"),
            job_count=("response_time", "count"),
            total_compute_units=("compute_units", "sum"),
            total_fees=("fees", "sum"),
            median_compute_units_per_second=("compute_units_per_second", "median"),
            mean_compute_units_per_second=("compute_units_per_second", "mean"),
            min_compute_units_per_second=("compute_units_per_second", "min"),
            max_compute_units_per_second=("compute_units_per_second", "max")
        ).reset_index()

    # Create rankings for each model/pipeline combination
    rankings = {}

    for model_pipeline in worker_performance["model_pipeline"].unique():
        # Get workers for this model/pipeline
        mp_workers = worker_performance[worker_performance["model_pipeline"] == model_pipeline]

        # Rank workers by median response time (lower is better)
        mp_workers = mp_workers.sort_values("median_response_time")
        mp_workers["rank"] = range(1, len(mp_workers) + 1)

        # Add percentile ranking (0-1 scale, lower is better)
        if len(mp_workers) > 1:
            mp_workers["percentile"] = (mp_workers["rank"] - 1) / (len(mp_workers) - 1)
        else:
            mp_workers["percentile"] = 0

        # Store ranked workers for this model/pipeline
        rankings[model_pipeline] = mp_workers

    context.log.info(f"Analyzed performance for AI workers across {len(rankings)} model/pipeline combinations")

    # Return the analysis results
    return {
        "transcode_performance": pd.DataFrame(),  # Empty for AI node_type
        "ai_performance": worker_performance,
        "ai_model_pipeline_rankings": rankings
    }

@asset(
    key_prefix=["summary"],
    group_name="analytics",
    description="Comprehensive worker summary combining fees, connections, and payments",
    ins={
        "worker_fees": AssetIn(key_prefix=["worker"]),
        "worker_connections": AssetIn(key_prefix=["worker"]),
        "worker_payments": AssetIn(key=["worker", "worker_payments"])
    },
    partitions_def=multi_partitions,
    required_resource_keys={"payment_processor", "pool"}
)
def worker_summary_analytics(
        context,
        worker_fees: Dict,
        worker_connections: Dict,
        worker_payments: Dict
) -> Dict:
    """
    Create a comprehensive summary of worker data combining fee information,
    connection status, performance metrics, and payment history.
    """
    # Parse partition key
    node_type, region = parse_partition_key(context.partition_key)
    context.log.info(f"Generating worker summary for {node_type}/{region}")

    # Get configured values instead of hard-coding
    pool_commission_rate = context.resources.pool.get_commission_rate()
    payment_threshold_wei = context.resources.payment_processor.get_payment_threshold()

    # Load previous summary for incremental processing
    previous_summary = None
    try:
        previous_summary = context.resources.io_manager.load_previous_output(context)
        if previous_summary:
            context.log.info(f"Loaded previous worker summary")
        else:
            context.log.info(f"No previous worker summary found — starting fresh")
    except Exception as e:
        context.log.warning(f"Could not load previous worker summary: {str(e)}")
        previous_summary = None

    # Extract data from nested structures
    worker_fee_states = worker_fees.get("worker_states", {})
    worker_connection_states = worker_connections.get("worker_states", {})
    payment_states = worker_payments.get("payment_states", {})

    # Initialize the worker summary structure
    worker_summary = {
        "node_type": node_type,
        "region": region,
        "timestamp": datetime.now().isoformat(),
        "workers": {},
        "aggregates": {
            "total_workers": 0,
            "active_workers": 0,
            "total_fees": 0,
            "total_fees_paid": 0,
            "total_pending_fees": 0,
            "total_connections": 0,
            "total_payments": 0
        }
    }

    # If we have a previous summary, use its workers as a starting point to maintain history
    if previous_summary and "workers" in previous_summary:
        for eth_address, worker_data in previous_summary["workers"].items():
            # Copy worker data but will update with fresh information
            worker_summary["workers"][eth_address] = worker_data.copy()

            # Reset metrics that will be updated with fresh data
            worker_summary["workers"][eth_address].update({
                "active": False,
                "connections": [],
                "connection_count": 0
            })

        context.log.info(f"Initialized with {len(worker_summary['workers'])} workers from previous summary")

    # Process worker fees - total fees comes from worker_fees
    if worker_fee_states:
        for eth_address, fee_info in worker_fee_states.items():
            # Initialize worker entry if not exists
            if eth_address not in worker_summary["workers"]:
                worker_summary["workers"][eth_address] = {
                    "eth_address": eth_address,
                    "node_type": node_type,
                    "region": region
                }

            # Add fee information
            total_fees = fee_info.get("total_fees", 0)
            worker_earnings = fee_info.get("worker_earnings", 0)
            pool_commission = fee_info.get("pool_commission", 0)

            worker_summary["workers"][eth_address].update({
                "total_fees": total_fees,
                "worker_earnings": worker_earnings,
                "pool_commission": pool_commission
            })

    # Process worker connections
    if worker_connection_states:
        for eth_address, connection_info in worker_connection_states.items():
            # Initialize worker entry if not exists
            if eth_address not in worker_summary["workers"]:
                worker_summary["workers"][eth_address] = {
                    "eth_address": eth_address,
                    "node_type": node_type,
                    "region": region
                }

            # Add connection information
            connections = connection_info.get("connections", [])
            worker_summary["workers"][eth_address].update({
                "connections": connections,
                "connection_count": len(connections),
                "active": len(connections) > 0
            })

            # Update aggregates
            if len(connections) > 0:
                worker_summary["aggregates"]["active_workers"] += 1
            worker_summary["aggregates"]["total_connections"] += len(connections)

    # Process worker payment data - paid fees comes from worker_payments
    if payment_states:
        total_payment_count = 0

        for eth_address, payment_state in payment_states.items():
            # Initialize worker entry if not exists
            if eth_address not in worker_summary["workers"]:
                worker_summary["workers"][eth_address] = {
                    "eth_address": eth_address,
                    "node_type": node_type,
                    "region": region,
                    "total_fees": 0,
                    "worker_earnings": 0,
                    "pool_commission": 0
                }

            # Extract payment info
            payment_history = payment_state.get("payment_history", [])
            total_fees_paid = payment_state.get("total_fees_paid", 0)
            unpaid_fees = payment_state.get("unpaid_fees", 0)

            # Add payment information
            payment_count = len(payment_history)
            last_payment = payment_history[-1] if payment_history else None
            last_payment_date = last_payment.get("timestamp") if last_payment else None

            worker_summary["workers"][eth_address].update({
                "payment_count": payment_count,
                "payment_history": payment_history,
                "last_payment_date": last_payment_date,
                "total_fees_paid": total_fees_paid,
                "pending_fees": unpaid_fees,
                "pending_eth": unpaid_fees / 1e18
            })

            # Update aggregates
            total_payment_count += payment_count

        worker_summary["aggregates"]["total_payments"] = total_payment_count

    # For any workers that don't have connection data, mark as inactive
    for eth_address, worker_data in worker_summary["workers"].items():
        if "active" not in worker_data:
            worker_data["active"] = False
            worker_data["connections"] = []
            worker_data["connection_count"] = 0

    # For any workers that don't have fee data, initialize with zeros
    for eth_address, worker_data in worker_summary["workers"].items():
        if "total_fees" not in worker_data:
            worker_data["total_fees"] = 0
            worker_data["worker_earnings"] = 0
            worker_data["pool_commission"] = 0

    # For any workers that don't have payment data, initialize with empty
    for eth_address, worker_data in worker_summary["workers"].items():
        if "payment_count" not in worker_data:
            worker_data["payment_count"] = 0
            worker_data["payment_history"] = []
            worker_data["last_payment_date"] = None
            worker_data["total_fees_paid"] = 0
            worker_data["pending_fees"] = worker_data.get("worker_earnings", 0) - worker_data.get("total_fees_paid", 0)
            worker_data["pending_eth"] = worker_data["pending_fees"] / 1e18

    # Update worker count
    worker_summary["aggregates"]["total_workers"] = len(worker_summary["workers"])

    # Update aggregates after all workers are processed
    worker_summary["aggregates"].update({
        "total_fees": sum(worker.get("total_fees", 0) for worker in worker_summary["workers"].values()),
        "total_fees_paid": sum(worker.get("total_fees_paid", 0) for worker in worker_summary["workers"].values()),
        "total_pending_fees": sum(worker.get("pending_fees", 0) for worker in worker_summary["workers"].values()),
        "total_pool_commission": sum(worker.get("pool_commission", 0) for worker in worker_summary["workers"].values()),
        "total_worker_earnings": sum(worker.get("worker_earnings", 0) for worker in worker_summary["workers"].values()),
        "pool_commission_rate": pool_commission_rate,  # Use the variable instead of hard-coded value
    })

    # Add payment eligibility based on configured threshold
    workers_eligible_for_payment = []

    for eth_address, worker_data in worker_summary["workers"].items():
        payment_due = worker_data.get("pending_fees", 0) >= payment_threshold_wei
        worker_data["payment_due"] = payment_due

        if payment_due:
            workers_eligible_for_payment.append({
                "eth_address": eth_address,
                "pending_fees": worker_data.get("pending_fees", 0),
                "pending_eth": worker_data.get("pending_eth", 0)
            })

    # Add metadata about workers eligible for payment
    worker_summary["aggregates"]["workers_eligible_for_payment"] = len(workers_eligible_for_payment)

    # Calculate additional helpful aggregates
    worker_summary["aggregates"]["total_pending_eth"] = worker_summary["aggregates"]["total_pending_fees"] / 1e18
    worker_summary["aggregates"]["total_fees_eth"] = worker_summary["aggregates"]["total_fees"] / 1e18
    worker_summary["aggregates"]["total_fees_paid_eth"] = worker_summary["aggregates"]["total_fees_paid"] / 1e18

    # Add percentage of active workers
    if worker_summary["aggregates"]["total_workers"] > 0:
        worker_summary["aggregates"]["active_worker_percentage"] = (
                worker_summary["aggregates"]["active_workers"] / worker_summary["aggregates"]["total_workers"] * 100
        )
    else:
        worker_summary["aggregates"]["active_worker_percentage"] = 0

    # Add metadata to the context
    context.add_output_metadata({
        "node_type": MetadataValue.text(node_type),
        "region": MetadataValue.text(region),
        "total_workers": MetadataValue.int(worker_summary["aggregates"]["total_workers"]),
        "active_workers": MetadataValue.int(worker_summary["aggregates"]["active_workers"]),
        "total_connections": MetadataValue.int(worker_summary["aggregates"]["total_connections"]),
        "total_fees_eth": MetadataValue.float(worker_summary["aggregates"]["total_fees_eth"]),
        "total_pending_eth": MetadataValue.float(worker_summary["aggregates"]["total_pending_eth"]),
        "total_payments": MetadataValue.int(worker_summary["aggregates"]["total_payments"]),
        "workers_eligible_for_payment": MetadataValue.int(len(workers_eligible_for_payment))
    })

    return worker_summary


@asset(
    key_prefix=["export"],
    compute_kind="json",
    group_name="export",
    description="Export worker summary to S3",
    ins={"worker_summary": AssetIn(key=["summary", "worker_summary_analytics"])},
    partitions_def=multi_partitions,
    required_resource_keys={"s3_metrics"}
)
def worker_summary_s3(context, worker_summary: Dict) -> None:
    """
    Export the comprehensive worker summary to a JSON file and upload to S3.
    """
    # Parse partition info from context
    node_type, region = parse_partition_key(context.partition_key)
    context.log.info(f"Exporting worker summary for partition: {node_type}/{region}")

    # Skip if there's no data
    if not worker_summary or not worker_summary.get("workers"):
        context.log.info("No worker summary data to export")
        return None

    # Generate a timestamp for file uniqueness
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")

    # Create base filename and versioned filename
    base_filename = f"worker_summary.json"

    # Get S3 resource
    s3 = context.resources.s3_metrics

    # Add timestamp to the exported data
    export_data = {
        "export_timestamp": timestamp,
        "data": worker_summary
    }

    # Convert data to JSON string
    json_data = json.dumps(export_data, indent=2)

    # Define S3 paths - use prefix with node_type and region
    prefix = s3.get_prefix_for_partition(region, node_type)
    s3_base_path = f"{prefix}{base_filename}"

    # Upload to S3
    try:
        context.log.info(f"Attempting to upload to bucket: {s3.bucket} using endpoint: {s3.config.endpoint_url}")

        # Upload the latest version
        s3.client().put_object(
            Bucket=s3.bucket,
            Key=s3_base_path,
            Body=json_data,
            ContentType="application/json"
        )
        # Log export success
        worker_count = len(worker_summary.get("workers", {}))
        active_worker_count = worker_summary.get("aggregates", {}).get("active_workers", 0)
        pending_eth = worker_summary.get("aggregates", {}).get("total_pending_eth", 0)

        context.log.info(
            f"Exported summary for {worker_count} workers ({active_worker_count} active) "
            f"with {pending_eth:.6f} ETH pending to S3: {s3_base_path}"
        )

        # Add metadata about the export
        context.add_output_metadata({
            "s3_bucket": MetadataValue.text(s3.bucket),
            "s3_base_path": MetadataValue.text(s3_base_path),
            "export_timestamp": MetadataValue.text(timestamp),
            "region": MetadataValue.text(region),
            "node_type": MetadataValue.text(node_type),
            "worker_count": MetadataValue.int(worker_count),
            "active_worker_count": MetadataValue.int(active_worker_count),
            "pending_eth": MetadataValue.float(pending_eth)
        })

    except Exception as e:
        context.log.error(f"Error uploading worker summary to S3: {str(e)}")
        import traceback
        context.log.error(f"Traceback: {traceback.format_exc()}")
        raise

    return None

@asset(
    key_prefix=["export"],
    compute_kind="json",
    group_name="export",
    description="Export worker performance analytics to S3",
    ins={"worker_performance": AssetIn(key=["worker_performance_analytics"])},
    partitions_def=multi_partitions,
    required_resource_keys={"s3_metrics"}
)
def worker_performance_s3(context, worker_performance: Dict) -> None:
    """
    Export the worker performance analytics to a JSON file and upload to S3.
    """
    # Parse partition info from context
    node_type, region = parse_partition_key(context.partition_key)
    context.log.info(f"Exporting worker performance for partition: {node_type}/{region}")

    # Skip if there's no data
    if not worker_performance:
        context.log.info("No worker performance data to export")
        return None

    # Skip if performance DataFrames are empty
    transcode_perf = worker_performance.get("transcode_performance")
    ai_perf = worker_performance.get("ai_performance")
    ai_rankings = worker_performance.get("ai_model_pipeline_rankings")

    if (transcode_perf is None or transcode_perf.empty) and \
       (ai_perf is None or ai_perf.empty) and \
       (not ai_rankings):
        context.log.info("No worker performance data to export")
        return None

    # Generate a timestamp for file uniqueness
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")

    # Create base filename and versioned filename
    base_filename = f"worker_performance.json"

    # Get S3 resource
    s3 = context.resources.s3_metrics

    # Prepare export data
    export_data = {
        "export_timestamp": timestamp,
        "node_type": node_type,
        "region": region,
        "transcode_performance": transcode_perf.to_dict(orient="records") if transcode_perf is not None and not transcode_perf.empty else [],
        "ai_performance": ai_perf.to_dict(orient="records") if ai_perf is not None and not ai_perf.empty else [],
        "ai_model_pipeline_rankings": {
            k: v.to_dict(orient="records") for k, v in ai_rankings.items()
        } if ai_rankings else {}
    }

    # Convert data to JSON string
    json_data = json.dumps(export_data, indent=2)

    # Define S3 paths - use prefix with node_type and region
    prefix = s3.get_prefix_for_partition(region, node_type)
    s3_base_path = f"{prefix}{base_filename}"

    # Upload to S3
    try:
        context.log.info(f"Attempting to upload to bucket: {s3.bucket} using endpoint: {s3.config.endpoint_url}")

        # Upload the latest version
        s3.client().put_object(
            Bucket=s3.bucket,
            Key=s3_base_path,
            Body=json_data,
            ContentType="application/json"
        )
        # Log export success
        transcode_worker_count = len(transcode_perf) if transcode_perf is not None and not transcode_perf.empty else 0
        ai_worker_count = len(ai_perf) if ai_perf is not None and not ai_perf.empty else 0
        ai_model_count = len(ai_rankings) if ai_rankings else 0

        context.log.info(
            f"Exported performance data for {transcode_worker_count} transcode workers, "
            f"{ai_worker_count} AI workers, across {ai_model_count} AI models/pipelines"
        )

        # Add metadata about the export
        context.add_output_metadata({
            "s3_bucket": MetadataValue.text(s3.bucket),
            "s3_base_path": MetadataValue.text(s3_base_path),
            "export_timestamp": MetadataValue.text(timestamp),
            "region": MetadataValue.text(region),
            "node_type": MetadataValue.text(node_type),
            "transcode_worker_count": MetadataValue.int(transcode_worker_count),
            "ai_worker_count": MetadataValue.int(ai_worker_count),
            "ai_model_count": MetadataValue.int(ai_model_count)
        })

    except Exception as e:
        context.log.error(f"Error uploading worker performance to S3: {str(e)}")
        import traceback
        context.log.error(f"Traceback: {traceback.format_exc()}")
        raise

    return None
from dagster import asset, AssetExecutionContext, AssetIn
from typing import Dict, List
from datetime import datetime

from openpool_management.models import RawEvent, parse_partition_key
from openpool_management.partitions import multi_partitions

@asset(
    key_prefix=["worker"],
    group_name="tracking",
    description="Track worker fee state including pool commission calculations",
    ins={"processed_jobs": AssetIn(key_prefix=["processed"])},
    partitions_def=multi_partitions,
    io_manager_key="io_manager",
    required_resource_keys={"pool"}
)
def worker_fees(context: AssetExecutionContext, processed_jobs: dict) -> dict:
    """
    Track worker fee state for the current batch of processed jobs.
    Maintains total fees, paid fees, and pool commission per worker.
    Returns both updated worker fee state and the updated worker states.
    """
    # Extract processed events from the dictionary
    processed_events = processed_jobs["processed_events"]

    # Parse partition key
    node_type, region = parse_partition_key(context.partition_key)
    context.log.info(f"Processing worker fees for partition: {node_type}/{region}")

    # Get pool commission rate from resource
    pool_commission_rate = context.resources.pool.get_commission_rate()

    # Filter for job-processed events
    job_events = [e for e in processed_events if e.event_type == "job-processed"]
    context.log.info(f"Found {len(job_events)} job processed events")

    # Try to load existing worker fee states from previous materialization
    existing_states = {}
    processed_event_ids = set()
    try:
        # Use Dagster's input context to load previous asset state
        from dagster import InputContext
        input_context = InputContext(
            asset_key=context.asset_key,
            partition_key=context.partition_key,
            metadata={}
        )
        # Load using the IO manager
        previous_output = context.resources.io_manager.load_input(input_context)
        # Get worker states or default to empty dict
        existing_states = previous_output.get("worker_states", {}) if previous_output else {}
        # Load previously processed event IDs for deduplication
        processed_event_ids = previous_output.get("_processed_event_ids", set()) if previous_output else set()
        if isinstance(processed_event_ids, list):
            processed_event_ids = set(processed_event_ids)
        context.log.info(f"Loaded {len(existing_states)} existing worker fee states via IO manager")
        context.log.info(f"Found {len(processed_event_ids)} previously processed event IDs")
    except Exception as e:
        context.log.warning(f"Could not load existing worker fee states: {str(e)}")
        context.log.warning(f"Exception details: {type(e).__name__}: {str(e)}")
        existing_states = {}

    # Deduplicate: filter out events we've already processed
    new_events = [e for e in job_events if e.id not in processed_event_ids]
    context.log.info(f"After deduplication: {len(new_events)} new events ({len(job_events) - len(new_events)} skipped)")

    # Track newly processed event IDs
    for event in new_events:
        processed_event_ids.add(event.id)

    # Create a new worker states dictionary
    worker_states = {}

    # First, initialize from existing workers to maintain previous state
    for eth_address, fee_state in existing_states.items():
        worker_states[eth_address] = {
            "eth_address": eth_address,
            "total_fees": fee_state.get("total_fees", 0),
            "pool_commission": fee_state.get("pool_commission", 0),
            "worker_earnings": fee_state.get("worker_earnings", 0),
        }

    # Process only new (deduplicated) job events to update fee state
    total_pool_commission = 0

    for event in new_events:
        # Extract ETH address and fees
        eth_address = event.get_eth_address()
        if not eth_address:
            context.log.warning(f"No ETH address found in job event: {event.id}")
            continue

        fees = event.payload.get("fees", 0)
        if fees <= 0:
            context.log.warning(f"No fees or invalid fees in job event: {event.id}")
            continue

        # Calculate pool commission and worker earnings
        pool_commission = int(fees * pool_commission_rate)
        worker_earnings = fees - pool_commission

        # Initialize worker state if not exists
        if eth_address not in worker_states:
            worker_states[eth_address] = {
                "eth_address": eth_address,
                "total_fees": 0,
                "pool_commission": 0,
                "worker_earnings": 0,
            }

        # Update worker state with job fees
        worker_states[eth_address]["total_fees"] += fees
        worker_states[eth_address]["pool_commission"] += pool_commission
        worker_states[eth_address]["worker_earnings"] += worker_earnings

        # Track total pool commission
        total_pool_commission += pool_commission

    # Calculate metrics for logging
    total_workers = len(worker_states)
    total_fees = sum(state["total_fees"] for state in worker_states.values())
    total_worker_earnings = sum(state["worker_earnings"] for state in worker_states.values())

    context.log.info(f"Processed fee data for {total_workers} workers")
    context.log.info(
        f"Total fees: {total_fees}, Pool commission: {total_pool_commission}, Worker earnings: {total_worker_earnings}")
    # Add per-worker fee details
    context.log.info(
        f"[{node_type}/{region}] Fee distribution: {total_fees / 1e18:.6f} ETH total "
        f"({total_pool_commission / 1e18:.6f} ETH commission, {total_worker_earnings / 1e18:.6f} ETH worker earnings)"
    )
    # Add metadata about what we tracked
    context.add_output_metadata({
        "node_type": node_type,
        "region": region,
        "worker_count": total_workers,
        "total_fees": total_fees,
        "total_pool_commission": total_pool_commission,
        "total_worker_earnings": total_worker_earnings,
        "pool_commission_rate": pool_commission_rate,
    })

    # Return data with both the worker states and supporting metadata
    return {
        "worker_states": worker_states,
        "_processed_event_ids": processed_event_ids,
        "metadata": {
            "node_type": node_type,
            "region": region,
            "timestamp": datetime.now().isoformat(),
            "total_workers": total_workers,
            "total_fees": total_fees,
            "total_pool_commission": total_pool_commission,
            "total_worker_earnings": total_worker_earnings,
        }
    }

@asset(
    key_prefix=["worker"],
    group_name="tracking",
    description="Track worker connection state",
    ins={"raw_events": AssetIn(key_prefix=["raw"])},
    partitions_def=multi_partitions,
    io_manager_key="io_manager"
)
def worker_connections(context: AssetExecutionContext, raw_events: List[RawEvent]) -> Dict:
    """
    Track worker connection state for the current batch of events.
    When a reset event is detected, this asset will completely clear all connections
    for the current partition before processing any new connection events.
    """
    # Parse partition key
    node_type, region = parse_partition_key(context.partition_key)
    context.log.info(f"Processing worker connections for partition: {node_type}/{region}")

    # Filter events
    connection_events = [e for e in raw_events if e.event_type == "worker-connected"]
    disconnection_events = [e for e in raw_events if e.event_type == "worker-disconnected"]
    reset_events = [e for e in raw_events if e.event_type == "orchestrator-reset"]

    # Sort events chronologically to ensure proper order of processing
    all_events = connection_events + disconnection_events + reset_events
    all_events.sort(key=lambda e: e.dt)

    # Log event counts
    context.log.info(
        f"Found {len(connection_events)} connection events, {len(disconnection_events)} disconnection events, "
        f"and {len(reset_events)} reset events"
    )

    # Try to load existing worker states from previous materialization
    existing_states = {}
    try:
        # CHANGED: Use Dagster's input context to load previous asset state
        from dagster import InputContext
        input_context = InputContext(
            asset_key=context.asset_key,
            partition_key=context.partition_key,
            metadata={}
        )
        # Load using the IO manager
        previous_output = context.resources.io_manager.load_input(input_context)
        # Get worker states or default to empty dict
        existing_states = previous_output.get("worker_states", {}) if previous_output else {}
        context.log.info(f"Loaded {len(existing_states)} existing worker connection states via IO manager")
    except Exception as e:
        context.log.warning(f"Could not load existing worker states: {str(e)}")
        context.log.warning(f"Exception details: {type(e).__name__}: {str(e)}")
        existing_states = {}

    # Create a clean worker states dictionary
    worker_states = {}

    # First check if there was a reset event - if so, start with a clean slate
    if reset_events:
        context.log.info(f"Orchestrator reset detected for {region} - starting with fresh worker state")
        # We don't need to do anything special here - just start with an empty dictionary
        # The existing worker state will be completely replaced
    else:
        # No reset, so we'll need to handle normal updates
        # Initialize from existing workers to maintain their ETH addresses even if no events
        for eth_address, worker_state in existing_states.items():
            # Only copy over workers that match our partition
            if worker_state.get("region") == region and worker_state.get("node_type") == node_type:
                # Create a fresh state object for each worker but with empty connections
                # Connections will be re-added based on the current batch of events
                worker_states[eth_address] = {
                    "eth_address": eth_address,
                    "node_type": node_type,
                    "region": region,
                    "connections": worker_state.get("connections", []).copy(),  # Copy existing connections
                    "active": bool(worker_state.get("connections", []))  # Set active based on connections
                }

    # Now process events in chronological order
    for event in all_events:
        # Skip events that don't belong to this partition
        if event.region != region:
            continue

        if event.event_type == "orchestrator-reset":
            # Reset detected - clear ALL worker states for this partition
            context.log.info(f"Processing reset event: clearing all connections for {region}")
            worker_states = {}  # Complete reset of all worker states

        elif event.event_type == "worker-connected":
            eth_address = event.get_eth_address()
            if not eth_address:
                context.log.warning(f"No ETH address found in connection event: {event.id}")
                continue

            # Get connection string from payload
            connection_string = event.payload.get("connection")
            if not connection_string:
                context.log.warning(f"No connection string in connection event for {eth_address}")
                continue

            # Initialize worker state if not exists
            if eth_address not in worker_states:
                worker_states[eth_address] = {
                    "eth_address": eth_address,
                    "node_type": node_type,
                    "region": region,
                    "connections": [],
                    "active": False
                }

            # Add connection if not already present
            if connection_string not in worker_states[eth_address]["connections"]:
                worker_states[eth_address]["connections"].append(connection_string)
                worker_states[eth_address]["active"] = True
                context.log.info(f"Added connection {connection_string} for worker {eth_address}")

        elif event.event_type == "worker-disconnected":
            eth_address = event.get_eth_address()
            if not eth_address:
                context.log.warning(f"No ETH address found in disconnection event: {event.id}")
                continue

            # Get connection string from payload
            connection_string = event.payload.get("connection")
            if not connection_string or eth_address not in worker_states:
                continue

            # Remove connection if present
            if connection_string in worker_states[eth_address]["connections"]:
                worker_states[eth_address]["connections"].remove(connection_string)
                worker_states[eth_address]["active"] = len(worker_states[eth_address]["connections"]) > 0
                context.log.info(f"Removed connection {connection_string} for worker {eth_address}")

    # Add helper methods as properties of each worker state
    for eth_address, worker in worker_states.items():
        worker["connection_count"] = len(worker["connections"])
        worker["is_active"] = len(worker["connections"]) > 0

    # Clean up - remove workers with no connections
    active_workers = {k: v for k, v in worker_states.items() if v["connections"]}
    removed_count = len(worker_states) - len(active_workers)
    if removed_count > 0:
        context.log.info(f"Removed {removed_count} workers with no active connections")

    worker_states = active_workers

    # Add metadata about what we found
    connection_count = sum(len(worker["connections"]) for worker in worker_states.values())
    context.log.info(f"Final state: {len(worker_states)} active workers with {connection_count} total connections")

    # At process completion, add detailed worker status
    active_pct = (len(active_workers) / len(worker_states) * 100) if worker_states else 0
    context.log.info(
        f"[{node_type}/{region}] Worker connection state: {len(active_workers)}/{len(worker_states)} active "
        f"({active_pct:.1f}%), {connection_count} total connections"
    )

    context.add_output_metadata({
        "node_type": node_type,
        "region": region,
        "worker_count": len(worker_states),
        "connection_count": connection_count,
        "connection_events": len(connection_events),
        "disconnection_events": len(disconnection_events),
        "reset_events": len(reset_events),
        "orchestrator_reset": len(reset_events) > 0
    })

    # Return both worker states and metadata
    return {
        "worker_states": worker_states,
        "metadata": {
            "node_type": node_type,
            "region": region,
            "timestamp": datetime.now().isoformat(),
            "worker_count": len(worker_states),
            "connection_count": connection_count,
            "reset_events": len(reset_events)
        }
    }
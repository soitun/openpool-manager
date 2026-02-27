# openpool_management/assets/events.py
from dagster import asset, AssetExecutionContext, MetadataValue, AssetIn
from typing import List
import json
from datetime import datetime
from openpool_management.models import RawEvent, parse_partition_key
from openpool_management.partitions import multi_partitions

# -------------------- Dagster Assets --------------------
@asset(
    key_prefix=["raw"],
    group_name="ingest",
    description="Ingest raw events from S3 bucket",
    partitions_def=multi_partitions,
    required_resource_keys={"s3"}
)
def raw_events(context: AssetExecutionContext) -> List[RawEvent]:
    """
    Fetch events from S3 bucket based on partition
    """
    s3 = context.resources.s3

    # Parse partition key
    node_type, region = parse_partition_key(context.partition_key)
    context.log.info(f"Processing partition: node_type={node_type}, region={region}")

    # Get file keys from config or find latest
    keys = []

    # Check if op_config exists (for sensor-triggered runs)
    if hasattr(context, 'op_config') and context.op_config is not None:
        keys = context.op_config.get("s3_keys", [])
        context.log.info(f"Found {len(keys)} keys in op_config")
    else:
        context.log.info("No keys provided, looking for latest files")

    if not keys:
        # Get the prefix for this partition
        prefix = s3.get_prefix_for_partition(region, node_type)
        context.log.info(f"Listing objects with prefix: {prefix}")

        # List objects in the bucket with this prefix
        objects = s3.list_objects(prefix)

        if not objects:
            context.log.warning(f"No objects found for prefix {prefix}")
            return []

        # Sort by last modified and get the most recent
        sorted_objects = sorted(objects, key=lambda x: x.get("LastModified", datetime.min))
        if sorted_objects:
            keys = [sorted_objects[-1]["Key"]]
            context.log.info(f"Found latest key: {keys[0]}")
        else:
            context.log.warning("No objects found to process")
            return []

    # Process events
    all_events = []
    processed_files = 0
    total_size = 0

    context.log.info(f"[{node_type}/{region}] Starting raw event processing, checking {len(keys)} files")

    for key in keys:
        try:
            # Get the file
            obj = s3.get_object(key)

            if not obj or "Body" not in obj:
                context.log.warning(f"Could not retrieve object for key {key}")
                continue

            # Read file content
            file_content = obj["Body"].read().decode("utf-8")
            file_size = obj.get("ContentLength", 0)
            total_size += file_size

            # Parse JSON
            json_data = json.loads(file_content)

            for item in json_data:
                try:
                    # Create RawEvent object
                    event = RawEvent.from_json(item)

                    # Filter events by region and node_type to ensure consistency
                    if event.region == region and event.node_type == node_type:
                        all_events.append(event)
                except Exception as e:
                    context.log.warning(f"Error processing event: {str(e)}")
                    continue

            processed_files += 1

        except Exception as e:
            context.log.error(f"Error processing file {key}: {str(e)}")

    # Calculate event type counts for metadata
    event_type_counts = {}
    for event in all_events:
        event_type = event.event_type
        if event_type not in event_type_counts:
            event_type_counts[event_type] = 0
        event_type_counts[event_type] += 1

    # Add metadata about what we found
    context.add_output_metadata({
        "num_events": len(all_events),
        "region": region,
        "node_type": node_type,
        "processed_files": processed_files,
        "total_data_size_bytes": total_size,
        "event_types": MetadataValue.json(event_type_counts),
        "s3_bucket": s3.bucket,
        "s3_keys": MetadataValue.json(keys[:10] + ['...'] if len(keys) > 10 else keys)
    })
    context.log.info(
        f"[{node_type}/{region}] Processed {processed_files} files ({total_size / 1024 / 1024:.2f} MB), "
        f"extracted {len(all_events)} events: " +
        f"{', '.join([f'{k}:{v}' for k, v in event_type_counts.items()])}"
    )
    return all_events


@asset(
    key_prefix=["processed"],
    group_name="transform",
    description="Track job events and correlate jobs based on request ID",
    ins={"raw_events": AssetIn(key_prefix=["raw"])},
    partitions_def=multi_partitions,
    io_manager_key="io_manager"
)
def processed_jobs(context: AssetExecutionContext, raw_events: List[RawEvent]) -> dict:
    """
    Track and correlate job events based on request ID.
    - For AI jobs: enriches processed events with ETH addresses from received events
    - For transcode jobs: passes through processed events (already have ETH addresses)

    Returns both processed events and pending jobs state.
    """
    # Parse partition key
    node_type, region = parse_partition_key(context.partition_key)
    context.log.info(f"Processing Jobs for partition: {region}/{node_type}")

    # Initialize list to store all enriched processed events
    enriched_processed_events = []

    # Load existing pending jobs from previous materialization
    pending_jobs = {}
    if node_type == "ai":
        try:
            prev_data = context.resources.io_manager.load_previous_output(context)
            if prev_data and isinstance(prev_data, dict):
                pending_jobs = prev_data.get("pending_jobs", {})
                context.log.info(f"Loaded {len(pending_jobs)} pending received jobs from previous materialization")
            else:
                context.log.info("No previous materialization found — starting fresh")
        except Exception as e:
            context.log.warning(f"Could not load pending jobs: {str(e)}")
            pending_jobs = {}

    # Handle each node type appropriately
    if node_type == "ai":
        # Process AI jobs (need correlation between received and processed events)
        received_events = [e for e in raw_events if e.event_type == "job-received"]
        processed_events = [e for e in raw_events if e.event_type == "job-processed"]

        context.log.info(f"Found {len(received_events)} received events and {len(processed_events)} processed events")

        # First process all new received events and add to pending jobs
        for event in received_events:
            request_id = event.get_correlation_key()
            if not request_id:
                context.log.warning(f"Received event missing request ID: {event.id}")
                continue

            # Extract ETH address
            eth_address = event.get_eth_address()
            if not eth_address:
                context.log.warning(f"No ETH address in received event: {event.id}")
                continue

            # Store in pending jobs
            pending_jobs[request_id] = {
                "ethAddress": eth_address,
                "requestID": request_id,
                "received_at": event.dt.isoformat(),
                **event.payload
            }

        # Process all processed events and try to match with received events
        processed_count = 0
        enriched_count = 0

        for event in processed_events:
            request_id = event.get_correlation_key()
            if not request_id:
                context.log.warning(f"Processed event missing request ID: {event.id}")
                continue

            processed_count += 1

            # If we have a matching received event, enrich processed event with ETH address
            if request_id in pending_jobs:
                eth_address = pending_jobs[request_id].get("ethAddress")

                if eth_address and "ethAddress" not in event.payload:
                    # Enrich processed event with ETH address
                    enriched_payload = {**event.payload, "ethAddress": eth_address}

                    # Create enriched event
                    enriched_event = RawEvent(
                        id=event.id,
                        dt=event.dt,
                        event_type=event.event_type,
                        node_type=event.node_type,
                        region=event.region,
                        payload=enriched_payload,
                        version=event.version
                    )

                    enriched_processed_events.append(enriched_event)
                    enriched_count += 1
                else:
                    # Just add the original event if it already has ethAddress
                    enriched_processed_events.append(event)

                # Remove from pending jobs now that it's been processed
                del pending_jobs[request_id]
            else:
                # No matching received event found
                context.log.info(f"No received event found for request ID: {request_id}")
                enriched_processed_events.append(event)

        context.log.info(
            f"[{node_type}/{region}] Job correlation stats: {enriched_count}/{processed_count} processed events matched "
            f"with requests ({(enriched_count / processed_count * 100) if processed_count else 0:.1f}%)"
        )

    elif node_type == "transcode":
        # For transcode jobs, we only need to track the processed events
        # ETH addresses are already included in the processed events payload
        processed_events = [e for e in raw_events if e.event_type == "job-processed"]
        context.log.info(f"Found {len(processed_events)} transcode processed events")

        # Just add all processed events to the output
        enriched_processed_events.extend(processed_events)
    else:
        context.log.warning(f"Unexpected node type: {node_type}")

    # Add metadata
    context.add_output_metadata({
        "processed_events": len(enriched_processed_events),
        "pending_received_jobs": len(pending_jobs) if node_type == "ai" else 0,
        "region": region,
        "node_type": node_type,
        "eth_addresses_added": sum(1 for e in enriched_processed_events
                                   if "ethAddress" in e.payload) if node_type == "ai" else 0
    })

    # Return both the processed events and the pending jobs state
    return {
        "processed_events": enriched_processed_events,
        "pending_jobs": pending_jobs
    }
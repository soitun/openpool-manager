from dagster import asset, AssetIn, MetadataValue
from datetime import datetime
import uuid
import os
import json
import tempfile
from openpool_management.models import parse_partition_key
from openpool_management.partitions import multi_partitions

# -------------------- WAL Helpers --------------------

WAL_DIR_TEMPLATE = "{base_dir}/wal/worker_payments/{node_type}/{region}"


def _get_wal_path(node_type, region):
    base_dir = os.environ.get("IO_MANAGER_BASE_DIR", "data")
    wal_dir = WAL_DIR_TEMPLATE.format(base_dir=base_dir, node_type=node_type, region=region)
    os.makedirs(wal_dir, exist_ok=True)
    return os.path.join(wal_dir, "payment_wal.jsonl")


def _write_wal_entry(wal_path, entry):
    """Append a WAL entry with fsync."""
    with open(wal_path, "a") as f:
        f.write(json.dumps(entry) + "\n")
        f.flush()
        os.fsync(f.fileno())


def _read_wal_entries(wal_path):
    """Read WAL, deduplicate by payment_id (last entry wins)."""
    if not os.path.exists(wal_path):
        return []
    by_id = {}
    with open(wal_path, "r") as f:
        for line in f:
            line = line.strip()
            if not line:
                continue
            try:
                entry = json.loads(line)
                by_id[entry["payment_id"]] = entry
            except (json.JSONDecodeError, KeyError):
                continue
    return list(by_id.values())


def _clear_wal(wal_path):
    """Atomically clear the WAL."""
    wal_dir = os.path.dirname(wal_path)
    tmp_fd, tmp_path = tempfile.mkstemp(dir=wal_dir, suffix=".tmp")
    os.close(tmp_fd)
    os.rename(tmp_path, wal_path)


def _reconcile_wal(wal_path, payment_states, context):
    """
    Reconcile WAL entries against current payment state.

    For each WAL entry:
    - completed: credit the payment if not already in history
    - pending: crash between WAL write and send_eth result — flag for investigation
    - failed: no action needed
    """
    entries = _read_wal_entries(wal_path)
    if not entries:
        return payment_states

    context.log.info(f"Reconciling {len(entries)} WAL entries")

    for entry in entries:
        status = entry.get("status")
        payment_id = entry.get("payment_id")
        eth_address = entry.get("eth_address")

        if status == "completed":
            # Check if this payment is already recorded in history
            state = payment_states.get(eth_address, {})
            existing_ids = {p.get("payment_id") for p in state.get("payment_history", [])}
            if payment_id not in existing_ids:
                context.log.warning(
                    f"WAL reconciliation: crediting completed payment {payment_id} "
                    f"for {eth_address} ({entry.get('amount_wei', 0) / 1e18:.6f} ETH)"
                )
                amount = entry.get("amount_wei", 0)
                if eth_address not in payment_states:
                    payment_states[eth_address] = {
                        "eth_address": eth_address,
                        "total_fees_earned": 0,
                        "total_fees_paid": 0,
                        "last_payment_timestamp": None,
                        "payment_history": []
                    }
                payment_states[eth_address]["total_fees_paid"] += amount
                payment_states[eth_address]["last_payment_timestamp"] = entry.get("timestamp")
                payment_states[eth_address].setdefault("payment_history", []).append({
                    "payment_id": payment_id,
                    "eth_address": eth_address,
                    "amount": amount,
                    "timestamp": entry.get("timestamp"),
                    "transaction_hash": entry.get("tx_hash"),
                    "status": "completed",
                    "source": "wal_reconciliation"
                })

        elif status == "pending":
            # Crash happened between WAL write and send_eth return.
            # We don't know if the ETH was sent. Flag for manual investigation.
            context.log.critical(
                f"WAL PENDING ENTRY: payment {payment_id} to {eth_address} for "
                f"{entry.get('amount_wei', 0) / 1e18:.6f} ETH — process crashed before "
                f"confirming send result. Worker will be SKIPPED until manually investigated."
            )
            if eth_address not in payment_states:
                payment_states[eth_address] = {
                    "eth_address": eth_address,
                    "total_fees_earned": 0,
                    "total_fees_paid": 0,
                    "last_payment_timestamp": None,
                    "payment_history": []
                }
            payment_states[eth_address]["_wal_needs_investigation"] = True

        # status == "failed" — no action needed

    return payment_states


# -------------------- Asset --------------------

@asset(
    key_prefix=["worker"],
    group_name="tracking",
    description="Track worker payment state and generate blockchain payments when threshold is reached",
    ins={"worker_fees": AssetIn(key_prefix=["worker"])},
    partitions_def=multi_partitions,
    io_manager_key="io_manager",
    required_resource_keys={"payment_processor"})
def worker_payments(context, worker_fees):
    """
    Track worker payment state and generate blockchain payments when threshold is reached.
    This asset maintains payment state and processes payments when workers reach the threshold.
    """
    # Extract worker fee states from the dictionary
    worker_fee_states = worker_fees["worker_states"]

    # Parse partition key
    node_type, region = parse_partition_key(context.partition_key)
    context.log.info(f"[{node_type}/{region}] Processing worker payments for {len(worker_fee_states)} workers")

    # Get the payment threshold from config
    payment_threshold_wei = context.resources.payment_processor.get_payment_threshold()

    context.log.info(
        f"[{node_type}/{region}] Payment threshold: {payment_threshold_wei / 1e18:.6f} ETH"
    )

    # Load existing payment states from previous materialization
    existing_payment_states = {}
    try:
        previous_output = context.resources.io_manager.load_previous_output(context)
        if previous_output:
            existing_payment_states = previous_output.get("payment_states", {})
            context.log.info(f"[{node_type}/{region}] Loaded {len(existing_payment_states)} existing payment states")
            if existing_payment_states:
                total_paid = sum(s.get("total_fees_paid", 0) for s in existing_payment_states.values())
                total_earned = sum(s.get("total_fees_earned", 0) for s in existing_payment_states.values())
                context.log.info(
                    f"[{node_type}/{region}] Prior state: {total_earned / 1e18:.6f} ETH earned, "
                    f"{total_paid / 1e18:.6f} ETH paid, {(total_earned - total_paid) / 1e18:.6f} ETH unpaid"
                )
        else:
            context.log.info(f"[{node_type}/{region}] No previous payment state found — starting fresh")
    except Exception as e:
        context.log.warning(f"[{node_type}/{region}] Could not load existing payment states: {str(e)}")
        existing_payment_states = {}

    # Initialize new payment states dictionary
    payment_states = {}

    # First, load existing payment states to maintain history
    for eth_address, state_dict in existing_payment_states.items():
        # Create a fresh payment state dictionary
        payment_states[eth_address] = {
            "eth_address": eth_address,
            "total_fees_earned": state_dict.get("total_fees_earned", 0),
            "total_fees_paid": state_dict.get("total_fees_paid", 0),
            "last_payment_timestamp": state_dict.get("last_payment_timestamp"),
            "payment_history": state_dict.get("payment_history", [])
        }

    # --- WAL Reconciliation ---
    wal_path = _get_wal_path(node_type, region)
    payment_states = _reconcile_wal(wal_path, payment_states, context)
    _clear_wal(wal_path)

    # Process worker fees to sync total earnings
    for eth_address, fee_info in worker_fee_states.items():
        worker_earnings = fee_info.get("worker_earnings", 0)

        # Debug worker earnings
        context.log.info(f"[{node_type}/{region}] Worker {eth_address}: {worker_earnings / 1e18:.6f} ETH earnings")

        # Use worker_earnings as total_fees_earned (post commission)
        # Create payment state if not exists
        if eth_address not in payment_states:
            context.log.info(f"[{node_type}/{region}] New worker detected: {eth_address}")
            payment_states[eth_address] = {
                "eth_address": eth_address,
                "total_fees_earned": worker_earnings,
                "total_fees_paid": 0,  # Initialize to zero if new worker
                "last_payment_timestamp": None,
                "payment_history": []
            }
        else:
            # Update total fees earned with latest worker_earnings
            old_earnings = payment_states[eth_address]["total_fees_earned"]
            payment_states[eth_address]["total_fees_earned"] = worker_earnings

            # Log change in earnings
            if worker_earnings != old_earnings:
                context.log.info(
                    f"[{node_type}/{region}] Worker {eth_address}: Updated earnings from "
                    f"{old_earnings / 1e18:.6f} ETH to {worker_earnings / 1e18:.6f} ETH "
                    f"(+{(worker_earnings - old_earnings) / 1e18:.6f} ETH)"
                )

    # Add unpaid_fees calculation to each payment state
    for eth_address, state in payment_states.items():
        state["unpaid_fees"] = state["total_fees_earned"] - state["total_fees_paid"]

        # Debug unpaid fees
        if state["unpaid_fees"] > 0:
            context.log.info(
                f"[{node_type}/{region}] Worker {eth_address}: {state['unpaid_fees'] / 1e18:.6f} ETH unpaid, "
                f"threshold: {payment_threshold_wei / 1e18:.6f} ETH"
            )

    # Track metrics for reporting
    workers_due_payment = []
    payment_events_generated = []
    total_payment_amount = 0

    # Initialize blockchain payment resource
    try:
        if hasattr(context.resources.payment_processor, 'setup'):
            context.resources.payment_processor.setup()
            context.log.info(f"[{node_type}/{region}] Blockchain payment processor initialized")
    except Exception as e:
        context.log.error(f"[{node_type}/{region}] Failed to initialize blockchain payment processor: {str(e)}")
        raise

    # Check each worker for payment eligibility
    for eth_address, state in payment_states.items():
        # Skip workers flagged for WAL investigation
        if state.get("_wal_needs_investigation"):
            context.log.warning(
                f"[{node_type}/{region}] SKIPPING worker {eth_address} — flagged for WAL investigation"
            )
            continue

        # Check if payment is due
        unpaid_fees = state["unpaid_fees"]
        if unpaid_fees >= payment_threshold_wei:
            context.log.info(
                f"[{node_type}/{region}] Worker {eth_address} eligible for payment: "
                f"{unpaid_fees / 1e18:.6f} ETH (threshold: {payment_threshold_wei / 1e18:.6f} ETH)"
            )
            workers_due_payment.append({
                "eth_address": eth_address,
                "unpaid_fees": unpaid_fees,
                "unpaid_eth": unpaid_fees / 1e18
            })

            # Track payment amount
            total_payment_amount += unpaid_fees

            # Generate a unique payment ID
            payment_id = f"pmt_{datetime.now().strftime('%Y%m%d')}_{uuid.uuid4().hex[:6]}"

            try:
                # 1. Write WAL "pending" entry BEFORE send
                _write_wal_entry(wal_path, {
                    "payment_id": payment_id,
                    "eth_address": eth_address,
                    "amount_wei": unpaid_fees,
                    "status": "pending",
                    "timestamp": datetime.now().isoformat()
                })

                # 2. Execute blockchain payment
                context.log.info(f"[{node_type}/{region}] Executing blockchain payment to {eth_address}...")

                tx_result = context.resources.payment_processor.send_eth(
                    amount_wei=unpaid_fees,
                    to_address=eth_address
                )

                # Extract transaction hash and status
                tx_hash = tx_result.get('transaction_hash')
                status = tx_result.get('status')

                # 3. Write WAL result entry AFTER send
                _write_wal_entry(wal_path, {
                    "payment_id": payment_id,
                    "eth_address": eth_address,
                    "amount_wei": unpaid_fees,
                    "status": status,
                    "tx_hash": tx_hash,
                    "timestamp": datetime.now().isoformat()
                })

                # Log transaction details
                context.log.info(
                    f"[{node_type}/{region}] Blockchain transaction details: "
                    f"Hash: {tx_hash}, Status: {status}"
                )

                # Create payment event with blockchain details
                payment_event = {
                    "payment_id": payment_id,
                    "eth_address": eth_address,
                    "amount": unpaid_fees,
                    "timestamp": datetime.now().isoformat(),
                    "transaction_hash": tx_hash,
                    "status": status,
                    "blockchain_details": tx_result
                }

                # Only update payment state if transaction was successful
                if status == 'completed':
                    # Record the payment in worker's state
                    if "payment_history" not in state:
                        state["payment_history"] = []

                    state["payment_history"].append(payment_event)
                    state["total_fees_paid"] += unpaid_fees
                    state["last_payment_timestamp"] = payment_event["timestamp"]
                    state["unpaid_fees"] = state["total_fees_earned"] - state["total_fees_paid"]

                    context.log.info(
                        f"[{node_type}/{region}] Payment completed {payment_id} for worker {eth_address}: "
                        f"{unpaid_fees / 1e18:.6f} ETH (TX: {tx_hash})"
                    )
                else:
                    context.log.error(
                        f"[{node_type}/{region}] Blockchain payment failed for {eth_address}: "
                        f"Status: {status}, Error: {tx_result.get('error', 'Unknown error')}"
                    )

                # Add to payment events regardless of status
                payment_events_generated.append(payment_event)

            except Exception as e:
                # Write WAL failure entry
                _write_wal_entry(wal_path, {
                    "payment_id": payment_id,
                    "eth_address": eth_address,
                    "amount_wei": unpaid_fees,
                    "status": "failed",
                    "error": str(e),
                    "timestamp": datetime.now().isoformat()
                })

                # Handle payment failure
                context.log.error(f"[{node_type}/{region}] Payment failed for {eth_address}: {str(e)}")
                context.log.error(f"Exception details: {type(e).__name__}: {str(e)}")
                import traceback
                context.log.error(f"Traceback: {traceback.format_exc()}")

                # Record failed payment attempt
                payment_event = {
                    "payment_id": payment_id,
                    "eth_address": eth_address,
                    "amount": unpaid_fees,
                    "timestamp": datetime.now().isoformat(),
                    "status": "failed",
                    "failure_reason": str(e)
                }

                payment_events_generated.append(payment_event)

    # Strip internal WAL investigation flags before persisting
    for state in payment_states.values():
        state.pop("_wal_needs_investigation", None)

    # Log payment summary
    if workers_due_payment:
        context.log.info(
            f"[{node_type}/{region}] Payment summary: {len(workers_due_payment)}/{len(payment_states)} workers "
            f"due payment, total: {total_payment_amount / 1e18:.6f} ETH"
        )
    else:
        context.log.info(
            f"[{node_type}/{region}] No workers eligible for payment at threshold {payment_threshold_wei / 1e18:.6f} ETH")

    # Export payment events to a JSON file if any were generated
    if payment_events_generated:
        # Create output directory if it doesn't exist
        os.makedirs("payments", exist_ok=True)

        # Generate timestamp for filename
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        filename = f"payments_{node_type}_{region}_{timestamp}.json"
        file_path = os.path.join("payments", filename)

        # Create payment data structure
        payment_data = {
            "timestamp": datetime.now().isoformat(),
            "node_type": node_type,
            "region": region,
            "payment_count": len(payment_events_generated),
            "total_payment_amount": total_payment_amount,
            "payment_threshold_wei": payment_threshold_wei,
            "payments": payment_events_generated
        }

        # Save to file
        with open(file_path, 'w') as f:
            json.dump(payment_data, f, indent=2)

        context.log.info(
            f"[{node_type}/{region}] Exported {len(payment_events_generated)} payment events to {file_path}")

    # Add metadata
    context.add_output_metadata({
        "node_type": MetadataValue.text(node_type),
        "region": MetadataValue.text(region),
        "total_workers": MetadataValue.int(len(payment_states)),
        "workers_due_payment": MetadataValue.int(len(workers_due_payment)),
        "payment_events_generated": MetadataValue.int(len(payment_events_generated)),
        "total_payment_amount": MetadataValue.int(total_payment_amount),
        "total_payment_amount_eth": MetadataValue.float(total_payment_amount / 1e18),
        "payment_threshold_wei": MetadataValue.int(payment_threshold_wei),
        "payment_threshold_eth": MetadataValue.float(payment_threshold_wei / 1e18),
        "workers_due_payment_details": MetadataValue.json(workers_due_payment)
    })

    # Return both payment states and payment events
    return {
        "payment_states": payment_states,
        "payment_events": payment_events_generated,
        "metadata": {
            "node_type": node_type,
            "region": region,
            "timestamp": datetime.now().isoformat(),
            "total_workers": len(payment_states),
            "workers_due_payment": len(workers_due_payment),
            "total_payment_amount": total_payment_amount,
            "payment_threshold_wei": payment_threshold_wei
        }
    }

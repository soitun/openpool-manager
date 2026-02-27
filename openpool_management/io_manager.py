# openpool_management/io_manager.py
import pickle
import logging
import os
import tempfile
from dagster import IOManager, io_manager

IO_MANAGER_BASE_DIR = os.environ.get("IO_MANAGER_BASE_DIR", "data")

class PartitionedFilesystemIOManager(IOManager):
    """Custom IO manager for partitioned assets with cleaner directory structure"""

    def __init__(self, base_dir=None):
        self.base_dir = base_dir or IO_MANAGER_BASE_DIR
        os.makedirs(self.base_dir, exist_ok=True)

    def _get_path(self, context):
        """Get path for the given output/input context with clean directory structure"""
        key_path = "/".join(context.asset_key.path)
        logging.info(f"IO Manager handling asset: {key_path}")

        if context.has_partition_key:
            logging.info(f"Partition key: {context.partition_key}, type: {type(context.partition_key)}")

            # Parse the partition key to get node_type and region
            node_type, region = None, None

            # Handle tuple-like partitions
            if isinstance(context.partition_key, (list, tuple)) and len(context.partition_key) == 2:
                node_type, region = context.partition_key
            # Handle string partitions with pipe delimiter
            elif isinstance(context.partition_key, str) and "|" in context.partition_key:
                parts = context.partition_key.split("|")
                if len(parts) == 2:
                    node_type, region = parts

            # Create a clean path without key=value format
            if node_type and region:
                # Simple directory segments: node_type/region
                partition_path = f"{node_type}/{region}"
                logging.info(f"Created clean path: {partition_path}")
            else:
                # Fallback for unexpected partition format
                safe_partition = str(context.partition_key).replace('|', '_').replace('/', '_')
                partition_path = f"partition/{safe_partition}"
                logging.info(f"Using fallback partition path: {partition_path}")

            full_path = os.path.join(self.base_dir, key_path, partition_path, "data.pkl")
            logging.info(f"Full path: {full_path}")
            return full_path
        else:
            # For non-partitioned assets
            full_path = os.path.join(self.base_dir, key_path, "data.pkl")
            logging.info(f"Non-partitioned path: {full_path}")
            return full_path

    def _get_empty_fallback(self, context):
        """Return an appropriate empty fallback value based on asset type."""
        asset_key_str = "/".join(context.asset_key.path)
        logging.warning(f"Returning empty fallback for asset: {asset_key_str}")

        if "worker_fees" in asset_key_str:
            return {"worker_states": {}, "metadata": {}}
        elif "worker_connections" in asset_key_str:
            return {"worker_states": {}, "metadata": {}}
        elif "worker_payments" in asset_key_str:
            return {"payment_states": {}, "payment_events": [], "metadata": {}}
        elif "processed_jobs" in asset_key_str:
            return {"processed_events": [], "pending_jobs": {}}
        elif "worker_performance_analytics" in asset_key_str:
            return {
                "transcode_performance": None,
                "ai_performance": None,
                "ai_model_pipeline_rankings": {},
                "_high_water_mark": None,
                "_recent_event_ids": set()
            }
        elif "worker_summary_analytics" in asset_key_str:
            return {"workers": {}, "aggregates": {"total_workers": 0, "active_workers": 0, "total_fees": 0}}
        else:
            return {}

    def load_for_sensor(self, asset_key_path, partition_key):
        """Load persisted data for a given asset and partition, for use in sensors.

        Unlike load_input/load_previous_output, this does not require any Dagster
        context — just the asset key path (e.g. ["worker", "worker_payments"]) and
        the partition key string (e.g. "ai|us-central").
        """
        key_path = "/".join(asset_key_path)

        # Parse partition key
        node_type, region = None, None
        if isinstance(partition_key, str) and "|" in partition_key:
            parts = partition_key.split("|")
            if len(parts) == 2:
                node_type, region = parts

        if node_type and region:
            partition_path = f"{node_type}/{region}"
        else:
            safe_partition = str(partition_key).replace('|', '_').replace('/', '_')
            partition_path = f"partition/{safe_partition}"

        path = os.path.join(self.base_dir, key_path, partition_path, "data.pkl")

        if not os.path.exists(path):
            return None

        try:
            with open(path, "rb") as f:
                return pickle.load(f)
        except Exception as e:
            logging.error(f"Error loading {path}: {e}")
            bak_path = path + ".bak"
            if os.path.exists(bak_path):
                try:
                    with open(bak_path, "rb") as f:
                        logging.info(f"Recovered from backup: {bak_path}")
                        return pickle.load(f)
                except Exception:
                    logging.error(f"Backup also corrupt: {bak_path}")
            return None

    def load_previous_output(self, context):
        """Load the previous output for this asset/partition directly from disk.

        Unlike load_input, this accepts an AssetExecutionContext directly,
        avoiding the need to construct a fragile InputContext. Returns None
        if no previous output exists.
        """
        path = self._get_path(context)

        if not os.path.exists(path):
            return None

        try:
            with open(path, "rb") as f:
                return pickle.load(f)
        except Exception as e:
            logging.error(f"Error loading previous output from {path}: {e}")
            bak_path = path + ".bak"
            if os.path.exists(bak_path):
                try:
                    with open(bak_path, "rb") as f:
                        logging.info(f"Recovered from backup: {bak_path}")
                        return pickle.load(f)
                except Exception:
                    logging.error(f"Backup also corrupt: {bak_path}")
            return None

    def handle_output(self, context, obj):
        """Save object to file using atomic write (temp file + fsync + rename)."""
        logging.info(f"Handling output for {context.asset_key}")
        path = self._get_path(context)
        dir_path = os.path.dirname(path)
        os.makedirs(dir_path, exist_ok=True)

        tmp_fd, tmp_path = tempfile.mkstemp(dir=dir_path, suffix=".pkl.tmp")
        try:
            with os.fdopen(tmp_fd, "wb") as f:
                pickle.dump(obj, f)
                f.flush()
                os.fsync(f.fileno())

            # Backup existing file before replacing
            bak_path = path + ".bak"
            if os.path.exists(path):
                os.replace(path, bak_path)

            os.rename(tmp_path, path)
            logging.info(f"Successfully saved data to {path}")
        except Exception:
            if os.path.exists(tmp_path):
                os.unlink(tmp_path)
            raise

    def load_input(self, context):
        """Load object from file with .bak recovery and graceful fallback."""
        logging.info(f"Loading input for {context.asset_key}")
        path = self._get_path(context)

        if not os.path.exists(path):
            return self._get_empty_fallback(context)

        try:
            with open(path, "rb") as f:
                return pickle.load(f)
        except Exception as e:
            logging.error(f"Error loading {path}: {e}")
            bak_path = path + ".bak"
            if os.path.exists(bak_path):
                try:
                    with open(bak_path, "rb") as f:
                        logging.info(f"Recovered from backup: {bak_path}")
                        return pickle.load(f)
                except Exception:
                    logging.error(f"Backup also corrupt: {bak_path}")
            return self._get_empty_fallback(context)


@io_manager
def partitioned_filesystem_io_manager():
    """IO manager factory function"""
    return PartitionedFilesystemIOManager()

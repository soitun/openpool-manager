# openpool_management/io_manager.py
import pickle
import logging
import os
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

    def handle_output(self, context, obj):
        """Save object to file"""
        logging.info(f"Handling output for {context.asset_key}")
        path = self._get_path(context)
        os.makedirs(os.path.dirname(path), exist_ok=True)

        # Ensure the directory exists
        os.makedirs(os.path.dirname(path), exist_ok=True)

        try:
            with open(path, "wb") as f:
                pickle.dump(obj, f)
            logging.info(f"Successfully saved data to {path}")
        except Exception as e:
            logging.error(f"Error saving data to {path}: {str(e)}")
            raise

    def load_input(self, context):
        """Load object from file with graceful fallback"""
        logging.info(f"Loading input for {context.asset_key}")
        path = self._get_path(context)

        # Modified to return empty dict instead of raising error if file doesn't exist
        if not os.path.exists(path):
            logging.warning(f"Input not found at path: {path}, returning empty object")

            # Determine appropriate empty value based on asset name/key
            asset_key_str = str(context.asset_key)

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
                    "transcode_performance": pd.DataFrame(),
                    "ai_performance": pd.DataFrame(),
                    "ai_model_pipeline_rankings": {}
                }
            elif "worker_summary_analytics" in asset_key_str:
                return {
                    "workers": {},
                    "aggregates": {
                        "total_workers": 0,
                        "active_workers": 0,
                        "total_fees": 0
                    }
                }
            else:
                # Generic fallback - empty dict
                return {}

        try:
            with open(path, "rb") as f:
                return pickle.load(f)
        except Exception as e:
            logging.error(f"Error loading input from {path}: {str(e)}")
            logging.error(f"Exception details: {type(e).__name__}: {str(e)}")

            # Return appropriate empty fallback
            asset_key_str = str(context.asset_key)
            if "worker_fees" in asset_key_str:
                return {"worker_states": {}, "metadata": {}}
            elif "worker_connections" in asset_key_str:
                return {"worker_states": {}, "metadata": {}}
            # ... other fallbacks as above ...
            else:
                return {}


@io_manager
def partitioned_filesystem_io_manager():
    """IO manager factory function"""
    return PartitionedFilesystemIOManager()
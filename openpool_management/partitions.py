"""
"""
from dagster import MultiPartitionsDefinition,StaticPartitionsDefinition

# -------------------- Dagster Partitions --------------------
# Static partitions for regions
region_partitions = StaticPartitionsDefinition(["us-central", "us-west","eu-central","oceania"])
# Static partitions for node types
node_type_partitions = StaticPartitionsDefinition(["ai", "transcode"])
# Combine region, node type
multi_partitions = MultiPartitionsDefinition({
    "region": region_partitions,
    "node_type": node_type_partitions
})
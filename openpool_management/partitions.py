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

# Active partition combinations — the grid has 8 cells (4 regions x 2 node types)
# but only these 5 are deployed. AI is only active in us-central;
# transcode runs in all 4 regions.
VALID_COMBINATIONS = [
    ("ai", "us-central"),
    ("transcode", "us-central"),
    ("transcode", "us-west"),
    ("transcode", "eu-central"),
    ("transcode", "oceania"),
]
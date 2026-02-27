from typing import Optional
from datetime import datetime
from pydantic import BaseModel

# -------------------- Domain Models --------------------
class RawEvent(BaseModel):
    """
    Represents a raw event from the S3 event logs
    """
    id: str
    dt: datetime
    event_type: str
    node_type: str
    region: str
    payload: dict
    version: int

    @classmethod
    def from_json(cls, json_data: dict) -> "RawEvent":
        """Create a RawEvent from JSON data"""
        return cls(
            id=json_data["ID"],
            dt=datetime.fromisoformat(json_data["DT"].replace("Z", "+00:00")),
            event_type=json_data["EventType"],
            node_type=json_data["NodeType"],
            region=json_data["Region"],
            payload=json_data["Payload"],
            version=json_data["Version"]
        )

    def to_dict(self) -> dict:
        """Convert to a dictionary for JSON serialization"""
        return {
            "id": self.id,
            "dt": self.dt.isoformat(),
            "event_type": self.event_type,
            "node_type": self.node_type,
            "region": self.region,
            "payload": self.payload,
            "version": self.version
        }

    @classmethod
    def from_dict(cls, data: dict) -> "RawEvent":
        """Create a RawEvent from a serialized dictionary"""
        return cls(
            id=data["id"],
            dt=datetime.fromisoformat(data["dt"]),
            event_type=data["event_type"],
            node_type=data["node_type"],
            region=data["region"],
            payload=data["payload"],
            version=data["version"]
        )

    def get_correlation_key(self) -> Optional[str]:
        """
        Extract a correlation key (request ID) if present in the payload
        """
        if self.event_type == "job-received" or self.event_type == "job-processed":
            return self.payload.get("requestID")
        return None

    def get_eth_address(self) -> Optional[str]:
        """Get the ethereum address if present"""
        return self.payload.get("ethAddress")

def parse_partition_key(partition_key):
    """Parse a multi-dimensional partition key into node_type and region."""
    if "|" in partition_key:
        parts = partition_key.split("|")
        if len(parts) == 2:
            return parts[0], parts[1]
    return None, None

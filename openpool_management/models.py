from typing import Dict, List, Optional
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

class PaymentRecord(BaseModel):
    """Record of a payment made to a worker"""
    timestamp: datetime
    payment_id: str
    amount: int
    status: str = "completed"  # completed, pending, failed
    transaction_hash: Optional[str] = None

class FeeRecord(BaseModel):
    """Individual fee record for a job"""
    timestamp: datetime
    job_id: str
    fees: int
    node_type: str

class WorkerFeeState(BaseModel):
    """
    Tracks the fee state of a worker in a specific region and node type
    """
    eth_address: str
    total_fees: int = 0
    job_count: int = 0
    last_updated: Optional[datetime] = None

    def get_stats(self) -> dict:
        """Get worker fee statistics"""
        return {
            "eth_address": self.eth_address,
            "total_fees": self.total_fees,
            "job_count": self.job_count,
            "last_updated": self.last_updated.isoformat() if self.last_updated else None,
        }

class WorkerState(BaseModel):
    """
    Tracks the state of a worker across regions
    """
    eth_address: str
    node_type: str
    region: str
    connections: List[str] = []  # Array of connection strings

    def add_connection(self, connection_string: str):
        """Add a connection string to the connections list"""
        if connection_string not in self.connections:
            self.connections.append(connection_string)

    def remove_connection(self, connection_string: str):
        """Remove a connection string from the connections list if present"""
        if connection_string in self.connections:
            self.connections.remove(connection_string)

    def is_active(self) -> bool:
        """Check if worker has any active connections"""
        return len(self.connections) > 0

    def get_connection_count(self) -> int:
        """Get the number of active connections"""
        return len(self.connections)

class WorkerPaymentState(BaseModel):
    """
    Tracks the payment state for a worker
    """
    eth_address: str
    total_fees_earned: int = 0  # In Wei
    total_fees_paid: int = 0  # In Wei
    last_payment_timestamp: Optional[datetime] = None
    payment_history: List[dict] = []  # List of previous payments

    @property
    def unpaid_fees(self) -> int:
        """Calculate unpaid fees"""
        return self.total_fees_earned - self.total_fees_paid

    def add_fees(self, amount: int):
        """Add fees to the worker's total earned"""
        self.total_fees_earned += amount

    def record_payment(self, payment_id: str, amount: int, timestamp: datetime):
        """Record a payment to the worker"""
        self.total_fees_paid += amount
        self.last_payment_timestamp = timestamp

        self.payment_history.append({
            "payment_id": payment_id,
            "amount": amount,
            "timestamp": timestamp.isoformat(),
        })

    def payment_due(self, threshold: int) -> bool:
        """Check if payment is due based on threshold"""
        return self.unpaid_fees >= threshold

    def to_dict(self) -> dict:
        """Convert to a dictionary for persistence"""
        return {
            "eth_address": self.eth_address,
            "total_fees_earned": self.total_fees_earned,
            "total_fees_paid": self.total_fees_paid,
            "unpaid_fees": self.unpaid_fees,
            "last_payment": self.last_payment_timestamp.isoformat() if self.last_payment_timestamp else None,
            "payment_history": self.payment_history,
            "payment_due": self.payment_due(1_000_000_000_000_000_000),  # 1 ETH in Wei as default
        }


class PaymentEvent(BaseModel):
    """
    Represents a payment made to a worker
    """
    payment_id: str
    eth_address: str
    amount: int  # In Wei
    timestamp: datetime
    transaction_hash: Optional[str] = None
    status: str = "pending"  # pending, completed, failed
    failure_reason: Optional[str] = None

    def mark_as_completed(self, tx_hash: str):
        """Mark the payment as completed with a transaction hash"""
        self.transaction_hash = tx_hash
        self.status = "completed"

    def mark_as_failed(self, reason: str):
        """Mark the payment as failed with a reason"""
        self.status = "failed"
        self.failure_reason = reason

    def to_dict(self) -> dict:
        """Convert to a dictionary for persistence"""
        return {
            "payment_id": self.payment_id,
            "eth_address": self.eth_address,
            "amount": self.amount,
            "timestamp": self.timestamp.isoformat(),
            "transaction_hash": self.transaction_hash,
            "status": self.status,
            "failure_reason": self.failure_reason,
        }

def parse_partition_key(partition_key):
    """Parse a multi-dimensional partition key into node_type and region."""
    if "|" in partition_key:
        parts = partition_key.split("|")
        if len(parts) == 2:
            return parts[0], parts[1]
    return None, None

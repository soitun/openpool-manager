"""
"""
from dagster import Config,EnvVar
from pydantic import Field
from typing import Optional,List

# -------------------- Configuration --------------------

class WorkerPaymentConfig(Config):
    """Configuration for payments"""
    payment_threshold_wei: int = Field(
        default_factory=lambda: int(EnvVar("PAYMENT_THRESHOLD_WEI").get_value("10000000000000000"))  # 0.1 ETH in Wei
    )
    dry_run: bool = Field(
        default_factory=lambda: EnvVar("PAYMENT_PROCESSING_DRY_RUN").get_value("false")
    )

class S3Config(Config):
    """Configuration for S3 resource access"""
    bucket: str = "raw-events"  # Default bucket name
    endpoint_url: str = "https://obj-store-console.xode.app/"  # S3 endpoint
    aws_access_key_id: Optional[str] = None
    aws_secret_access_key: Optional[str] = None
    aws_region: str = "us-east-1"  # Default region for S3 client
    path_style: bool = True  # Use path-style addressing

class RawEventsConfig(Config):
    """Configuration for raw events asset"""
    s3_keys: Optional[List[str]] = None

class PaymentConfig(Config):
    """Configuration for payment processing"""
    payment_threshold_wei: int = Field(
        default_factory=lambda: int(EnvVar("PAYMENT_THRESHOLD_WEI").get_value("10000000000000000"))  # 0.01 ETH in Wei
    )
    min_payment_interval_seconds: int = Field(
        default_factory=lambda: int(EnvVar("MIN_PAYMENT_INTERVAL_SECONDS").get_value("86400"))  # 24 hours in seconds
    )
    dry_run: bool = Field(
        default_factory=lambda: EnvVar("PAYMENT_PROCESSING_DRY_RUN").get_value("true").lower() == "true"
    )

class PoolConfig(Config):
    """Configuration for pool fee structure"""
    commission_rate: float = Field(
        default_factory=lambda: float(EnvVar("POOL_COMMISSION_RATE").get_value("0.25"))  # 25% pool commission
    )

class IOManagerConfig(Config):
    """Configuration for IO manager"""
    base_dir: str = Field(
        default_factory=lambda: EnvVar("IO_MANAGER_BASE_DIR").get_value("data")
    )
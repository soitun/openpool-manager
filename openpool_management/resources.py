import boto3
from typing import Optional, Dict, Any, List
from dagster import ConfigurableResource
from openpool_management.configs import S3Config, PaymentConfig, PoolConfig


class S3Resource(ConfigurableResource):
    """
    A Dagster resource for interacting with S3-compatible object storage.

    This resource provides methods for listing and retrieving objects based on
    the OpenPool region/node_type directory structure.
    """
    config: S3Config

    @property
    def bucket(self) -> str:
        return self.config.bucket

    def get_prefix_for_partition(self, region: str, node_type: str) -> str:
        """Construct S3 prefix for a specific region/node_type partition."""
        # Adjust the prefix to include the bucket in the path if needed
        return f"{region}/{node_type}/"

    def client(self):
        """
        Returns a configured boto3 S3 client.
        Ensures proper configuration for S3-compatible storage.
        """
        # Create the S3 client with proper configuration
        addressing_style = 'path' if self.config.path_style else 'virtual'

        return boto3.client(
            "s3",
            region_name=self.config.aws_region,
            endpoint_url=self.config.endpoint_url,
            aws_access_key_id=self.config.aws_access_key_id,
            aws_secret_access_key=self.config.aws_secret_access_key,
            # Add these config parameters for S3-compatible storage
            config=boto3.session.Config(
                signature_version='s3v4',
                s3={'addressing_style': addressing_style}
            )
        )

    def get_paginator(self, operation_name):
        """
        Returns a paginator for the specified S3 operation.
        """
        client = self.client()
        return client.get_paginator(operation_name)

    def list_objects(self, prefix: str) -> List[Dict[str, Any]]:
        """
        List objects in the bucket with the given prefix.
        Returns a list of objects or empty list if none found.
        """
        try:
            client = self.client()

            # Construct the full path including bucket if needed
            full_path = f"{self.bucket}/{prefix}" if self.bucket not in prefix else prefix

            # When using certain S3-compatible storage, we might need to adjust how
            # we call list_objects_v2
            paginator = client.get_paginator('list_objects_v2')
            page_iterator = paginator.paginate(Bucket=self.bucket, Prefix=prefix)

            all_objects = []
            for page in page_iterator:
                if "Contents" in page:
                    all_objects.extend(page["Contents"])

            return all_objects
        except Exception as e:
            # Log error and return empty list
            print(f"Error listing objects with prefix {prefix}: {e}")
            return []

    def get_object(self, key: str) -> Optional[Dict[str, Any]]:
        """
        Get an object from the bucket.
        Returns the object or None if not found.
        """
        try:
            client = self.client()
            response = client.get_object(Bucket=self.bucket, Key=key)
            return response
        except Exception as e:
            print(f"Error getting object {key}: {e}")
            return None

class PaymentResource(ConfigurableResource):
    """Resource for handling payments"""
    config: PaymentConfig

    def get_payment_threshold(self) -> int:
        """Get the current payment threshold in Wei"""
        return self.config.payment_threshold_wei

    def should_process_payment(self, unpaid_amount: int) -> bool:
        """Determine if a payment should be processed based on threshold"""
        return unpaid_amount >= self.config.payment_threshold_wei

class PoolResource(ConfigurableResource):
    """Resource for pool configuration"""
    config: PoolConfig

    def get_commission_rate(self, worker_address: str = None) -> float:
        """Get the commission rate, potentially customized per worker"""
        # Could implement custom logic per worker if commission_variable is True
        return self.config.commission_rate
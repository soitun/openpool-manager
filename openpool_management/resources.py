import boto3
from typing import Optional, Dict, Any, List
from dagster import ConfigurableResource
from pydantic import PrivateAttr
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


class Web3Client:
    """Stateful client for Web3 interactions with support for keystore files"""

    def __init__(self, config):
        self.config = config
        self.w3 = None
        self.account = None

    def setup(self):
        """Initialize the web3 connection and account"""
        from web3 import Web3
        import json
        import os

        # Initialize Web3 connection
        self.w3 = Web3(Web3.HTTPProvider(self.config.rpc_endpoint))

        # Add middleware for POA chains if needed
        try:
            # First try the new way (web3.py >= 6.0.0)
            from web3.middleware import geth_poa
            self.w3.middleware_onion.inject(geth_poa.geth_poa_middleware, layer=0)
        except ImportError:
            try:
                # Try the old way for backwards compatibility
                from web3.middleware import geth_poa_middleware
                self.w3.middleware_onion.inject(geth_poa_middleware, layer=0)
            except ImportError:
                # If middleware not available, log a warning but continue
                print("Warning: POA middleware not available in this web3.py version")

        # Check connection
        if not self.w3.is_connected():
            raise Exception(f"Failed to connect to Ethereum node at {self.config.rpc_endpoint}")

        # Process private key - could be a raw key or a keystore file path
        private_key = self.config.private_key

        # Check if private_key is a path to a keystore file
        if private_key and (private_key.endswith('.json') or os.path.isfile(private_key)):
            print(f"Loading keystore file from: {private_key}")

            try:
                # Try to load as a keystore file
                with open(private_key, 'r') as keyfile:
                    keystore = json.load(keyfile)

                # Get password from config or file
                password = self.config.keystore_password

                # If password is a file path, read it
                if password and (os.path.isfile(password)):
                    with open(password, 'r') as password_file:
                        password = password_file.read().strip()

                # Decrypt the keystore using the password
                private_key = self.w3.eth.account.decrypt(keystore, password)
                print(f"Successfully decrypted keystore")
            except Exception as e:
                raise Exception(f"Failed to load keystore file: {str(e)}")
        elif not private_key:
            raise Exception("No private key or keystore file provided")

        # Set up account from private key
        self.account = self.w3.eth.account.from_key(private_key)

        # Log account address
        print(f"Payment account initialized: {self.account.address}")

        # Check account balance
        balance = self.w3.eth.get_balance(self.account.address)
        print(f"Account balance: {Web3.from_wei(balance, 'ether')} ETH")

        return self

    def send_eth(self, amount_wei, to_address):
        """Send ETH to a worker address"""
        from web3 import Web3

        # Ensure Web3 is initialized
        if not self.w3:
            self.setup()

        # Validate address
        if not self.w3.is_address(to_address):
            raise ValueError(f"Invalid Ethereum address: {to_address}")

        # Check sender balance
        sender_balance = self.w3.eth.get_balance(self.account.address)
        if sender_balance < amount_wei:
            raise ValueError(
                f"Insufficient balance: {Web3.from_wei(sender_balance, 'ether')} ETH, "
                f"needed: {Web3.from_wei(amount_wei, 'ether')} ETH"
            )

        # Get nonce for the transaction
        nonce = self.w3.eth.get_transaction_count(self.account.address, 'pending')

        # Get current gas price
        gas_price = self.w3.eth.gas_price

        # Check against gas price threshold
        if gas_price > self.config.max_gas_price_wei:
            raise ValueError(
                f"Gas price too high: {Web3.from_wei(gas_price, 'gwei')} gwei, "
                f"maximum: {Web3.from_wei(self.config.max_gas_price_wei, 'gwei')} gwei"
            )

        # Prepare transaction
        tx = {
            'nonce': nonce,
            'to': to_address,
            'value': amount_wei,
            'gas': self.config.gas_limit,
            'gasPrice': gas_price,
            'chainId': self.config.chain_id
        }

        # Sign transaction - using the private key we got during setup
        if hasattr(self.account, '_private_key'):
            private_key = self.account._private_key
        else:
            # Fall back to the original key from config
            private_key = self.config.private_key

        # Use the properly accessed private key for signing
        signed_tx = self.w3.eth.account.sign_transaction(tx, private_key)

        # Handle different attribute names in different web3.py versions
        if hasattr(signed_tx, 'rawTransaction'):
            raw_tx = signed_tx.rawTransaction
        elif hasattr(signed_tx, 'raw_transaction'):
            raw_tx = signed_tx.raw_transaction
        else:
            raise AttributeError("Cannot find raw transaction data on signed transaction")

        # Send transaction
        tx_hash = self.w3.eth.send_raw_transaction(raw_tx)
        tx_hash_hex = tx_hash.hex()

        # Wait for transaction receipt
        try:
            receipt = self.w3.eth.wait_for_transaction_receipt(tx_hash, timeout=60)

            # Check transaction status
            if receipt.status != 1:
                raise Exception(f"Transaction {tx_hash_hex} failed with status: {receipt.status}")

            # Return transaction details
            return {
                'transaction_hash': tx_hash_hex,
                'from': self.account.address,
                'to': to_address,
                'amount': amount_wei,
                'amount_eth': float(Web3.from_wei(amount_wei, 'ether')),
                'gas_used': receipt.gasUsed,
                'gas_price': gas_price,
                'block_number': receipt.blockNumber,
                'status': 'completed'
            }

        except Exception as e:
            return {
                'transaction_hash': tx_hash_hex,
                'from': self.account.address,
                'to': to_address,
                'amount': amount_wei,
                'amount_eth': float(Web3.from_wei(amount_wei, 'ether')),
                'status': 'error',
                'error': str(e)
            }

class PaymentResource(ConfigurableResource):
    """Resource for handling blockchain payments"""
    config: PaymentConfig
    _client: Optional[Web3Client] = PrivateAttr(default=None)

    def get_client(self):
        """Get or create a Web3Client instance (cached for reuse)"""
        if self._client is None:
            self._client = Web3Client(self.config)
        return self._client

    def get_payment_threshold(self) -> int:
        """Get the current payment threshold in Wei"""
        return self.config.payment_threshold_wei

    def should_process_payment(self, unpaid_amount: int) -> bool:
        """Determine if a payment should be processed based on threshold"""
        return unpaid_amount >= self.config.payment_threshold_wei

    def setup(self):
        """Initialize the web3 connection and account via client"""
        client = self.get_client()
        client.setup()
        return client

    def send_eth(self, amount_wei, to_address):
        """Send ETH to a worker address via cached client"""
        client = self.get_client()
        if not client.w3:
            client.setup()
        return client.send_eth(amount_wei, to_address)

class PoolResource(ConfigurableResource):
    """Resource for pool configuration"""
    config: PoolConfig

    def get_commission_rate(self, worker_address: str = None) -> float:
        """Get the commission rate, potentially customized per worker"""
        # Could implement custom logic per worker if commission_variable is True
        return self.config.commission_rate
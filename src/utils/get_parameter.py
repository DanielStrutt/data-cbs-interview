"""
Utility for fetching a single parameter from AWS SSM Parameter Store.

All parameters are resolved under the /data-cbs-interview/ namespace.
SecureString parameters are decrypted automatically via the AWS-managed key (aws/ssm).

Usage Examples:
    from src.utils.get_parameter import get_parameter

    api_key = get_parameter("alpha_vantage_api_key")
    bucket  = get_parameter("s3_bucket")
"""

import logging
import os

import boto3
from botocore.exceptions import ClientError

logger = logging.getLogger(__name__)

SSM_NAMESPACE = "/data-cbs-interview"
_DEFAULT_REGION = "eu-west-2"


def get_parameter(name: str, region: str = _DEFAULT_REGION, required: bool = True) -> str | None:
    """
    Fetch a parameter value from SSM Parameter Store.

    The full path is: /data-cbs-interview/<ENV>/<name>
    ENV is read from the ENV environment variable (default: dev).

    Args:
        name:     Parameter name without namespace/env prefix, e.g. "s3_bucket".
        region:   AWS region (default: eu-west-2).
        required: If True, raises RuntimeError when the parameter is not found.

    Returns:
        The parameter value as a string, or None if not found and required=False.
    """
    env = os.environ.get("ENV", "dev")
    path = f"{SSM_NAMESPACE}/{env}/{name}"
    client = boto3.client("ssm", region_name=region)

    try:
        response = client.get_parameter(Name=path, WithDecryption=True)
        value = response["Parameter"]["Value"]
        logger.debug("Fetched SSM parameter: %s", path)
        return value
    except ClientError as e:
        if e.response["Error"]["Code"] == "ParameterNotFound":
            if required:
                raise RuntimeError(
                    f"Required SSM parameter not found: {path}\n"
                    f"Check ENV is set correctly (current: {os.environ.get('ENV', 'dev')}) "
                    f"and run config/ssm_setup.sh to provision parameters."
                ) from e
            logger.debug("Optional SSM parameter not found: %s", path)
            return None
        raise


if __name__ == "__main__":
    import sys

    if len(sys.argv) < 2:
        print("Usage: uv run src/utils/get_parameter.py <parameter_name>")
        sys.exit(1)

    logging.basicConfig(level=logging.WARNING)
    result = get_parameter(sys.argv[1])
    print(result)

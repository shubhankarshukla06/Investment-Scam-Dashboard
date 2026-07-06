"""
utils/aws_upload.py
Handles uploading PDFs to AWS S3 and returns a CloudFront CDN URL.
"""

import os
import logging

import boto3
from botocore.exceptions import BotoCoreError, ClientError

from config import Config

logger = logging.getLogger(__name__)


def _get_s3_client():
    """Create and return a boto3 S3 client using credentials from Config."""
    return boto3.client(
        "s3",
        region_name=Config.AWS_REGION,
        aws_access_key_id=Config.AWS_ACCESS_KEY_ID,
        aws_secret_access_key=Config.AWS_SECRET_ACCESS_KEY,
    )


def upload_pdf(local_file_path: str) -> str:
    """
    Upload a PDF file to S3 and return its CloudFront URL.

    The file is placed under the `case-reports/` prefix in the bucket
    configured via S3_BUCKET_NAME, with public read access and the
    correct Content-Type header.

    Args:
        local_file_path: Absolute or relative path to the local PDF file.

    Returns:
        CloudFront URL string pointing to the uploaded PDF.

    Raises:
        FileNotFoundError: If the local file does not exist.
        RuntimeError: If the S3 upload fails for any reason.
    """
    if not os.path.isfile(local_file_path):
        raise FileNotFoundError(f"PDF not found at path: {local_file_path}")

    filename = os.path.basename(local_file_path)
    s3_key = f"{Config.S3_KEY_PREFIX}/{filename}"

    logger.info(
        "Uploading '%s' to s3://%s/%s ...",
        filename,
        Config.S3_BUCKET_NAME,
        s3_key,
    )

    try:
        client = _get_s3_client()
        client.upload_file(
            Filename=local_file_path,
            Bucket=Config.S3_BUCKET_NAME,
            Key=s3_key,
            ExtraArgs={
                "ContentType": "application/pdf",
                "ContentDisposition": f'inline; filename="{filename}"',
            },
        )
        logger.info("Upload successful: s3://%s/%s", Config.S3_BUCKET_NAME, s3_key)
    except (BotoCoreError, ClientError) as exc:
        logger.error("S3 upload failed: %s", exc)
        raise RuntimeError(f"Failed to upload PDF to S3: {exc}") from exc

    # Build CloudFront URL (strip trailing slash from domain just in case)
    domain = Config.CLOUDFRONT_DOMAIN.rstrip("/")
    cloudfront_url = f"https://{domain}/{s3_key}"
    logger.info("CloudFront URL: %s", cloudfront_url)
    return cloudfront_url


def delete_from_s3(filename: str) -> None:
    """
    Delete a PDF from S3 given its bare filename (not full key).

    Args:
        filename: Just the filename, e.g. 'npci-123_abc_def--insta.com.pdf'

    Raises:
        RuntimeError: If deletion fails.
    """
    s3_key = f"{Config.S3_KEY_PREFIX}/{filename}"
    logger.info(
        "Deleting s3://%s/%s ...", Config.S3_BUCKET_NAME, s3_key
    )
    try:
        client = _get_s3_client()
        client.delete_object(Bucket=Config.S3_BUCKET_NAME, Key=s3_key)
        logger.info("Deleted from S3: %s", s3_key)
    except (BotoCoreError, ClientError) as exc:
        logger.error("S3 delete failed: %s", exc)
        raise RuntimeError(f"Failed to delete PDF from S3: {exc}") from exc

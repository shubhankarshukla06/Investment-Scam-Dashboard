"""
config.py — Centralised environment-variable loading.
All other modules import from here instead of calling os.getenv() directly.
"""

import os
from dotenv import load_dotenv

load_dotenv()


class Config:
    """Application-wide configuration, sourced from environment variables."""

    # Flask
    SECRET_KEY: str = os.getenv("SECRET_KEY", "change-me-in-production")
    FLASK_DEBUG: bool = os.getenv("FLASK_DEBUG", "false").lower() == "true"

    # AWS
    AWS_ACCESS_KEY_ID: str = os.getenv("AWS_ACCESS_KEY_ID", "")
    AWS_SECRET_ACCESS_KEY: str = os.getenv("AWS_SECRET_ACCESS_KEY", "")
    AWS_REGION: str = os.getenv("AWS_REGION", "ap-south-1")
    S3_BUCKET_NAME: str = os.getenv("S3_BUCKET_NAME", "")
    CLOUDFRONT_DOMAIN: str = os.getenv("CLOUDFRONT_DOMAIN", "")   # e.g. d1abc123.cloudfront.net
    S3_KEY_PREFIX: str = "case-reports"                            # folder inside the bucket

    # Supabase
    SUPABASE_URL: str = os.getenv("SUPABASE_URL", "")
    SUPABASE_KEY: str = os.getenv("SUPABASE_KEY", "")
    SUPABASE_TABLE: str = "reports"

    # Upload limits
    MAX_CONTENT_LENGTH: int = 25 * 1024 * 1024    # 25 MB

    @classmethod
    def validate(cls) -> None:
        """
        Raise RuntimeError for any required variable that is missing.
        Call this at startup so misconfiguration is caught immediately.
        """
        required = {
            "AWS_ACCESS_KEY_ID": cls.AWS_ACCESS_KEY_ID,
            "AWS_SECRET_ACCESS_KEY": cls.AWS_SECRET_ACCESS_KEY,
            "S3_BUCKET_NAME": cls.S3_BUCKET_NAME,
            "CLOUDFRONT_DOMAIN": cls.CLOUDFRONT_DOMAIN,
            "SUPABASE_URL": cls.SUPABASE_URL,
            "SUPABASE_KEY": cls.SUPABASE_KEY,
        }
        missing = [k for k, v in required.items() if not v]
        if missing:
            raise RuntimeError(
                f"Missing required environment variables: {', '.join(missing)}"
            )

"""
utils/filename_generator.py
Generates unique PDF filenames in the format:
    npci-<10-digit-random>_<8-char-uuid>_<14-char-uuid>--<hostname>.pdf

Example:
    npci-1782707868_698c756c_6a41f69c98915--instagram.com.pdf
"""

import random
import uuid
from urllib.parse import urlparse


def _extract_hostname(url: str) -> str:
    """
    Extract a clean hostname from a URL, stripping 'www.' prefix.

    Examples:
        https://www.instagram.com/p/abc  →  instagram.com
        https://telegram.org/blog/abc    →  telegram.org
        not-a-url                        →  unknown
    """
    try:
        parsed = urlparse(url)
        # urlparse needs a scheme to parse correctly
        if not parsed.scheme:
            parsed = urlparse("https://" + url)
        hostname = parsed.hostname or "unknown"
        if hostname.startswith("www."):
            hostname = hostname[4:]
        return hostname
    except Exception:
        return "unknown"


def _random_10_digits() -> str:
    """Return a random 10-digit integer string (no leading zeros)."""
    return str(random.randint(1_000_000_000, 9_999_999_999))


def _uuid_segment(length: int) -> str:
    """Return the first `length` characters of a hex UUID4 string."""
    return uuid.uuid4().hex[:length]


def generate_filename(source_url: str) -> str:
    """
    Generate a unique PDF filename tied to the source URL's hostname.

    Format:
        npci-<10d>_<8c>_<14c>--<hostname>.pdf

    Args:
        source_url: The source URL from which to extract the hostname.

    Returns:
        A filename string, e.g. 'npci-1782707868_698c756c_6a41f69c98915--instagram.com.pdf'
    """
    hostname = _extract_hostname(source_url)
    rand_10 = _random_10_digits()
    seg_8 = _uuid_segment(8)
    seg_14 = _uuid_segment(14)
    return f"npci-{rand_10}_{seg_8}_{seg_14}--{hostname}.pdf"

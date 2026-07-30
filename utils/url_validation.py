"""URL validation utility for SSRF prevention."""

import ipaddress
import socket
from urllib.parse import urlparse


def _validate_url_safe(url: str) -> bool:
    """Validate a URL to prevent SSRF attacks.

    - Only http/https schemes allowed
    - Blocks private/loopback/link-local/multicast IPs (including DNS-resolved)
    - Blocks empty hostnames
    """
    if not url or not isinstance(url, str):
        return False

    try:
        parsed = urlparse(url)
        if parsed.scheme not in ("https", "http"):
            return False
        if not parsed.netloc:
            return False
        hostname = parsed.netloc.split(":")[0].split("@")[-1]

        # Try direct IP match first
        try:
            ip = ipaddress.ip_address(hostname)
            # Explicit public IP — safe (negated check for ruff SIM103)
            return not (ip.is_private or ip.is_loopback or ip.is_link_local or ip.is_multicast)
        except ValueError:
            pass  # Not a literal IP, resolve DNS below

        # Resolve DNS hostname and check all resolved addresses
        try:
            addrs = socket.getaddrinfo(hostname, None)
            for addr in addrs:
                ip = ipaddress.ip_address(addr[4][0])
                if ip.is_private or ip.is_loopback or ip.is_link_local or ip.is_multicast:
                    return False
        except (socket.gaierror, OSError):
            # Hostname could not be resolved — reject to be safe
            return False

        return True
    except Exception:
        return False

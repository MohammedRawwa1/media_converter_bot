"""URL validation utility for SSRF prevention."""

def _validate_url_safe(url: str) -> bool:
    """Validate a URL to prevent SSRF attacks.
    
    - Only http/https schemes allowed
    - Blocks private/loopback/link-local/multicast IPs
    - Blocks empty hostnames
    """
    if not url or not isinstance(url, str):
        return False
    from urllib.parse import urlparse
    try:
        parsed = urlparse(url)
        if parsed.scheme not in ("https", "http"):
            return False
        if not parsed.netloc:
            return False
        hostname = parsed.netloc.split(":")[0].split("@")[-1]
        try:
            import ipaddress
            ip = ipaddress.ip_address(hostname)
            if ip.is_private or ip.is_loopback or ip.is_link_local or ip.is_multicast:
                return False
        except ValueError:
            pass
        return True
    except Exception:
        return False

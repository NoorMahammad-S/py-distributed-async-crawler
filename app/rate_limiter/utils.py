from urllib.parse import urlparse

def domain_from_url(url: str) -> str:
    return urlparse(url).netloc.lower()

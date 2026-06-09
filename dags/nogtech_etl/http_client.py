from __future__ import annotations

import requests
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry


def build_http_session() -> requests.Session:
    """Cria uma sessao HTTP com retry para chamadas instaveis da BrasilAPI."""
    retry_strategy = Retry(
        total=3,
        backoff_factor=1,
        status_forcelist=(429, 500, 502, 503, 504),
        allowed_methods=("GET",),
    )
    session = requests.Session()
    session.mount("https://", HTTPAdapter(max_retries=retry_strategy))
    return session

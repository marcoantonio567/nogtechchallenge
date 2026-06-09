from __future__ import annotations

from nogtech_etl.http_client import build_http_session


def test_build_http_session_configures_retries_for_transient_errors() -> None:
    session = build_http_session()
    adapter = session.get_adapter("https://brasilapi.com.br")
    retry = adapter.max_retries

    assert retry.total == 3
    assert set(retry.status_forcelist) == {429, 500, 502, 503, 504}
    assert set(retry.allowed_methods) == {"GET"}

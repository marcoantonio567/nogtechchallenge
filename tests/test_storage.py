from __future__ import annotations

from nogtech_etl.storage import read_json_cache, write_json_cache


def test_read_json_cache_returns_empty_dict_when_file_does_not_exist(tmp_path) -> None:
    assert read_json_cache(tmp_path / "missing.json") == {}


def test_read_json_cache_returns_empty_dict_when_file_is_corrupted(tmp_path) -> None:
    cache_path = tmp_path / "cache.json"
    cache_path.write_text("{json quebrado", encoding="utf-8")

    assert read_json_cache(cache_path) == {}


def test_write_json_cache_persists_data_and_cleans_temporary_file(tmp_path) -> None:
    cache_path = tmp_path / "cache.json"
    data = {"01001000": {"cidade": "Sao Paulo", "uf": "SP"}}

    write_json_cache(cache_path, data)

    assert read_json_cache(cache_path) == data
    assert not (tmp_path / "cache.tmp").exists()

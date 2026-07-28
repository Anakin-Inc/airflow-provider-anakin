"""Tests for get_provider_info() — the one module in this package with zero
Airflow/anakin-sdk imports, so (unlike test_hooks.py/test_operators.py)
these actually run in any bare Python 3.10+ environment."""

from __future__ import annotations

from anakin_provider import __version__, get_provider_info


def test_get_provider_info_has_required_keys() -> None:
    info = get_provider_info()
    # Per Airflow's provider_info.schema.json, only "name" and "description"
    # are strictly required — but a real provider needs package-name and
    # versions too for the CLI/UI to display it usefully.
    for key in ("package-name", "name", "description", "versions"):
        assert key in info


def test_get_provider_info_versions_matches_module_version() -> None:
    info = get_provider_info()
    assert info["versions"] == [__version__]


def test_get_provider_info_connection_type_points_at_real_hook_path() -> None:
    info = get_provider_info()
    [conn_type] = info["connection-types"]
    assert conn_type["connection-type"] == "anakin"
    assert conn_type["hook-class-name"] == "anakin_provider.hooks.anakin.AnakinHook"


def test_get_provider_info_operators_point_at_real_module_path() -> None:
    info = get_provider_info()
    [operators_entry] = info["operators"]
    assert operators_entry["python-modules"] == ["anakin_provider.operators.anakin"]

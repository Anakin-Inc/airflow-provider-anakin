"""Unit tests for AnakinHook.

Mocks `BaseHook.get_connection` (the Airflow metadata-DB lookup) and the
`anakin.Anakin` client construction — never makes a network call and never
needs a real Airflow metadata database. Requires `apache-airflow` and
`anakin-sdk` installed to *run*; see SUBMIT.md for why that wasn't
exercised in this environment.
"""

from __future__ import annotations

from unittest.mock import MagicMock, patch

import pytest
from airflow.exceptions import AirflowNotFoundException

from anakin_provider.hooks.anakin import DEFAULT_ANAKIN_BASE_URL, AnakinHook


def _fake_connection(*, password: str | None, extra: dict | None = None) -> MagicMock:
    conn = MagicMock()
    conn.password = password
    conn.extra_dejson = extra or {}
    return conn


@pytest.fixture(autouse=True)
def _no_ambient_api_key(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.delenv("ANAKIN_API_KEY", raising=False)


def test_default_conn_name_and_conn_type() -> None:
    assert AnakinHook.default_conn_name == "anakin_default"
    assert AnakinHook.conn_type == "anakin"
    assert AnakinHook.conn_name_attr == "anakin_conn_id"


def test_get_conn_uses_connection_password_as_api_key() -> None:
    hook = AnakinHook(anakin_conn_id="anakin_test")
    with patch.object(
        hook, "get_connection", return_value=_fake_connection(password="ask-secret")
    ):
        with patch("anakin_provider.hooks.anakin.Anakin") as mock_anakin_cls:
            hook.get_conn()
            mock_anakin_cls.assert_called_once_with(
                api_key="ask-secret", base_url=DEFAULT_ANAKIN_BASE_URL
            )


def test_get_conn_honours_base_url_override_from_extra() -> None:
    hook = AnakinHook()
    with patch.object(
        hook,
        "get_connection",
        return_value=_fake_connection(
            password="ask-secret", extra={"base_url": "https://staging.anakin.io/v1"}
        ),
    ):
        with patch("anakin_provider.hooks.anakin.Anakin") as mock_anakin_cls:
            hook.get_conn()
            mock_anakin_cls.assert_called_once_with(
                api_key="ask-secret", base_url="https://staging.anakin.io/v1"
            )


def test_get_conn_falls_back_to_env_var_when_connection_missing() -> None:
    hook = AnakinHook()
    with patch.object(
        hook, "get_connection", side_effect=AirflowNotFoundException("no such conn")
    ):
        with patch("anakin_provider.hooks.anakin.Anakin") as mock_anakin_cls:
            hook.get_conn()
            # api_key=None tells anakin.Anakin() to fall back to ANAKIN_API_KEY itself.
            mock_anakin_cls.assert_called_once_with(
                api_key=None, base_url=DEFAULT_ANAKIN_BASE_URL
            )


def test_get_conn_caches_the_client() -> None:
    hook = AnakinHook()
    with patch.object(
        hook, "get_connection", return_value=_fake_connection(password="ask-secret")
    ):
        with patch("anakin_provider.hooks.anakin.Anakin") as mock_anakin_cls:
            client1 = hook.get_conn()
            client2 = hook.get_conn()
            assert client1 is client2
            mock_anakin_cls.assert_called_once()


def test_test_connection_fails_with_no_key_anywhere() -> None:
    hook = AnakinHook()
    with patch.object(
        hook, "get_connection", side_effect=AirflowNotFoundException("no such conn")
    ):
        ok, message = hook.test_connection()
    assert ok is False
    assert "ANAKIN_API_KEY" in message


def test_test_connection_succeeds_without_a_network_call() -> None:
    hook = AnakinHook()
    with patch.object(
        hook, "get_connection", return_value=_fake_connection(password="ask-secret")
    ):
        with patch("anakin_provider.hooks.anakin.Anakin") as mock_anakin_cls:
            ok, message = hook.test_connection()
            assert ok is True
            assert "no network call" in message.lower()
            # test_connection must not touch billed endpoints.
            mock_anakin_cls.return_value.search.assert_not_called()
            mock_anakin_cls.return_value.agentic_search.assert_not_called()


def test_scrape_search_agentic_search_delegate_to_client() -> None:
    hook = AnakinHook()
    with patch.object(
        hook, "get_connection", return_value=_fake_connection(password="ask-secret")
    ):
        with patch("anakin_provider.hooks.anakin.Anakin") as mock_anakin_cls:
            client = mock_anakin_cls.return_value

            hook.scrape("https://example.com", country="gb")
            client.scrape.assert_called_once_with("https://example.com", country="gb")

            hook.search("latest AI agents", limit=3)
            client.search.assert_called_once_with("latest AI agents", limit=3)

            hook.agentic_search("compare CRMs", use_browser=False)
            client.agentic_search.assert_called_once_with("compare CRMs", use_browser=False)

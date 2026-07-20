"""Shared test fixtures."""

import pytest

from airflow_dev_mcp import server


@pytest.fixture(autouse=True)
def _reset_module_caches():
    """The server caches the detected API prefix and the JWT in module globals.

    Reset them around every test so cases don't leak detection/auth state into
    each other. Safe for both unit and integration tests.
    """
    server._token_cache = None
    server._api_prefix_cache = None
    yield
    server._token_cache = None
    server._api_prefix_cache = None

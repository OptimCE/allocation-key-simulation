"""Fixtures shared by the worker tests."""

import tempfile

import pytest


@pytest.fixture(autouse=True)
def _temp_files_in_tmp_path(monkeypatch, tmp_path):
    """Write the solves' time-series files (``worker.timeseries``) under ``tmp_path``.

    Every solve writes one. A test that fails before the dispatcher deletes it
    would otherwise leave it in the system temp dir, and tests can list
    ``tmp_path`` to check that nothing was left behind.
    """
    monkeypatch.setattr(tempfile, "tempdir", str(tmp_path))

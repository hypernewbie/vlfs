"""Test fixtures and utilities for VLFS."""

import os
import subprocess
from pathlib import Path
from typing import Any, Callable

import pytest


@pytest.fixture
def rclone_mock(mocker: Any) -> Callable:
    """Mock rclone subprocess calls.

    Returns a callable that can be configured to return specific exit codes
    and outputs for different rclone subcommands.

    Usage:
        def test_something(rclone_mock):
            mock = rclone_mock({
                'lsd': (0, 'bucket1\nbucket2', ''),
                'copy': (0, '', ''),
                'cat': (1, '', 'file not found'),
            })
            # Now subprocess.run(['rclone', ...]) returns configured values

    Advanced usage with handler:
        def test_something(rclone_mock):
            def handler(cmd):
                if cmd[1] == 'ls':
                    return (0, 'output', '')
                raise RcloneError("fail", 1, "", "")
            mock = rclone_mock({'_handler': handler})
    """

    def _create_mock(responses: dict[str, Any] | None = None) -> dict:
        """Configure mock responses for rclone subcommands.

        Args:
            responses: Dict mapping subcommand names to (returncode, stdout, stderr) tuples,
                      or a '_handler' key with a callable that takes cmd list and returns tuple.
                      Default returns (0, '', '') for any command.

        Returns:
            Dict tracking all calls made to the mock.
        """
        call_log: list[list[str]] = []
        responses = responses or {}
        handler = responses.get("_handler")

        def mock_run(*args: Any, **kwargs: Any) -> Any:
            cmd = args[0] if args else kwargs.get("args", [])
            call_log.append(list(cmd))

            # Extract subcommand (e.g., 'rclone lsd ...' -> 'lsd')
            subcommand = cmd[1] if len(cmd) > 1 else "unknown"

            if handler:
                # Use custom handler
                returncode, stdout, stderr = handler(cmd)
            elif subcommand in responses:
                returncode, stdout, stderr = responses[subcommand]
            else:
                # Default success response
                returncode, stdout, stderr = 0, "", ""

            # Create mock result object matching subprocess.CompletedProcess
            class MockResult:
                def __init__(self, rc: int, out: str, err: str) -> None:
                    self.returncode = rc
                    self.stdout = out
                    self.stderr = err

            return MockResult(returncode, stdout, stderr)

        mocker.patch("vlfs.subprocess.run", side_effect=mock_run)

        return {"calls": call_log, "responses": responses}

    return _create_mock


@pytest.fixture
def repo_root(tmp_path: Path) -> Path:
    """Create a temporary repository root with VLFS structure.

    Creates:
        <tmp_path>/
            .vlfs/
            .vlfs-cache/
                objects/
            tools/
            assets/

    Returns:
        Path to the temporary repository root.
    """
    vlfs_dir = tmp_path / ".vlfs"
    vlfs_dir.mkdir()

    cache_dir = tmp_path / ".vlfs-cache" / "objects"
    cache_dir.mkdir(parents=True)

    tools_dir = tmp_path / "tools"
    tools_dir.mkdir()

    assets_dir = tmp_path / "assets"
    assets_dir.mkdir()

    return tmp_path


@pytest.fixture
def env_vars(monkeypatch: Any) -> Callable:
    """Manage environment variables for tests.

    Usage:
        def test_something(env_vars):
            env_vars({
                'RCLONE_CONFIG_R2_ACCESS_KEY_ID': 'test-key',
                'RCLONE_CONFIG_R2_SECRET_ACCESS_KEY': 'test-secret',
            })
            # Variables are set for this test and cleaned up after
    """

    def _set_env(vars_dict: dict[str, str | None]) -> None:
        for key, value in vars_dict.items():
            if value is None:
                monkeypatch.delenv(key, raising=False)
            else:
                monkeypatch.setenv(key, value)

    return _set_env


@pytest.fixture
def mock_rclone_binary(tmp_path: Path, monkeypatch: Any) -> Path:
    """Create a mock rclone binary that logs its arguments.

    Creates a shell script that records all invocations to a log file.
    Useful for integration-style tests that need to verify rclone was called.

    Returns:
        Path to the mock rclone binary.
    """
    rclone_path = tmp_path / "rclone"
    log_path = tmp_path / "rclone_calls.log"

    if os.name == "nt":  # Windows
        rclone_path = tmp_path / "rclone.bat"
        script_content = f"""@echo off
echo %* >> "{log_path}"
exit /b 0
"""
    else:  # Unix-like
        script_content = f"""#!/bin/bash
echo "$@" >> "{log_path}"
exit 0
"""

    rclone_path.write_text(script_content)
    rclone_path.chmod(0o755)

    # Add to PATH
    monkeypatch.setenv("PATH", str(tmp_path) + os.pathsep + os.environ.get("PATH", ""))

    return rclone_path


@pytest.fixture(autouse=True)
def mock_r2_creds(monkeypatch: Any) -> None:
    """Set dummy R2 credentials for all tests."""
    monkeypatch.setenv("RCLONE_CONFIG_R2_ACCESS_KEY_ID", "test_key")
    monkeypatch.setenv("RCLONE_CONFIG_R2_SECRET_ACCESS_KEY", "test_secret")
    monkeypatch.setenv(
        "RCLONE_CONFIG_R2_ENDPOINT", "https://test.r2.cloudflarestorage.com"
    )


@pytest.fixture(autouse=True)
def block_real_subprocess(monkeypatch: Any) -> None:
    """Block real subprocess calls to prevent CI hangs.

    Tests that need subprocess must use rclone_mock fixture which
    overrides this with a proper mock.
    """
    original_run = subprocess.run

    def guarded_run(*args: Any, **kwargs: Any) -> Any:
        cmd = args[0] if args else kwargs.get("args", [])
        if cmd and len(cmd) > 0 and "rclone" in str(cmd[0]).lower():
            # Return a fake success for rclone commands
            class FakeResult:
                returncode = 0
                stdout = ""
                stderr = ""

            return FakeResult()
        return original_run(*args, **kwargs)

    # Patch subprocess.run in the vlfs module
    import vlfs

    monkeypatch.setattr("vlfs.subprocess.run", guarded_run)

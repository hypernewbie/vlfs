"""Unit tests for configurable cloud storage buckets."""

import os
from pathlib import Path
from unittest.mock import MagicMock, patch

import pytest

import vlfs


@pytest.fixture
def mock_config(repo_root):
    """Create a config.toml with custom bucket names."""
    vlfs_dir = repo_root / ".vlfs"
    vlfs_dir.mkdir(parents=True, exist_ok=True)
    config_path = vlfs_dir / "config.toml"
    config_content = """
[remotes.r2]
provider = "Cloudflare"
bucket = "custom-r2-bucket"

[remotes.gdrive]
bucket = "custom-drive-folder"
"""
    config_path.write_text(config_content)
    return config_path


class TestConfigurableBuckets:
    """Test that bucket names are correctly read from config and passed to functions."""

    def test_push_uses_configured_buckets(self, repo_root, mock_config, monkeypatch):
        """Test that push command passes configured buckets to upload functions."""
        monkeypatch.chdir(repo_root)

        # Mock dependencies
        mock_validate = MagicMock(return_value=True)
        mock_upload_r2 = MagicMock(return_value=True)
        mock_upload_drive = MagicMock(return_value=True)
        mock_ensure_auth = MagicMock(return_value=0)
        
        # Mock functions in vlfs module
        monkeypatch.setattr(vlfs, "validate_r2_connection", mock_validate)
        monkeypatch.setattr(vlfs, "upload_to_r2", mock_upload_r2)
        monkeypatch.setattr(vlfs, "upload_to_drive", mock_upload_drive)
        monkeypatch.setattr(vlfs, "ensure_r2_auth", mock_ensure_auth)
        monkeypatch.setattr(vlfs, "has_drive_token", lambda: True)

        # Create a dummy file to push
        test_file = repo_root / "test_file.txt"
        test_file.write_text("content")

        # Test R2 Push
        vlfs.main(["push", "test_file.txt"])
        
        # Verify validation called with custom bucket
        mock_validate.assert_called_with(bucket="custom-r2-bucket")
        
        # Verify upload called with custom bucket
        # Note: We need to inspect call args to find the bucket arg
        args, kwargs = mock_upload_r2.call_args
        assert kwargs.get("bucket") == "custom-r2-bucket"

        # Test Drive Push
        vlfs.main(["push", "--private", "test_file.txt"])
        
        args, kwargs = mock_upload_drive.call_args
        assert kwargs.get("bucket") == "custom-drive-folder"

    def test_pull_uses_configured_buckets(self, repo_root, mock_config, monkeypatch):
        """Test that pull command passes configured buckets to download functions."""
        monkeypatch.chdir(repo_root)

        # Mock dependencies
        mock_validate = MagicMock(return_value=True)
        mock_download_r2 = MagicMock(return_value=1)
        mock_download_drive = MagicMock(return_value=1)
        
        monkeypatch.setattr(vlfs, "validate_r2_connection", mock_validate)
        monkeypatch.setattr(vlfs, "download_from_r2", mock_download_r2)
        monkeypatch.setattr(vlfs, "download_from_drive", mock_download_drive)
        monkeypatch.setattr(vlfs, "has_drive_token", lambda: True)

        # Setup index with R2 and Drive entries
        vlfs_dir = repo_root / ".vlfs"
        index_data = {
            "version": 1,
            "entries": {
                "r2_file.txt": {
                    "hash": "abc",
                    "size": 10,
                    "compressed_size": 5,
                    "mtime": 123,
                    "object_key": "ab/c",
                    "remote": "r2"
                },
                "drive_file.txt": {
                    "hash": "def",
                    "size": 10,
                    "compressed_size": 5,
                    "mtime": 123,
                    "object_key": "de/f",
                    "remote": "gdrive"
                }
            }
        }
        vlfs.write_index(vlfs_dir, index_data)

        # Run pull
        vlfs.main(["pull"])

        # Verify validation
        mock_validate.assert_called_with(bucket="custom-r2-bucket")

        # Verify R2 download
        # Check call args for download_from_r2
        args, kwargs = mock_download_r2.call_args
        assert kwargs.get("bucket") == "custom-r2-bucket"

        # Verify Drive download
        args, kwargs = mock_download_drive.call_args
        assert kwargs.get("bucket") == "custom-drive-folder"

    def test_default_buckets(self, repo_root, monkeypatch):
        """Test that default 'vlfs' bucket is used when config is missing."""
        monkeypatch.chdir(repo_root)
        
        # Ensure no config file
        vlfs_dir = repo_root / ".vlfs"
        vlfs_dir.mkdir(parents=True, exist_ok=True)
        config_path = vlfs_dir / "config.toml"
        if config_path.exists():
            config_path.unlink()

        mock_validate = MagicMock(return_value=True)
        mock_upload_r2 = MagicMock(return_value=True)
        mock_ensure_auth = MagicMock(return_value=0)

        monkeypatch.setattr(vlfs, "validate_r2_connection", mock_validate)
        monkeypatch.setattr(vlfs, "upload_to_r2", mock_upload_r2)
        monkeypatch.setattr(vlfs, "ensure_r2_auth", mock_ensure_auth)

        test_file = repo_root / "test_file.txt"
        test_file.write_text("content")

        vlfs.main(["push", "test_file.txt"])

        mock_validate.assert_called_with(bucket="vlfs")
        args, kwargs = mock_upload_r2.call_args
        assert kwargs.get("bucket") == "vlfs"

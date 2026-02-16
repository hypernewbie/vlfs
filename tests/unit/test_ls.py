"""Unit tests for ls command."""

import json
from unittest.mock import patch, MagicMock

import pytest

import vlfs


class TestList:
    """Test ls command."""

    def test_list_files(self, tmp_path, capsys):
        """Should list files."""
        repo_root = tmp_path
        vlfs_dir = repo_root / ".vlfs"
        vlfs.ensure_dirs(vlfs_dir, repo_root / ".vlfs-cache")
        
        index_data = {
            "version": 1,
            "entries": {
                "file1": {"hash": "h1", "size": 100, "remote": "r2"},
                "file2": {"hash": "h2", "size": 200, "remote": "gdrive"}
            }
        }
        vlfs.write_index(vlfs_dir, index_data)
        
        vlfs.cmd_list(repo_root, vlfs_dir)
        
        captured = capsys.readouterr()
        assert "file1" in captured.out
        assert "file2" in captured.out

    def test_list_long_format(self, tmp_path, capsys):
        """Should list files in long format."""
        repo_root = tmp_path
        vlfs_dir = repo_root / ".vlfs"
        vlfs.ensure_dirs(vlfs_dir, repo_root / ".vlfs-cache")
        
        index_data = {
            "version": 1,
            "entries": {
                "file1": {"hash": "h1hashhash", "size": 1024, "remote": "r2"}
            }
        }
        vlfs.write_index(vlfs_dir, index_data)
        
        vlfs.cmd_list(repo_root, vlfs_dir, long_format=True)
        
        captured = capsys.readouterr()
        assert "h1hashha" in captured.out  # Hash truncated
        assert "1.0KB" in captured.out     # Size formatted
        assert "r2" in captured.out
        assert "file1" in captured.out

    def test_list_json(self, tmp_path, capsys):
        """Should list files as JSON."""
        repo_root = tmp_path
        vlfs_dir = repo_root / ".vlfs"
        vlfs.ensure_dirs(vlfs_dir, repo_root / ".vlfs-cache")
        
        index_data = {
            "version": 1,
            "entries": {
                "file1": {"hash": "h1", "size": 100, "remote": "r2"}
            }
        }
        vlfs.write_index(vlfs_dir, index_data)
        
        vlfs.cmd_list(repo_root, vlfs_dir, json_output=True)
        
        captured = capsys.readouterr()
        data = json.loads(captured.out)
        assert len(data) == 1
        assert data[0]["path"] == "file1"
        assert data[0]["hash"] == "h1"

    def test_list_remote_filter(self, tmp_path, capsys):
        """Should filter by remote."""
        repo_root = tmp_path
        vlfs_dir = repo_root / ".vlfs"
        vlfs.ensure_dirs(vlfs_dir, repo_root / ".vlfs-cache")
        
        index_data = {
            "version": 1,
            "entries": {
                "file1": {"hash": "h1", "size": 100, "remote": "r2"},
                "file2": {"hash": "h2", "size": 200, "remote": "gdrive"}
            }
        }
        vlfs.write_index(vlfs_dir, index_data)
        
        vlfs.cmd_list(repo_root, vlfs_dir, remote_filter="gdrive")
        
        captured = capsys.readouterr()
        assert "file1" not in captured.out
        assert "file2" in captured.out

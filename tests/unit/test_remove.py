"""Unit tests for remove command."""

import json
from pathlib import Path
from unittest.mock import patch, MagicMock

import pytest

import vlfs


class TestRemove:
    """Test remove command."""

    def test_remove_single_file(self, tmp_path, rclone_mock, monkeypatch):
        """Should remove file from index, cache, and remote."""
        repo_root = tmp_path
        monkeypatch.chdir(repo_root)
        vlfs_dir = repo_root / ".vlfs"
        cache_dir = repo_root / ".vlfs-cache"
        
        # Setup directories
        vlfs.ensure_dirs(vlfs_dir, cache_dir)
        
        # Create a file and track it
        file_path = repo_root / "test.file"
        file_path.write_bytes(b"content")
        
        # Mock index
        index_data = {
            "version": 1,
            "entries": {
                "test.file": {
                    "object_key": "ab/cd/hash",
                    "hash": "hash",
                    "remote": "r2",
                    "size": 7,
                    "mtime": 12345.0
                }
            }
        }
        vlfs.write_index(vlfs_dir, index_data)
        
        # Create cache object
        cache_obj = cache_dir / "objects" / "ab/cd/hash"
        cache_obj.parent.mkdir(parents=True, exist_ok=True)
        cache_obj.write_bytes(b"compressed")
        
        # Mock rclone
        mock = rclone_mock({
            "deletefile": (0, "", "")
        })
        
        # Run remove
        vlfs.cmd_remove(repo_root, vlfs_dir, cache_dir, ["test.file"], force=True)
        
        # Check index
        new_index = vlfs.read_index(vlfs_dir)
        assert "test.file" not in new_index["entries"]
        
        # Check cache
        assert not cache_obj.exists()
        
        # Check remote call
        assert mock["calls"][0] == ["rclone", "deletefile", "r2:vlfs/ab/cd/hash", "--s3-no-check-bucket"]

    def test_remove_directory(self, tmp_path, rclone_mock, monkeypatch):
        """Should remove all files in directory."""
        repo_root = tmp_path
        monkeypatch.chdir(repo_root)
        vlfs_dir = repo_root / ".vlfs"
        cache_dir = repo_root / ".vlfs-cache"
        vlfs.ensure_dirs(vlfs_dir, cache_dir)
        
        (repo_root / "dir").mkdir()
        (repo_root / "dir/f1").write_bytes(b"1")
        (repo_root / "dir/f2").write_bytes(b"2")
        
        index_data = {
            "version": 1,
            "entries": {
                "dir/f1": {"object_key": "k1", "remote": "r2"},
                "dir/f2": {"object_key": "k2", "remote": "r2"},
                "other": {"object_key": "k3", "remote": "r2"}
            }
        }
        vlfs.write_index(vlfs_dir, index_data)
        
        # Mock rclone
        mock = rclone_mock({
            "deletefile": (0, "", "")
        })
        
        (cache_dir / "objects/k1").parent.mkdir(parents=True, exist_ok=True)
        (cache_dir / "objects/k1").touch()
        (cache_dir / "objects/k2").parent.mkdir(parents=True, exist_ok=True)
        (cache_dir / "objects/k2").touch()
        
        vlfs.cmd_remove(repo_root, vlfs_dir, cache_dir, ["dir"], force=True)
        
        new_index = vlfs.read_index(vlfs_dir)
        assert "dir/f1" not in new_index["entries"]
        assert "dir/f2" not in new_index["entries"]
        assert "other" in new_index["entries"]
        
        # Should have called deletefile twice
        cmds = [c[2] for c in mock["calls"] if c[1] == "deletefile"]
        assert "r2:vlfs/k1" in cmds
        assert "r2:vlfs/k2" in cmds

    def test_deduplication_preserves_object(self, tmp_path, rclone_mock, monkeypatch):
        """Should not delete object if referenced by another file."""
        repo_root = tmp_path
        monkeypatch.chdir(repo_root)
        vlfs_dir = repo_root / ".vlfs"
        cache_dir = repo_root / ".vlfs-cache"
        vlfs.ensure_dirs(vlfs_dir, cache_dir)
        
        index_data = {
            "version": 1,
            "entries": {
                "f1": {"object_key": "shared_key", "remote": "r2"},
                "f2": {"object_key": "shared_key", "remote": "r2"}
            }
        }
        vlfs.write_index(vlfs_dir, index_data)
        
        mock = rclone_mock({"deletefile": (0, "", "")})
        
        # Create cache object
        cache_obj = cache_dir / "objects" / "shared_key"
        cache_obj.parent.mkdir(parents=True, exist_ok=True)
        cache_obj.touch()
        
        # Remove f1
        vlfs.cmd_remove(repo_root, vlfs_dir, cache_dir, ["f1"], force=True)
        
        # Check index
        new_index = vlfs.read_index(vlfs_dir)
        assert "f1" not in new_index["entries"]
        assert "f2" in new_index["entries"]
        
        # Cache object should still exist
        assert cache_obj.exists()
        
        # Remote delete should NOT have been called
        assert len([c for c in mock["calls"] if c[1] == "deletefile"]) == 0
        
        # Now remove f2
        vlfs.cmd_remove(repo_root, vlfs_dir, cache_dir, ["f2"], force=True)
        
        # Now it should be gone
        assert not cache_obj.exists()
        assert len([c for c in mock["calls"] if c[1] == "deletefile"]) == 1

    def test_delete_file_flag(self, tmp_path, rclone_mock, monkeypatch):
        """Should delete workspace file if flag set."""
        repo_root = tmp_path
        monkeypatch.chdir(repo_root)
        vlfs_dir = repo_root / ".vlfs"
        cache_dir = repo_root / ".vlfs-cache"
        vlfs.ensure_dirs(vlfs_dir, cache_dir)
        
        file_path = repo_root / "test.file"
        file_path.write_bytes(b"content")
        
        index_data = {
            "version": 1,
            "entries": {
                "test.file": {"object_key": "k", "remote": "r2"}
            }
        }
        vlfs.write_index(vlfs_dir, index_data)
        rclone_mock({})
        
        # Run with delete_file=True
        vlfs.cmd_remove(repo_root, vlfs_dir, cache_dir, ["test.file"], force=True, delete_file=True)
        
        assert not file_path.exists()

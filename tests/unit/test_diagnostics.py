"""Unit tests for diagnostic tools (lookup, verify --remote, repair)."""

import json
from pathlib import Path
import pytest
import vlfs

class TestDiagnosticTools:
    def test_lookup_finds_file(self, repo_root, capsys):
        """Should find a file by partial hash."""
        index = {
            "version": 1,
            "entries": {
                "file1.txt": {
                    "hash": "abcdef123456",
                    "object_key": "ab/cd/abcdef123456",
                    "remote": "r2",
                    "size": 100
                }
            }
        }
        vlfs.write_index(repo_root / ".vlfs", index)
        
        # Search by hash
        vlfs.cmd_lookup(repo_root, repo_root / ".vlfs", "abcdef")
        captured = capsys.readouterr()
        assert "file1.txt" in captured.out
        assert "abcdef123456" in captured.out
        
        # Search by object key
        vlfs.cmd_lookup(repo_root, repo_root / ".vlfs", "ab/cd")
        captured = capsys.readouterr()
        assert "file1.txt" in captured.out

    def test_verify_remote_detects_404(self, repo_root, rclone_mock, capsys):
        """Should detect when an object is missing from remote."""
        index = {
            "version": 1,
            "entries": {
                "missing.txt": {
                    "hash": "missinghash",
                    "object_key": "mi/ss/missinghash",
                    "remote": "r2"
                },
                "exists.txt": {
                    "hash": "existshash",
                    "object_key": "ex/is/existshash",
                    "remote": "r2"
                }
            }
        }
        vlfs.write_index(repo_root / ".vlfs", index)
        
        # Mock rclone lsjson to only return 'exists.txt' object
        mock_lsjson = [
            {"Path": "ex/is/existshash", "IsDir": False}
        ]
        
        rclone_mock({
            "lsjson": (0, json.dumps(mock_lsjson), "")
        })
        
        # Run verify --remote
        cache_dir = repo_root / ".vlfs-cache"
        vlfs.cmd_verify(repo_root, repo_root / ".vlfs", cache_dir, remote=True)
        
        captured = capsys.readouterr()
        assert "MISSING REMOTE missing.txt" in captured.out
        assert "1 missing remote" in captured.out

    def test_repair_fixes_missing_remote(self, repo_root, rclone_mock, capsys):
        """Should re-upload missing remote objects if they exist in cache."""
        obj_key = "mi/ss/missinghash"
        index = {
            "version": 1,
            "entries": {
                "missing.txt": {
                    "hash": "missinghash",
                    "object_key": obj_key,
                    "remote": "r2"
                }
            }
        }
        vlfs.write_index(repo_root / ".vlfs", index)
        
        # Put object in local cache
        cache_dir = repo_root / ".vlfs-cache"
        obj_path = cache_dir / "objects" / obj_key
        obj_path.parent.mkdir(parents=True, exist_ok=True)
        obj_path.write_bytes(b"compressed data")
        
        # Mock rclone: lsjson returns empty (missing), copyto succeeds
        rclone_mock({
            "lsjson": (0, "[]", ""),
            "copyto": (0, "", ""),
            "sync": (0, "", "")
        })
        
        # Run repair
        vlfs.cmd_repair(repo_root, repo_root / ".vlfs", cache_dir)
        
        captured = capsys.readouterr()
        assert "Re-uploading missing.txt" in captured.out
        assert "Fixed 1 missing remote objects" in captured.out
        assert "Repair complete!" in captured.out

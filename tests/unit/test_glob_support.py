import pytest
from pathlib import Path
from unittest.mock import patch, MagicMock
import vlfs
import json
import os

@pytest.fixture
def repo_with_files(tmp_path):
    """Create a repo with some files for testing globs."""
    repo_root = tmp_path
    (repo_root / ".vlfs").mkdir()
    (repo_root / ".vlfs" / "config.toml").write_text("")
    
    # Create structure
    (repo_root / "images").mkdir()
    (repo_root / "images" / "a.png").write_text("content a")
    (repo_root / "images" / "b.png").write_text("content b")
    (repo_root / "images" / "c.txt").write_text("content c")
    
    (repo_root / "src").mkdir()
    (repo_root / "src" / "main.py").write_text("print('hello')")
    (repo_root / "src" / "util.py").write_text("pass")
    
    return repo_root

def test_resolve_targets(repo_with_files, monkeypatch):
    monkeypatch.chdir(repo_with_files)
    
    # Case 1: Wildcard
    targets = vlfs.resolve_targets("images/*.png")
    names = sorted([t.name for t in targets])
    assert names == ["a.png", "b.png"]
    
    # Case 2: Recursive
    targets = vlfs.resolve_targets("**/*.py")
    names = sorted([t.name for t in targets])
    assert names == ["main.py", "util.py"]
    
    # Case 3: Exact
    targets = vlfs.resolve_targets("images/c.txt")
    assert len(targets) == 1
    assert targets[0].name == "c.txt"

def test_cmd_push_glob(repo_with_files, monkeypatch):
    vlfs_dir = repo_with_files / ".vlfs"
    cache_dir = repo_with_files / ".vlfs-cache"
    
    monkeypatch.chdir(repo_with_files)
    
    with (
        patch("vlfs.ensure_r2_auth", return_value=0),
        patch("vlfs.validate_r2_connection"),
        patch("vlfs._push_single_file_collect") as mock_push,
        patch("vlfs.update_index_entries"),
        patch("vlfs.Path.cwd", return_value=repo_with_files)
    ):
        mock_push.return_value = (0, {"some": "entry"})
        
        ret = vlfs.cmd_push(
            repo_root=repo_with_files,
            vlfs_dir=vlfs_dir,
            cache_dir=cache_dir,
            paths=["images/*.png"],
            private=False,
            dry_run=False
        )
        
        assert ret == 0
        assert mock_push.call_count == 2
        
def test_cmd_remove_glob_filesystem(repo_with_files):
    vlfs_dir = repo_with_files / ".vlfs"
    cache_dir = repo_with_files / ".vlfs-cache"
    
    index = {
        "entries": {
            "images/a.png": {"object_key": "k1"},
            "images/b.png": {"object_key": "k2"},
            "images/c.txt": {"object_key": "k3"}
        }
    }
    with open(vlfs_dir / "index.json", "w") as f:
        json.dump(index, f)
        
    with (
        patch("vlfs.read_index", return_value=index),
        patch("vlfs.write_index") as mock_write,
        patch("builtins.input", return_value="y"),
        patch("vlfs.delete_from_remote"),
        patch("vlfs.Path.cwd", return_value=repo_with_files)
    ):
        ret = vlfs.cmd_remove(
            repo_root=repo_with_files,
            vlfs_dir=vlfs_dir,
            cache_dir=cache_dir,
            paths=["images/*.png"],
            delete_file=True
        )
        
        assert ret == 0
        
        args = mock_write.call_args
        saved_entries = args[0][1]["entries"]
        assert "images/a.png" not in saved_entries
        assert "images/b.png" not in saved_entries
        assert "images/c.txt" in saved_entries

def test_cmd_remove_glob_missing_files(repo_with_files):
    """Test removing files that are tracked but missing from disk."""
    vlfs_dir = repo_with_files / ".vlfs"
    cache_dir = repo_with_files / ".vlfs-cache"
    
    # Delete from disk
    (repo_with_files / "images" / "a.png").unlink()
    
    index = {
        "entries": {
            "images/a.png": {"object_key": "k1"},
            "images/b.png": {"object_key": "k2"}
        }
    }
    
    with (
        patch("vlfs.read_index", return_value=index),
        patch("vlfs.write_index") as mock_write,
        patch("builtins.input", return_value="y"),
        patch("vlfs.delete_from_remote"),
        patch("vlfs.Path.cwd", return_value=repo_with_files)
    ):
        # remove images/*.png
        # images/a.png is missing from disk, so resolve_targets won't find it.
        # But our fallback logic should match it in index.
        ret = vlfs.cmd_remove(
            repo_root=repo_with_files,
            vlfs_dir=vlfs_dir,
            cache_dir=cache_dir,
            paths=["images/*.png"],
            delete_file=True
        )
        
        assert ret == 0
        args = mock_write.call_args
        saved_entries = args[0][1]["entries"]
        assert "images/a.png" not in saved_entries # Should be removed via index match
        assert "images/b.png" not in saved_entries

def test_cmd_ls_pattern(repo_with_files, capsys):
    vlfs_dir = repo_with_files / ".vlfs"
    
    index = {
        "entries": {
            "images/a.png": {"hash": "h1"},
            "images/b.png": {"hash": "h2"},
            "images/c.txt": {"hash": "h3"}
        }
    }
    
    with (
        patch("vlfs.read_index", return_value=index),
        patch("vlfs.Path.cwd", return_value=repo_with_files)
    ):
        vlfs.cmd_list(repo_with_files, vlfs_dir, pattern="*.png")
        
        out = capsys.readouterr().out
        assert "images/a.png" in out
        assert "images/b.png" in out
        assert "images/c.txt" not in out

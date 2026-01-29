import pytest
from pathlib import Path
import hashlib
import zstandard
import vlfs
import os

# Helper to create a fake object in cache
def create_cached_object(cache_dir, content, compression_level=3):
    cctx = zstandard.ZstdCompressor(level=compression_level)
    compressed = cctx.compress(content)
    
    sha = hashlib.sha256(content).hexdigest().lower()
    object_key = f"{sha[:2]}/{sha[2:4]}/{sha}"
    
    obj_path = cache_dir / 'objects' / object_key
    obj_path.parent.mkdir(parents=True, exist_ok=True)
    obj_path.write_bytes(compressed)
    
    return object_key, sha

def test_materialize_safety(tmp_path):
    repo_root = tmp_path / "repo"
    cache_dir = tmp_path / "cache"
    repo_root.mkdir()
    (cache_dir / "objects").mkdir(parents=True)
    
    # Create a "remote" file content
    target_content = b"version 2"
    obj_key, target_hash = create_cached_object(cache_dir, target_content)
    
    index = {
        'version': 1,
        'entries': {
            'file.bin': {
                'hash': target_hash,
                'object_key': obj_key,
                'size': len(target_content)
            }
        }
    }
    
    # Scenario 1: File doesn't exist -> Should write
    written, bytes_w, skipped = vlfs.materialize_workspace(index, repo_root, cache_dir)
    assert written == 1
    assert skipped == []
    assert (repo_root / "file.bin").read_bytes() == target_content
    
    # Scenario 2: File exists and matches -> Should skip (optimization)
    # Reset stats
    written, bytes_w, skipped = vlfs.materialize_workspace(index, repo_root, cache_dir)
    assert written == 0
    assert skipped == []
    
    # Scenario 3: File exists and DIFFERENT (local mod) -> Should skip (safety)
    local_content = b"version 1 modified"
    (repo_root / "file.bin").write_bytes(local_content)
    
    written, bytes_w, skipped = vlfs.materialize_workspace(index, repo_root, cache_dir)
    assert written == 0
    assert skipped == ['file.bin']
    assert (repo_root / "file.bin").read_bytes() == local_content # Preserved
    
    # Scenario 4: Force overwrite
    written, bytes_w, skipped = vlfs.materialize_workspace(index, repo_root, cache_dir, force=True)
    assert written == 1
    assert skipped == []
    assert (repo_root / "file.bin").read_bytes() == target_content # Overwritten

def test_materialize_nested_path(tmp_path):
    repo_root = tmp_path / "repo"
    cache_dir = tmp_path / "cache"
    repo_root.mkdir()
    (cache_dir / "objects").mkdir(parents=True)
    
    target_content = b"nested content"
    obj_key, target_hash = create_cached_object(cache_dir, target_content)
    
    index = {
        'version': 1,
        'entries': {
            'folder/subfolder/file.bin': { # Forward slashes
                'hash': target_hash,
                'object_key': obj_key,
                'size': len(target_content)
            }
        }
    }
    
    # Should handle path creation regardless of OS
    written, bytes_w, skipped = vlfs.materialize_workspace(index, repo_root, cache_dir)
    assert written == 1
    
    expected_path = repo_root / "folder" / "subfolder" / "file.bin"
    assert expected_path.exists()
    assert expected_path.read_bytes() == target_content

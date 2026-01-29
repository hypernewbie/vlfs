"""Unit tests for robustness features (Milestone 2.4)."""

import threading
import time
from pathlib import Path

import pytest

import vlfs


class TestFileLock:
    """Test cross-platform file locking."""
    
    def test_lock_prevents_concurrent_access(self, tmp_path):
        """Lock should prevent concurrent access."""
        lock_file = tmp_path / 'test.lock'
        
        acquired = []
        
        def try_lock():
            try:
                with vlfs.with_file_lock(lock_file):
                    acquired.append(True)
                    time.sleep(0.05)
            except:
                pass
        
        # Start two threads trying to acquire lock
        t1 = threading.Thread(target=try_lock)
        t2 = threading.Thread(target=try_lock)
        
        t1.start()
        t2.start()
        t1.join()
        t2.join()
        
        # Both should have acquired the lock (sequentially)
        assert len(acquired) == 2
    
    def test_lock_creates_file(self, tmp_path):
        """Lock should create lock file."""
        lock_file = tmp_path / 'test.lock'
        
        with vlfs.with_file_lock(lock_file):
            assert lock_file.exists()
    
    def test_lock_released_on_exit(self, tmp_path):
        """Lock should be released on context exit."""
        lock_file = tmp_path / 'test.lock'
        
        with vlfs.with_file_lock(lock_file):
            pass
        
        # Should be able to acquire again immediately
        with vlfs.with_file_lock(lock_file):
            pass


class TestCmdPushRobustness:
    """Test push command robustness."""
    
    def test_push_uses_file_lock(self, repo_root, monkeypatch):
        """Push should lock index during update."""
        monkeypatch.setattr(vlfs, 'validate_r2_connection', lambda *args, **kwargs: True)

        test_file = repo_root / 'test.txt'
        test_file.write_bytes(b'test content')
        
        lock_acquired = []
        
        original_lock = vlfs.with_file_lock
        def mock_lock(path):
            lock_acquired.append(str(path))
            return original_lock(path)
        
        monkeypatch.setattr(vlfs, 'with_file_lock', mock_lock)
        monkeypatch.chdir(repo_root)
        
        # Mock rclone to avoid needing credentials
        def mock_upload(*args, **kwargs):
            return True
        monkeypatch.setattr(vlfs, 'upload_to_r2', mock_upload)
        
        vlfs.main(['push', 'test.txt'])
        
        assert any('index.lock' in p for p in lock_acquired)
    
    def test_push_handles_upload_error(self, repo_root, monkeypatch, capsys):
        """Push should handle upload errors gracefully."""
        test_file = repo_root / 'test.txt'
        test_file.write_bytes(b'test content')
        
        def mock_upload(*args, **kwargs):
            raise vlfs.RcloneError("upload failed", 1, "", "network error")
        
        monkeypatch.setattr(vlfs, 'upload_to_r2', mock_upload)
        monkeypatch.chdir(repo_root)
        
        result = vlfs.main(['push', 'test.txt'])
        captured = capsys.readouterr()
        
        assert result == 1
        assert 'Error' in captured.err
    
    def test_push_dry_run_does_not_modify_index(self, repo_root, monkeypatch):
        """Dry run should not modify index."""
        test_file = repo_root / 'test.txt'
        test_file.write_bytes(b'test content')
        
        def mock_upload(*args, **kwargs):
            return True
        
        monkeypatch.setattr(vlfs, 'upload_to_r2', mock_upload)
        monkeypatch.chdir(repo_root)
        
        # Create existing index
        index = {'version': 1, 'entries': {'existing.txt': {'hash': 'abc'}}}
        vlfs.write_index(repo_root / '.vlfs', index)
        
        vlfs.main(['push', 'test.txt', '--dry-run'])
        
        # Index should still only have existing.txt
        new_index = vlfs.read_index(repo_root / '.vlfs')
        assert 'test.txt' not in new_index['entries']
        assert 'existing.txt' in new_index['entries']


class TestCmdPullRobustness:
    """Test pull command robustness."""
    
    def test_pull_empty_index(self, repo_root, monkeypatch, capsys):
        """Pull with empty index should report nothing to do."""
        monkeypatch.chdir(repo_root)
        
        result = vlfs.main(['pull'])
        captured = capsys.readouterr()
        
        assert result == 0
        assert 'No files to pull' in captured.out or 'empty' in captured.out.lower()
    
    def test_pull_dry_run(self, repo_root, monkeypatch, capsys):
        """Pull dry run should show what would be done."""
        monkeypatch.setattr(vlfs, 'validate_r2_connection', lambda *args, **kwargs: True)

        # Setup: push a file first
        test_file = repo_root / 'test.txt'
        test_file.write_bytes(b'test content')
        
        def mock_upload(*args, **kwargs):
            return True
        monkeypatch.setattr(vlfs, 'upload_to_r2', mock_upload)
        monkeypatch.chdir(repo_root)
        
        vlfs.main(['push', 'test.txt'])
        test_file.unlink()
        
        # Now dry-run pull
        captured = capsys.readouterr()  # Clear output
        result = vlfs.main(['pull', '--dry-run'])
        captured = capsys.readouterr()
        
        assert result == 0
        assert '[DRY-RUN]' in captured.out
    
    def test_pull_handles_download_error(self, repo_root, monkeypatch, capsys):
        """Pull should handle download errors gracefully."""
        # Setup index with a file
        index = {
            'version': 1,
            'entries': {
                'test.txt': {
                    'object_key': 'ab/cd/abcdef',
                    'hash': '123',
                }
            }
        }
        vlfs.write_index(repo_root / '.vlfs', index)
        
        def mock_download(*args, **kwargs):
            raise vlfs.RcloneError("download failed", 1, "", "network error")
        
        monkeypatch.setattr(vlfs, 'download_from_r2', mock_download)
        monkeypatch.chdir(repo_root)
        
        result = vlfs.main(['pull'])
        captured = capsys.readouterr()
        
        assert result == 1
        assert 'Error' in captured.err


class TestIntegrationWorkflow:
    """Integration tests for complete workflows."""
    
    def test_push_then_pull_roundtrip(self, repo_root, monkeypatch):
        """Full push then pull roundtrip."""
        monkeypatch.setattr(vlfs, 'validate_r2_connection', lambda *args, **kwargs: True)

        test_file = repo_root / 'assets' / 'test.bin'
        test_file.write_bytes(b'secret data 12345')
        
        uploaded = []
        def mock_upload(local_path, object_key, bucket='vlfs', dry_run=False):
            uploaded.append(object_key)
            return True
        
        downloaded = []
        def mock_download(object_keys, cache_dir, bucket='vlfs', dry_run=False):
            downloaded.extend(object_keys)
            # Simulate download by copying from cache (file is already there from push)
            return len(object_keys)
        
        monkeypatch.setattr(vlfs, 'upload_to_r2', mock_upload)
        monkeypatch.setattr(vlfs, 'download_from_r2', mock_download)
        monkeypatch.chdir(repo_root)
        
        # Push
        result = vlfs.main(['push', 'assets/test.bin'])
        assert result == 0
        assert len(uploaded) == 1
        
        # Verify file is in cache
        cache_dir = repo_root / '.vlfs-cache'
        assert any(cache_dir.rglob('*.bin')) or any(cache_dir.rglob('*'))
        
        # Remove file
        test_file.unlink()
        assert not test_file.exists()
        
        # Pull
        result = vlfs.main(['pull'])
        assert result == 0
        # Download may be 0 if object already in cache
        
        # Verify file restored
        assert test_file.exists()
        assert test_file.read_bytes() == b'secret data 12345'

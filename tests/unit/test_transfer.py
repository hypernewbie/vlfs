"""Unit tests for upload/download to R2 (Milestone 2.2 and 2.3)."""

from pathlib import Path

import pytest

import vlfs


class TestUploadToR2:
    """Test upload_to_r2 function."""
    
    def test_uploads_new_file(self, tmp_path, rclone_mock):
        """Should upload file that doesn't exist remotely."""
        local_file = tmp_path / 'test.txt'
        local_file.write_bytes(b'test content')
        
        mock = rclone_mock({
            'ls': (0, '', ''),  # Object doesn't exist
            'copyto': (0, '', ''),
        })
        
        result = vlfs.upload_to_r2(local_file, 'ab/cd/abcdef')
        
        assert result is True
        # Should have called ls then copyto
        assert len([c for c in mock['calls'] if c[1] == 'copyto']) == 1
    
    def test_skips_existing_file(self, tmp_path, rclone_mock):
        """Should skip upload if object already exists."""
        local_file = tmp_path / 'test.txt'
        local_file.write_bytes(b'test content')
        
        mock = rclone_mock({
            'ls': (0, '-rw-r--r-- 1 user group 12 Jan 1 00:00 file', ''),
        })
        
        result = vlfs.upload_to_r2(local_file, 'ab/cd/abcdef')
        
        assert result is True
        # Should not have called copyto
        assert len([c for c in mock['calls'] if c[1] == 'copyto']) == 0
    
    def test_dry_run_does_not_upload(self, tmp_path, rclone_mock, capsys):
        """Dry run should print but not upload."""
        local_file = tmp_path / 'test.txt'
        local_file.write_bytes(b'test content')
        
        mock = rclone_mock({
            'ls': (0, '', ''),
            'copy': (0, '', ''),
        })
        
        result = vlfs.upload_to_r2(local_file, 'ab/cd/abcdef', dry_run=True)
        captured = capsys.readouterr()
        
        assert result is True
        assert '[DRY-RUN]' in captured.out
        assert len([c for c in mock['calls'] if c[1] == 'copy']) == 0
    
    def test_retries_on_failure(self, tmp_path, rclone_mock):
        """Should retry on transient failures."""
        local_file = tmp_path / 'test.txt'
        local_file.write_bytes(b'test content')
        
        call_count = [0]
        
        def handler(cmd):
            if cmd[1] == 'ls':
                return (0, '', '')  # Object doesn't exist
            elif cmd[1] == 'copyto':
                call_count[0] += 1
                if call_count[0] < 2:
                    raise vlfs.RcloneError("transient", 1, "", "")
                return (0, '', '')
        
        rclone_mock({'_handler': handler})
        
        # Should succeed after retry
        result = vlfs.upload_to_r2(local_file, 'ab/cd/abcdef')
        
        assert result is True
        assert call_count[0] == 2


class TestDownloadFromR2:
    """Test download_from_r2 function."""
    
    def test_downloads_objects(self, tmp_path, rclone_mock, monkeypatch):
        """Should download list of objects."""
        cache_dir = tmp_path / 'cache'

        # Provide dummy credentials
        monkeypatch.setenv('RCLONE_CONFIG_R2_ACCESS_KEY_ID', 'dummy')
        monkeypatch.setenv('RCLONE_CONFIG_R2_SECRET_ACCESS_KEY', 'dummy')
        monkeypatch.setenv('RCLONE_CONFIG_R2_ENDPOINT', 'https://example.com')
        
        mock = rclone_mock({
            'copy': (0, '', ''),
        })
        
        object_keys = ['ab/cd/obj1', 'ef/gh/obj2']
        result = vlfs.download_from_r2(object_keys, cache_dir)
        
        assert result == 2
        # Should have called copy once with files-from
        copy_calls = [c for c in mock['calls'] if c[1] == 'copy']
        assert len(copy_calls) == 1
        assert '--files-from' in copy_calls[0]
        assert '--transfers' in copy_calls[0]
    
    def test_empty_list_returns_zero(self, tmp_path, rclone_mock):
        """Empty list should return 0."""
        cache_dir = tmp_path / 'cache'
        
        result = vlfs.download_from_r2([], cache_dir)
        
        assert result == 0
    
    def test_dry_run_does_not_download(self, tmp_path, rclone_mock, capsys):
        """Dry run should print but not download."""
        cache_dir = tmp_path / 'cache'
        
        mock = rclone_mock({
            'copy': (0, '', ''),
        })
        
        object_keys = ['ab/cd/obj1']
        result = vlfs.download_from_r2(object_keys, cache_dir, dry_run=True)
        captured = capsys.readouterr()
        
        assert result == 1
        assert '[DRY-RUN]' in captured.out
        assert len([c for c in mock['calls'] if c[1] == 'copy']) == 0


class TestComputeMissingObjects:
    """Test compute_missing_objects function."""
    
    def test_returns_missing_objects(self, tmp_path):
        """Should return objects not in cache."""
        cache_dir = tmp_path / 'cache'
        cache_dir.mkdir()
        (cache_dir / 'objects').mkdir()
        
        # Create one object in cache
        (cache_dir / 'objects' / 'ab').mkdir(parents=True)
        (cache_dir / 'objects' / 'ab' / 'cd').mkdir()
        (cache_dir / 'objects' / 'ab' / 'cd' / 'abcdef').write_bytes(b'data')
        
        index = {
            'version': 1,
            'entries': {
                'file1.txt': {'object_key': 'ab/cd/abcdef'},
                'file2.txt': {'object_key': 'ef/gh/efghij'},
            }
        }
        
        missing = vlfs.compute_missing_objects(index, cache_dir)
        
        assert 'ef/gh/efghij' in missing
        assert 'ab/cd/abcdef' not in missing
    
    def test_deduplicates(self, tmp_path):
        """Should deduplicate object keys."""
        cache_dir = tmp_path / 'cache'
        cache_dir.mkdir()
        
        index = {
            'version': 1,
            'entries': {
                'file1.txt': {'object_key': 'ab/cd/abc'},
                'file2.txt': {'object_key': 'ab/cd/abc'},  # Same key
            }
        }
        
        missing = vlfs.compute_missing_objects(index, cache_dir)
        
        assert missing == ['ab/cd/abc']  # Only once
    
    def test_empty_index(self, tmp_path):
        """Empty index should return empty list."""
        cache_dir = tmp_path / 'cache'
        cache_dir.mkdir()
        
        index = {'version': 1, 'entries': {}}
        
        missing = vlfs.compute_missing_objects(index, cache_dir)
        
        assert missing == []


class TestMaterializeWorkspace:
    """Test materialize_workspace function."""
    
    def test_writes_files_from_cache(self, repo_root):
        """Should decompress and write files."""
        cache_dir = repo_root / '.vlfs-cache'
        
        # Store an object
        test_file = repo_root / 'test.txt'
        test_file.write_bytes(b'test content')
        object_key = vlfs.store_object(test_file, cache_dir)
        test_file.unlink()
        
        index = {
            'version': 1,
            'entries': {
                'test.txt': {
                    'object_key': object_key,
                    'hash': vlfs.hash_file(test_file)[0] if test_file.exists() else 'abc',
                }
            }
        }
        
        files_written, bytes_written, _ = vlfs.materialize_workspace(index, repo_root, cache_dir)
        
        assert files_written == 1
        assert (repo_root / 'test.txt').exists()
        assert (repo_root / 'test.txt').read_bytes() == b'test content'
    
    def test_skips_unchanged_files(self, repo_root):
        """Should skip files that match hash."""
        cache_dir = repo_root / '.vlfs-cache'
        
        # Create file and index
        test_file = repo_root / 'test.txt'
        test_file.write_bytes(b'static content')
        hex_digest, size, mtime = vlfs.hash_file(test_file)
        object_key = vlfs.store_object(test_file, cache_dir)
        
        index = {
            'version': 1,
            'entries': {
                'test.txt': {
                    'object_key': object_key,
                    'hash': hex_digest,
                    'size': size,
                    'mtime': mtime,
                }
            }
        }
        
        files_written, _, _ = vlfs.materialize_workspace(index, repo_root, cache_dir)
        
        assert files_written == 0  # Already up to date
    
    def test_dry_run_does_not_write(self, repo_root, capsys):
        """Dry run should not write files."""
        cache_dir = repo_root / '.vlfs-cache'
        
        # Store an object
        test_file = repo_root / 'test.txt'
        test_file.write_bytes(b'test content')
        object_key = vlfs.store_object(test_file, cache_dir)
        test_file.unlink()
        
        index = {
            'version': 1,
            'entries': {
                'test.txt': {
                    'object_key': object_key,
                    'hash': 'abc',
                }
            }
        }
        
        files_written, _, _ = vlfs.materialize_workspace(index, repo_root, cache_dir, dry_run=True)
        captured = capsys.readouterr()
        
        assert files_written == 1  # Counted but not written
        assert '[DRY-RUN]' in captured.out
        assert not (repo_root / 'test.txt').exists()
    
    def test_handles_missing_cache_objects(self, repo_root):
        """Should skip files with missing cache objects."""
        cache_dir = repo_root / '.vlfs-cache'
        
        index = {
            'version': 1,
            'entries': {
                'missing.txt': {
                    'object_key': 'xx/yy/zzzz',
                    'hash': 'abc',
                }
            }
        }
        
        files_written, _, _ = vlfs.materialize_workspace(index, repo_root, cache_dir)
        
        assert files_written == 0  # Skipped missing object

    def test_pull_restore_skips_downloads(self, repo_root, monkeypatch, rclone_mock):
        """pull --restore should materialize from cache and NOT call rclone."""
        vlfs_dir = repo_root / '.vlfs'
        cache_dir = repo_root / '.vlfs-cache'
        
        # Setup cache with an object
        test_file = repo_root / 'restored.txt'
        content = b'content'
        test_file.write_bytes(content)
        
        # Get hash before unlinking
        real_hash, _, _ = vlfs.hash_file(test_file)
        
        object_key = vlfs.store_object(test_file, cache_dir)
        test_file.unlink() # Workspace file is gone
        
        index = {
            'version': 1,
            'entries': {
                'restored.txt': {
                    'object_key': object_key,
                    'hash': real_hash,
                    'remote': 'r2'
                }
            }
        }
        vlfs.write_index(vlfs_dir, index)
        
        # Mock rclone to fail if called
        def fail_handler(cmd):
            # Allow ls for checking existence if needed by other parts, 
            # but cmd_pull with --restore shouldn't call it.
            pytest.fail(f"rclone should NOT be called during pull --restore! Called: {cmd}")
            
        rclone_mock({'_handler': fail_handler})
        
        # Run pull --restore
        monkeypatch.chdir(repo_root)
        result = vlfs.main(['pull', '--restore'])
        
        assert result == 0
        assert (repo_root / 'restored.txt').exists()
        assert (repo_root / 'restored.txt').read_bytes() == content

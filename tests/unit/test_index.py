"""Unit tests for index management (Milestone 1.3)."""

import json
from pathlib import Path

import pytest

import vlfs


class TestReadIndex:
    """Test reading index.json."""
    
    def test_missing_index_returns_default(self, tmp_path):
        """Missing index should return default structure."""
        vlfs_dir = tmp_path / '.vlfs'
        
        index = vlfs.read_index(vlfs_dir)
        
        assert index['version'] == 1
        assert index['entries'] == {}
    
    def test_reads_existing_index(self, tmp_path):
        """Should read existing index file."""
        vlfs_dir = tmp_path / '.vlfs'
        vlfs_dir.mkdir()
        index_file = vlfs_dir / 'index.json'
        index_file.write_text(json.dumps({'version': 1, 'entries': {'test.txt': {'hash': 'abc'}}}))
        
        index = vlfs.read_index(vlfs_dir)
        
        assert index['entries']['test.txt']['hash'] == 'abc'
    
    def test_version_guard_rejects_unknown(self, tmp_path):
        """Should raise on unsupported version."""
        vlfs_dir = tmp_path / '.vlfs'
        vlfs_dir.mkdir()
        index_file = vlfs_dir / 'index.json'
        index_file.write_text(json.dumps({'version': 999, 'entries': {}}))
        
        with pytest.raises(ValueError, match='Unsupported index version'):
            vlfs.read_index(vlfs_dir)


class TestWriteIndex:
    """Test writing index.json."""
    
    def test_creates_file(self, tmp_path):
        """Should create index.json."""
        vlfs_dir = tmp_path / '.vlfs'
        data = {'version': 1, 'entries': {'file.txt': {'hash': 'abc123'}}}
        
        vlfs.write_index(vlfs_dir, data)
        
        index_file = vlfs_dir / 'index.json'
        assert index_file.exists()
    
    def test_roundtrip(self, tmp_path):
        """Write then read should preserve data."""
        vlfs_dir = tmp_path / '.vlfs'
        original = {'version': 1, 'entries': {'path/to/file.txt': {'hash': 'def456', 'size': 100}}}
        
        vlfs.write_index(vlfs_dir, original)
        loaded = vlfs.read_index(vlfs_dir)
        
        assert loaded == original
    
    def test_atomic_write(self, tmp_path):
        """Should not leave partial files on error."""
        vlfs_dir = tmp_path / '.vlfs'
        # Make directory read-only to force error
        vlfs_dir.mkdir()
        
        # This is hard to test without mocking, but we can at least verify
        # the file doesn't exist if we raise before completion
        # (The actual atomic test would require filesystem failure injection)


class TestComputeStatus:
    """Test status computation."""
    
    def test_empty_index_up_to_date(self, repo_root):
        """Empty index should show no changes."""
        index = {'version': 1, 'entries': {}}
        
        status = vlfs.compute_status(index, repo_root)
        
        assert status['missing'] == []
        assert status['modified'] == []
        assert status['extra'] == []
    
    def test_detects_missing_file(self, repo_root):
        """Should detect missing indexed files."""
        index = {'version': 1, 'entries': {'missing.txt': {'hash': 'abc', 'size': 10, 'mtime': 12345}}}
        
        status = vlfs.compute_status(index, repo_root)
        
        assert 'missing.txt' in status['missing']
    
    def test_detects_modified_size(self, repo_root):
        """Should detect modified files by size."""
        # Create file
        test_file = repo_root / 'test.txt'
        test_file.write_text('original content')
        
        # Index with wrong size
        index = {'version': 1, 'entries': {'test.txt': {
            'hash': 'wrong',
            'size': 999,
            'mtime': test_file.stat().st_mtime
        }}}
        
        status = vlfs.compute_status(index, repo_root)
        
        assert 'test.txt' in status['modified']
    
    def test_unchanged_file_not_modified(self, repo_root):
        """Unchanged files should not appear as modified."""
        # Create and index file
        test_file = repo_root / 'test.txt'
        test_file.write_text('stable content')
        
        hex_digest, size, mtime = vlfs.hash_file(test_file)
        index = {'version': 1, 'entries': {'test.txt': {
            'hash': hex_digest,
            'size': size,
            'mtime': mtime
        }}}
        
        status = vlfs.compute_status(index, repo_root)
        
        assert 'test.txt' not in status['modified']
        assert 'test.txt' not in status['missing']
    
    def test_path_separator_normalization(self, repo_root):
        """Should handle path separators correctly."""
        # Create file in subdirectory (tools/ already exists from fixture)
        test_file = repo_root / 'tools' / 'clang.exe'
        test_file.write_text('binary content')
        
        # Index with forward slashes
        hex_digest, size, mtime = vlfs.hash_file(test_file)
        index = {'version': 1, 'entries': {'tools/clang.exe': {
            'hash': hex_digest,
            'size': size,
            'mtime': mtime
        }}}
        
        status = vlfs.compute_status(index, repo_root)
        
        # Should not be marked as missing or modified
        assert 'tools/clang.exe' not in status['missing']
        assert 'tools/clang.exe' not in status['modified']


class TestStatusCommand:
    """Test status CLI command."""
    
    def test_status_up_to_date(self, repo_root, capsys, monkeypatch):
        """Status should report up to date when no changes."""
        monkeypatch.chdir(repo_root)
        
        result = vlfs.main(['status'])
        captured = capsys.readouterr()
        
        assert result == 0
        assert 'up to date' in captured.out.lower()
    
    def test_status_shows_missing(self, repo_root, capsys, monkeypatch):
        """Status should report missing files."""
        # Create index with missing file (.vlfs/ already exists from fixture)
        vlfs_dir = repo_root / '.vlfs'
        index = {'version': 1, 'entries': {'gone.txt': {'hash': 'abc', 'size': 10, 'mtime': 1}}}
        (vlfs_dir / 'index.json').write_text(json.dumps(index))
        
        monkeypatch.chdir(repo_root)
        result = vlfs.main(['status'])
        captured = capsys.readouterr()
        
        assert result == 0
        assert 'Missing' in captured.out or 'missing' in captured.out
    
    def test_status_rejects_bad_version(self, repo_root, capsys, monkeypatch):
        """Status should error on unsupported index version."""
        vlfs_dir = repo_root / '.vlfs'
        bad_index = {'version': 999, 'entries': {}}
        (vlfs_dir / 'index.json').write_text(json.dumps(bad_index))
        
        monkeypatch.chdir(repo_root)
        result = vlfs.main(['status'])
        captured = capsys.readouterr()
        
        assert result == 1
        assert 'Error' in captured.err


class TestPushCommand:
    """Test push CLI command."""
    
    def test_push_stores_file(self, repo_root, monkeypatch, rclone_mock):
        """Push should store file in cache."""
        # Mock rclone to avoid needing credentials
        rclone_mock({
            'ls': (0, '', ''),  # Object doesn't exist
            'copy': (0, '', ''),
        })
        
        test_file = repo_root / 'assets' / 'texture.png'
        test_file.write_bytes(b'fake image data')

        monkeypatch.chdir(repo_root)
        result = vlfs.main(['push', 'assets/texture.png'])

        assert result == 0
        # Check cache was populated
        cache_dir = repo_root / '.vlfs-cache' / 'objects'
        assert any(cache_dir.rglob('*'))  # Something was created

    def test_push_creates_index_entry(self, repo_root, monkeypatch, rclone_mock):
        """Push should add entry to index."""
        # Mock rclone to avoid needing credentials
        rclone_mock({
            'ls': (0, '', ''),  # Object doesn't exist
            'copy': (0, '', ''),
        })
        
        test_file = repo_root / 'tools' / 'compiler.exe'
        test_file.write_bytes(b'fake binary')

        monkeypatch.chdir(repo_root)
        vlfs.main(['push', 'tools/compiler.exe'])
        
        index = vlfs.read_index(repo_root / '.vlfs')
        assert 'tools/compiler.exe' in index['entries']
        assert 'hash' in index['entries']['tools/compiler.exe']
        assert 'size' in index['entries']['tools/compiler.exe']
        assert 'mtime' in index['entries']['tools/compiler.exe']
        assert 'object_key' in index['entries']['tools/compiler.exe']
    
    def test_push_missing_file_errors(self, repo_root, capsys, monkeypatch):
        """Push of nonexistent file should error."""
        monkeypatch.chdir(repo_root)
        result = vlfs.main(['push', 'nonexistent.txt'])
        
        assert result == 1
        captured = capsys.readouterr()
        assert 'not found' in captured.err.lower() or 'Error' in captured.err

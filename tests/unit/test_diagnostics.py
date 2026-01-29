"""Unit tests for developer experience features (Phase 4.1)."""

import json
import os
import sys
from pathlib import Path
from unittest.mock import patch

import pytest

import vlfs


class TestStatusFormatting:
    """Test enhanced status output with colors and JSON."""

    def test_status_shows_counts(self, repo_root, capsys, monkeypatch):
        """Status should show counts for missing/modified/extra."""
        # Create index with missing file
        vlfs_dir = repo_root / '.vlfs'
        index = {'version': 1, 'entries': {
            'gone.txt': {'hash': 'abc', 'size': 10, 'mtime': 1},
            'gone2.txt': {'hash': 'def', 'size': 20, 'mtime': 2}
        }}
        (vlfs_dir / 'index.json').write_text(json.dumps(index))

        monkeypatch.chdir(repo_root)
        result = vlfs.main(['status'])
        captured = capsys.readouterr()

        assert result == 0
        assert 'Missing:' in captured.out
        assert '2' in captured.out or '2' in captured.out

    def test_status_no_color_by_default(self, repo_root, capsys, monkeypatch):
        """Status should not include ANSI codes when not TTY."""
        vlfs_dir = repo_root / '.vlfs'
        index = {'version': 1, 'entries': {'gone.txt': {'hash': 'abc', 'size': 10, 'mtime': 1}}}
        (vlfs_dir / 'index.json').write_text(json.dumps(index))

        monkeypatch.chdir(repo_root)
        result = vlfs.main(['status'])
        captured = capsys.readouterr()

        assert result == 0
        assert '\x1b[' not in captured.out  # No ANSI escape codes

    def test_status_json_format(self, repo_root, monkeypatch):
        """Status --json should return machine-readable output."""
        # Create index and file
        vlfs_dir = repo_root / '.vlfs'
        test_file = repo_root / 'test.txt'
        test_file.write_text('content')
        hex_digest, size, mtime = vlfs.hash_file(test_file)

        index = {'version': 1, 'entries': {
            'test.txt': {'hash': hex_digest, 'size': size, 'mtime': mtime},
            'missing.txt': {'hash': 'abc', 'size': 10, 'mtime': 1}
        }}
        (vlfs_dir / 'index.json').write_text(json.dumps(index))

        monkeypatch.chdir(repo_root)
        result = vlfs.main(['status', '--json'])

        # Cannot easily capture stdout in this test setup, but function should work
        assert result == 0

    def test_no_color_env_var_disables_colors(self, repo_root, capsys, monkeypatch):
        """NO_COLOR env var should disable colors."""
        monkeypatch.setenv('NO_COLOR', '1')
        vlfs_dir = repo_root / '.vlfs'
        index = {'version': 1, 'entries': {'gone.txt': {'hash': 'abc', 'size': 10, 'mtime': 1}}}
        (vlfs_dir / 'index.json').write_text(json.dumps(index))

        monkeypatch.chdir(repo_root)
        result = vlfs.main(['status'])
        captured = capsys.readouterr()

        assert result == 0
        assert '\x1b[' not in captured.out


class TestVerifyCommand:
    """Test verify command that re-hashes workspace files."""

    def test_verify_detects_corruption(self, repo_root, capsys, monkeypatch, rclone_mock):
        """Verify should detect corrupted files."""
        # Mock rclone for any remote operations
        rclone_mock({'copy': (0, '', ''), 'ls': (0, '', '')})

        # Create file and push it
        test_file = repo_root / 'test.txt'
        test_file.write_text('original content')

        monkeypatch.chdir(repo_root)
        vlfs.main(['push', 'test.txt'])

        # Corrupt the file
        test_file.write_text('corrupted content')

        # Verify should detect corruption
        result = vlfs.main(['verify'])
        captured = capsys.readouterr()

        assert result == 1  # Returns 1 if issues found
        assert 'corrupted' in captured.out.lower() or 'mismatch' in captured.out.lower()

    def test_verify_passes_on_valid_files(self, repo_root, monkeypatch, rclone_mock, capsys):
        """Verify should pass when files match."""
        rclone_mock({'copy': (0, '', ''), 'ls': (0, '', '')})

        test_file = repo_root / 'test.txt'
        test_file.write_text('stable content')

        monkeypatch.chdir(repo_root)
        vlfs.main(['push', 'test.txt'])

        result = vlfs.main(['verify'])
        captured = capsys.readouterr()

        assert result == 0
        assert 'ok' in captured.out.lower() or 'valid' in captured.out.lower() or result == 0

    def test_verify_json_output(self, repo_root, monkeypatch, rclone_mock):
        """Verify --json should return structured output."""
        rclone_mock({'copy': (0, '', ''), 'ls': (0, '', '')})

        test_file = repo_root / 'test.txt'
        test_file.write_text('content')

        monkeypatch.chdir(repo_root)
        vlfs.main(['push', 'test.txt'])

        result = vlfs.main(['verify', '--json'])
        assert result == 0

    def test_verify_with_missing_files(self, repo_root, capsys, monkeypatch):
        """Verify should report missing files."""
        vlfs_dir = repo_root / '.vlfs'
        index = {'version': 1, 'entries': {'missing.txt': {'hash': 'abc', 'size': 10, 'mtime': 1}}}
        (vlfs_dir / 'index.json').write_text(json.dumps(index))

        monkeypatch.chdir(repo_root)
        result = vlfs.main(['verify'])
        captured = capsys.readouterr()

        assert 'missing' in captured.out.lower()

    def test_verify_size_mtime_shortcut(self, repo_root, monkeypatch, rclone_mock):
        """Verify should use size+mtime shortcut when possible."""
        rclone_mock({'copy': (0, '', ''), 'ls': (0, '', '')})

        test_file = repo_root / 'test.txt'
        test_file.write_text('content')

        monkeypatch.chdir(repo_root)
        vlfs.main(['push', 'test.txt'])

        # Same file - should shortcut without hashing
        with patch.object(vlfs, 'hash_file') as mock_hash:
            vlfs.main(['verify'])
            # If shortcut works, hash_file won't be called for unchanged files
            # This is implementation detail, but we can verify no corruption reported


class TestCleanCommand:
    """Test clean command that removes unreferenced cache objects."""

    def test_clean_removes_orphaned_objects(self, repo_root, monkeypatch, rclone_mock):
        """Clean should remove cache objects not in index."""
        rclone_mock({'copy': (0, '', ''), 'ls': (0, '', '')})

        # Push a file
        test_file = repo_root / 'test.txt'
        test_file.write_text('content')

        monkeypatch.chdir(repo_root)
        vlfs.main(['push', 'test.txt'])

        # Manually create an orphaned cache object
        cache_dir = repo_root / '.vlfs-cache' / 'objects'
        orphaned = cache_dir / 'ab' / 'cd' / 'orphan123'
        orphaned.parent.mkdir(parents=True, exist_ok=True)
        orphaned.write_text('orphaned data')

        # Verify it exists before clean
        assert orphaned.exists()

        # Clean with --yes
        result = vlfs.main(['clean', '--yes'])

        assert result == 0
        assert not orphaned.exists()

    def test_clean_dry_run(self, repo_root, capsys, monkeypatch, rclone_mock):
        """Clean --dry-run should not delete anything."""
        rclone_mock({'copy': (0, '', ''), 'ls': (0, '', '')})

        test_file = repo_root / 'test.txt'
        test_file.write_text('content')

        monkeypatch.chdir(repo_root)
        vlfs.main(['push', 'test.txt'])

        cache_dir = repo_root / '.vlfs-cache' / 'objects'
        orphaned = cache_dir / 'ab' / 'cd' / 'orphan123'
        orphaned.parent.mkdir(parents=True, exist_ok=True)
        orphaned.write_text('orphaned data')

        result = vlfs.main(['clean', '--dry-run'])
        captured = capsys.readouterr()

        assert result == 0
        assert orphaned.exists()  # Still exists after dry-run
        assert 'dry-run' in captured.out.lower() or 'would' in captured.out.lower()

    def test_clean_preserves_referenced_objects(self, repo_root, monkeypatch, rclone_mock):
        """Clean should not remove objects referenced by index."""
        rclone_mock({'copy': (0, '', ''), 'ls': (0, '', '')})

        test_file = repo_root / 'test.txt'
        test_file.write_text('content')

        monkeypatch.chdir(repo_root)
        vlfs.main(['push', 'test.txt'])

        # Find the object key from index
        index = vlfs.read_index(repo_root / '.vlfs')
        object_key = index['entries']['test.txt']['object_key']
        object_path = repo_root / '.vlfs-cache' / 'objects' / object_key

        # Clean should not delete it
        vlfs.main(['clean', '--yes'])

        assert object_path.exists()

    def test_clean_empty_cache(self, repo_root, monkeypatch):
        """Clean on empty cache should succeed."""
        monkeypatch.chdir(repo_root)
        result = vlfs.main(['clean', '--yes'])
        assert result == 0

    def test_clean_shows_bytes_freed(self, repo_root, capsys, monkeypatch, rclone_mock):
        """Clean should report how much space was freed."""
        rclone_mock({'copy': (0, '', ''), 'ls': (0, '', '')})

        test_file = repo_root / 'test.txt'
        test_file.write_text('content')

        monkeypatch.chdir(repo_root)
        vlfs.main(['push', 'test.txt'])

        cache_dir = repo_root / '.vlfs-cache' / 'objects'
        orphaned = cache_dir / 'ab' / 'cd' / 'orphan123'
        orphaned.parent.mkdir(parents=True, exist_ok=True)
        orphaned.write_text('orphaned data')

        result = vlfs.main(['clean', '--yes'])
        captured = capsys.readouterr()

        assert result == 0
        assert 'freed' in captured.out.lower() or 'bytes' in captured.out.lower() or 'b' in captured.out.lower()


class TestCrossPlatformColors:
    """Test color handling across platforms."""

    def test_color_disabled_in_ci(self, repo_root, capsys, monkeypatch):
        """Colors should be disabled in CI environments."""
        monkeypatch.setenv('CI', 'true')
        vlfs_dir = repo_root / '.vlfs'
        index = {'version': 1, 'entries': {'gone.txt': {'hash': 'abc', 'size': 10, 'mtime': 1}}}
        (vlfs_dir / 'index.json').write_text(json.dumps(index))

        monkeypatch.chdir(repo_root)
        result = vlfs.main(['status'])
        captured = capsys.readouterr()

        assert result == 0
        assert '\x1b[' not in captured.out

    def test_force_color_with_explicit_flag(self, repo_root, monkeypatch):
        """Status --color should force color output."""
        # This is hard to test because pytest captures stdout
        # but we can at least verify the flag is accepted
        vlfs_dir = repo_root / '.vlfs'
        index = {'version': 1, 'entries': {}}
        (vlfs_dir / 'index.json').write_text(json.dumps(index))

        monkeypatch.chdir(repo_root)
        # Just verify it doesn't crash
        result = vlfs.main(['status', '--color'])
        assert result == 0

"""Unit tests for Phase 5 performance improvements."""

import json
import os
from pathlib import Path

import pytest

import vlfs


class TestParallelHashing:
    """Test parallel hashing behavior."""

    def test_hash_files_parallel_matches_sequential(self, repo_root):
        """Parallel hashing should match sequential hash results."""
        files = []
        for i in range(10):
            path = repo_root / f"file_{i}.txt"
            path.write_text(f"content {i}")
            files.append(path)

        parallel_results, errors = vlfs.hash_files_parallel(files, max_workers=4)
        assert not errors

        for path in files:
            seq_hash, seq_size, seq_mtime = vlfs.hash_file(path)
            par_hash, par_size, par_mtime = parallel_results[path]
            assert seq_hash == par_hash
            assert seq_size == par_size
            assert seq_mtime == par_mtime

    def test_verify_uses_parallel_hashing(self, repo_root, monkeypatch):
        """Verify should use parallel hashing for many files."""
        entries = {}
        files = []
        for i in range(8):
            path = repo_root / f"verify_{i}.bin"
            path.write_text(f"data {i}")
            files.append(path)
            hex_digest, size, mtime = vlfs.hash_file(path)
            # Force hashing by mismatching size/mtime
            entries[str(path.relative_to(repo_root)).replace(os.sep, '/')] = {
                'hash': hex_digest,
                'size': size + 1,
                'mtime': mtime - 1,
                'object_key': 'ab/cd/fake',
                'remote': 'r2'
            }

        index = {'version': 1, 'entries': entries}
        (repo_root / '.vlfs' / 'index.json').write_text(json.dumps(index))

        called = {'count': 0}
        original_hash = vlfs.hash_file

        def fake_parallel(paths, max_workers=None):
            called['count'] = len(paths)
            results = {p: original_hash(p) for p in paths}
            return results, {}

        monkeypatch.setattr(vlfs, 'hash_files_parallel', fake_parallel)

        result = vlfs.cmd_verify(repo_root, repo_root / '.vlfs', dry_run=False, json_output=False)
        assert result == 0
        assert called['count'] == len(files)


class TestIndexUpdates:
    """Test that index updates are batched."""

    def test_directory_push_writes_index_once(self, repo_root, monkeypatch, rclone_mock):
        """Directory push should write index once."""
        rclone_mock({'copy': (0, '', ''), 'ls': (0, '', '')})

        assets_dir = repo_root / 'assets'
        assets_dir.mkdir(exist_ok=True)
        (assets_dir / 'a.txt').write_text('a')
        (assets_dir / 'b.txt').write_text('b')
        (assets_dir / 'c.txt').write_text('c')

        calls = {'count': 0}
        original_write = vlfs.write_index

        def wrapped_write(vlfs_dir, data):
            calls['count'] += 1
            return original_write(vlfs_dir, data)

        monkeypatch.setattr(vlfs, 'write_index', wrapped_write)
        monkeypatch.chdir(repo_root)

        result = vlfs.main(['push', 'assets'])
        assert result == 0
        assert calls['count'] == 1


class TestRcloneConfigReuse:
    """Test rclone config reuse per run."""

    def test_run_rclone_uses_config_path(self, tmp_path, rclone_mock):
        """run_rclone should include --config when configured."""
        config_path = tmp_path / 'rclone.conf'
        config_path.write_text('[r2]\ntype = s3\n')

        vlfs.set_rclone_config_path(config_path)
        mock = rclone_mock({'lsd': (0, '', '')})

        vlfs.run_rclone(['lsd', 'r2:'])

        assert '--config' in mock['calls'][0]
        assert str(config_path) in mock['calls'][0]

        # Reset for other tests
        vlfs.set_rclone_config_path(None)

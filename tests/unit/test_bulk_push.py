"""Unit tests for bulk push operations (Phase 4.3)."""

import json
import os
from pathlib import Path

import pytest

import vlfs


class TestRecursivePush:
    """Test pushing directories recursively."""

    def test_push_directory_recursive(self, repo_root, monkeypatch, rclone_mock):
        """Push directory should upload all files recursively."""
        rclone_mock({'copy': (0, '', ''), 'ls': (0, '', '')})

        # Create directory structure
        assets_dir = repo_root / 'assets' / 'textures'
        assets_dir.mkdir(parents=True)
        (assets_dir / 'wood.png').write_text('wood texture')
        (assets_dir / 'metal.png').write_text('metal texture')
        (repo_root / 'assets' / 'readme.txt').write_text('readme')

        monkeypatch.chdir(repo_root)
        result = vlfs.main(['push', 'assets'])

        assert result == 0
        index = vlfs.read_index(repo_root / '.vlfs')
        assert len(index['entries']) >= 3  # All files indexed

    def test_push_directory_ignores_vlfs_dirs(self, repo_root, monkeypatch, rclone_mock):
        """Push directory should ignore .vlfs/ and .vlfs-cache/."""
        rclone_mock({'copy': (0, '', ''), 'ls': (0, '', '')})

        # Create files that should be ignored
        (repo_root / '.vlfs' / 'config.toml').write_text('[test]')
        (repo_root / '.vlfs-cache' / 'temp').write_text('temp')
        (repo_root / 'actual.txt').write_text('actual')

        monkeypatch.chdir(repo_root)
        result = vlfs.main(['push', '.'])

        assert result == 0
        index = vlfs.read_index(repo_root / '.vlfs')
        # Should only have actual.txt, not vlfs internal files
        entry_paths = list(index['entries'].keys())
        assert all('.vlfs' not in p for p in entry_paths)
        assert all('.vlfs-cache' not in p for p in entry_paths)

    def test_push_directory_ignores_git(self, repo_root, monkeypatch, rclone_mock):
        """Push directory should ignore .git/."""
        rclone_mock({'copy': (0, '', ''), 'ls': (0, '', '')})

        git_dir = repo_root / '.git'
        git_dir.mkdir()
        (git_dir / 'config').write_text('git config')
        (repo_root / 'file.txt').write_text('content')

        monkeypatch.chdir(repo_root)
        result = vlfs.main(['push', '.'])

        assert result == 0
        index = vlfs.read_index(repo_root / '.vlfs')
        # Check no entries start with .git/ (but .gitignore is OK)
        assert not any(p.startswith('.git/') for p in index['entries'].keys())


class TestGlobPush:
    """Test pushing with glob patterns."""

    def test_push_glob_pattern(self, repo_root, monkeypatch, rclone_mock):
        """Push with --glob should match files."""
        rclone_mock({'copy': (0, '', ''), 'ls': (0, '', '')})

        # Create files (tools dir may already exist from fixture)
        tools_dir = repo_root / 'tools'
        tools_dir.mkdir(exist_ok=True)
        (tools_dir / 'compiler.exe').write_text('compiler')
        (tools_dir / 'linker.exe').write_text('linker')
        (tools_dir / 'readme.txt').write_text('readme')

        monkeypatch.chdir(repo_root)
        result = vlfs.main(['push', '--glob', 'tools/**/*.exe'])

        assert result == 0
        index = vlfs.read_index(repo_root / '.vlfs')
        entry_paths = list(index['entries'].keys())
        assert any('compiler.exe' in p for p in entry_paths)
        assert any('linker.exe' in p for p in entry_paths)
        assert not any('readme.txt' in p for p in entry_paths)

    def test_push_glob_recursive(self, repo_root, monkeypatch, rclone_mock):
        """Push --glob should support ** recursive patterns."""
        rclone_mock({'copy': (0, '', ''), 'ls': (0, '', '')})

        # Create nested structure
        (repo_root / 'src' / 'a').mkdir(parents=True)
        (repo_root / 'src' / 'b').mkdir(parents=True)
        (repo_root / 'src' / 'a' / 'file.txt').write_text('a')
        (repo_root / 'src' / 'b' / 'file.txt').write_text('b')
        (repo_root / 'other.txt').write_text('other')

        monkeypatch.chdir(repo_root)
        result = vlfs.main(['push', '--glob', 'src/**/*.txt'])

        assert result == 0
        index = vlfs.read_index(repo_root / '.vlfs')
        entry_paths = list(index['entries'].keys())
        assert len(entry_paths) == 2
        assert not any('other.txt' in p for p in entry_paths)


class TestPushAll:
    """Test push --all functionality."""

    def test_push_all_modified_files(self, repo_root, monkeypatch, rclone_mock):
        """Push --all should push only new or modified files."""
        rclone_mock({'copy': (0, '', ''), 'ls': (0, '', '')})

        # Create initial files and push one
        test_file = repo_root / 'test.txt'
        test_file.write_text('original')

        monkeypatch.chdir(repo_root)
        vlfs.main(['push', 'test.txt'])

        # Modify file
        test_file.write_text('modified')

        # Push --all should push modified file
        result = vlfs.main(['push', '--all'])
        assert result == 0

    def test_push_all_skips_unchanged(self, repo_root, monkeypatch, rclone_mock):
        """Push --all should skip unchanged files."""
        rclone_mock({'copy': (0, '', ''), 'ls': (0, '', '')})

        test_file = repo_root / 'test.txt'
        test_file.write_text('stable')

        monkeypatch.chdir(repo_root)
        vlfs.main(['push', 'test.txt'])

        # Push --all should not push anything (already up to date)
        result = vlfs.main(['push', '--all'])
        assert result == 0  # Still returns 0

    def test_push_all_with_modified_files(self, repo_root, monkeypatch, rclone_mock):
        """Push --all should push modified files compared to index."""
        rclone_mock({'copy': (0, '', ''), 'ls': (0, '', '')})

        # Create and push files to index
        (repo_root / 'a.txt').write_text('original a')
        (repo_root / 'b.txt').write_text('original b')

        monkeypatch.chdir(repo_root)
        vlfs.main(['push', 'a.txt'])
        vlfs.main(['push', 'b.txt'])
        
        # Modify one file
        (repo_root / 'a.txt').write_text('modified a')

        # Push --all should only push the modified file
        result = vlfs.main(['push', '--all'])

        assert result == 0
        # Both files should still be in index
        index = vlfs.read_index(repo_root / '.vlfs')
        assert len(index['entries']) == 2


class TestCrossPlatformPaths:
    """Test path handling across platforms."""

    def test_paths_use_forward_slashes_in_index(self, repo_root, monkeypatch, rclone_mock):
        """Index should store paths with forward slashes."""
        rclone_mock({'copy': (0, '', ''), 'ls': (0, '', '')})

        # Create nested file
        nested_dir = repo_root / 'tools' / 'bin'
        nested_dir.mkdir(parents=True)
        (nested_dir / 'tool.exe').write_text('tool')

        monkeypatch.chdir(repo_root)
        vlfs.main(['push', 'tools'])

        index = vlfs.read_index(repo_root / '.vlfs')
        for path in index['entries'].keys():
            assert '/' in path
            assert '\\' not in path or path.count('\\') == 0

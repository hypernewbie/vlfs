"""Unit tests for CLI and project structure (Milestone 1.1)."""

import os
from pathlib import Path

import pytest

import vlfs


class TestCLIParsing:
    """Test CLI argument parsing."""

    def test_main_returns_int(self, repo_root, monkeypatch):
        """main() should return an int exit code."""
        monkeypatch.chdir(repo_root)
        result = vlfs.main(["status"])
        assert isinstance(result, int)

    def test_unknown_command_returns_nonzero(self, repo_root, monkeypatch):
        """Unknown commands should return non-zero exit code."""
        monkeypatch.chdir(repo_root)
        result = vlfs.main(["unknowncommand"])
        assert result != 0

    def test_help_returns_zero(self, repo_root, monkeypatch):
        """Help should return 0."""
        monkeypatch.chdir(repo_root)
        result = vlfs.main(["--help"])
        assert result == 0

    def test_no_args_prints_help(self, repo_root, monkeypatch, capsys):
        """No args should print help and return 0."""
        monkeypatch.chdir(repo_root)
        result = vlfs.main([])
        captured = capsys.readouterr()
        assert result == 0
        assert "usage:" in captured.out


class TestConfigLoading:
    """Test configuration loading."""

    def test_load_config_default_path(self, repo_root):
        """Should load .vlfs/config.toml by default."""
        config_file = repo_root / ".vlfs" / "config.toml"
        config_file.write_text("[remotes]\n")

        config = vlfs.load_config(repo_root / ".vlfs")
        assert "remotes" in config

    def test_load_config_missing_returns_empty(self, repo_root):
        """Missing config should return empty dict."""
        config = vlfs.load_config(repo_root / ".vlfs")
        assert config == {}

    def test_vlfs_config_env_override(self, repo_root, monkeypatch):
        """VLFS_CONFIG env var should override config path."""
        custom_config = repo_root / "custom" / "vlfs.toml"
        custom_config.parent.mkdir()
        custom_config.write_text("[custom]\nvalue = 1\n")

        monkeypatch.setenv("VLFS_CONFIG", str(custom_config))
        vlfs_dir, _ = vlfs.resolve_paths(repo_root)

        assert vlfs_dir == custom_config.parent


class TestDirectoryLayout:
    """Test directory structure creation."""

    def test_ensure_dirs_creates_vlfs(self, repo_root):
        """Should create .vlfs/ directory."""
        vlfs_dir = repo_root / ".vlfs"
        cache_dir = repo_root / ".vlfs-cache"

        vlfs.ensure_dirs(vlfs_dir, cache_dir)

        assert vlfs_dir.exists()
        assert vlfs_dir.is_dir()

    def test_ensure_dirs_creates_cache(self, repo_root):
        """Should create .vlfs-cache/objects/ directory."""
        vlfs_dir = repo_root / ".vlfs"
        cache_dir = repo_root / ".vlfs-cache"

        vlfs.ensure_dirs(vlfs_dir, cache_dir)

        assert (cache_dir / "objects").exists()
        assert (cache_dir / "objects").is_dir()

    def test_ensure_dirs_does_not_overwrite(self, repo_root):
        """Should not overwrite existing contents."""
        vlfs_dir = repo_root / ".vlfs"
        cache_dir = repo_root / ".vlfs-cache"

        # Add existing content (directory already exists from fixture)
        existing_file = vlfs_dir / "existing.txt"
        existing_file.write_text("keep me")

        vlfs.ensure_dirs(vlfs_dir, cache_dir)

        assert existing_file.exists()
        assert existing_file.read_text() == "keep me"


class TestGitignore:
    """Test .gitignore management."""

    def test_creates_gitignore_if_missing(self, repo_root):
        """Should create .gitignore if it doesn't exist."""
        gitignore = repo_root / ".gitignore"

        vlfs.ensure_gitignore(repo_root)

        assert gitignore.exists()

    def test_adds_required_entries(self, repo_root):
        """Should add VLFS entries."""
        vlfs.ensure_gitignore(repo_root)

        content = (repo_root / ".gitignore").read_text()
        # TASK_C: Legacy entries should no longer be added
        assert ".vlfs/gdrive-token.json" not in content
        assert ".vlfs-cache/" in content

    def test_idempotent(self, repo_root):
        """Should not duplicate entries on multiple runs."""
        vlfs.ensure_gitignore(repo_root)
        vlfs.ensure_gitignore(repo_root)
        vlfs.ensure_gitignore(repo_root)

        content = (repo_root / ".gitignore").read_text()
        # Count occurrences
        assert content.count(".vlfs-cache/") == 1

    def test_preserves_existing_content(self, repo_root):
        """Should preserve existing .gitignore entries."""
        gitignore = repo_root / ".gitignore"
        gitignore.write_text("*.pyc\n")

        vlfs.ensure_gitignore(repo_root)

        content = gitignore.read_text()
        assert "*.pyc" in content
        assert ".vlfs-cache/" in content


class TestResolvePaths:
    """Test path resolution with environment overrides."""

    def test_default_paths(self, repo_root):
        """Should use default paths without env vars."""
        vlfs_dir, cache_dir = vlfs.resolve_paths(repo_root)

        assert vlfs_dir == repo_root / ".vlfs"
        assert cache_dir == repo_root / ".vlfs-cache"

    def test_vlfs_cache_env_override(self, repo_root, monkeypatch):
        """VLFS_CACHE should override cache directory."""
        monkeypatch.setenv("VLFS_CACHE", "/custom/cache")

        _, cache_dir = vlfs.resolve_paths(repo_root)

        assert cache_dir == Path("/custom/cache")

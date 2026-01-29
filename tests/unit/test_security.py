import pytest
import os
import sys
from pathlib import Path
import vlfs

class TestUserConfigDir:
    def test_returns_platform_path(self):
        """Should return platform-appropriate config dir."""
        path = vlfs.get_user_config_dir()
        assert 'vlfs' in str(path)
        assert path.exists()
    
    def test_env_override(self, tmp_path, monkeypatch):
        """VLFS_USER_CONFIG should override."""
        monkeypatch.setenv('VLFS_USER_CONFIG', str(tmp_path))
        path = vlfs.get_user_config_dir()
        assert path == tmp_path
    
    def test_creates_directory(self, tmp_path, monkeypatch):
        """Should create dir if missing."""
        new_dir = tmp_path / 'new' / 'vlfs'
        monkeypatch.setenv('VLFS_USER_CONFIG', str(new_dir))
        path = vlfs.get_user_config_dir()
        assert path.exists()

class TestConfigMerge:
    def test_user_overrides_repo(self, repo_root, tmp_path, monkeypatch):
        """User config should override repo config."""
        # Repo config
        (repo_root / '.vlfs' / 'config.toml').write_text('''
[remotes.r2]
public_base_url = "https://repo.example.com"

[defaults]
compression_level = 3
''')
        # User config
        user_dir = tmp_path / 'user'
        user_dir.mkdir()
        (user_dir / 'config.toml').write_text('''
[defaults]
compression_level = 9
''')
        monkeypatch.setenv('VLFS_USER_CONFIG', str(user_dir))
        
        config = vlfs.load_merged_config(repo_root / '.vlfs')
        
        assert config['remotes']['r2']['public_base_url'] == "https://repo.example.com"
        assert config['defaults']['compression_level'] == 9
    
    def test_works_without_user_config(self, repo_root):
        """Should work with only repo config."""
        (repo_root / '.vlfs' / 'config.toml').write_text('''
[defaults]
compression_level = 5
''')
        config = vlfs.load_merged_config(repo_root / '.vlfs')
        assert config['defaults']['compression_level'] == 5

class TestSecretWarning:
    def test_warns_on_secrets_in_repo_config(self, repo_root, capsys):
        """Should warn if secrets in repo config."""
        (repo_root / '.vlfs' / 'config.toml').write_text('''
[drive]
client_secret = "oops-committed-secret"
''')
        
        vlfs.warn_if_secrets_in_repo(repo_root / '.vlfs')
        captured = capsys.readouterr()
        
        assert 'Warning' in captured.err
        assert 'secrets' in captured.err.lower()
    
    def test_no_warning_without_secrets(self, repo_root, capsys):
        """Should not warn on clean config."""
        (repo_root / '.vlfs' / 'config.toml').write_text('''
[remotes.r2]
public_base_url = "https://example.com"
''')
        
        vlfs.warn_if_secrets_in_repo(repo_root / '.vlfs')
        captured = capsys.readouterr()
        
        assert 'Warning' not in captured.err

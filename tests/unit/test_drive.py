"""Unit tests for Google Drive backend (Milestone 3.x)."""

import os
from pathlib import Path

import pytest

import vlfs


class TestHasDriveToken:
    """Test Drive token detection."""
    
    def test_returns_true_when_token_exists(self, repo_root, monkeypatch, tmp_path):
        """Should return True if token file exists."""
        user_config = tmp_path / 'user_config'
        user_config.mkdir()
        monkeypatch.setenv('VLFS_USER_CONFIG', str(user_config))
        
        token_file = user_config / 'gdrive-token.json'
        token_file.write_text('{"token": "test"}')
        
        result = vlfs.has_drive_token(repo_root / '.vlfs')
        
        assert result is True
    
    def test_returns_false_when_no_token(self, repo_root, monkeypatch, tmp_path):
        """Should return False if token file missing."""
        user_config = tmp_path / 'user_config'
        user_config.mkdir()
        monkeypatch.setenv('VLFS_USER_CONFIG', str(user_config))
        
        result = vlfs.has_drive_token(repo_root / '.vlfs')
        
        assert result is False
    
    def test_raises_in_ci_without_token(self, repo_root, monkeypatch, tmp_path):
        """Should raise in CI environment without token."""
        user_config = tmp_path / 'user_config'
        user_config.mkdir()
        monkeypatch.setenv('VLFS_USER_CONFIG', str(user_config))
        monkeypatch.setenv('CI', 'true')
        
        with pytest.raises(RuntimeError, match='not available in CI'):
            vlfs.has_drive_token(repo_root / '.vlfs')
    
    def test_raises_with_vlfs_no_drive(self, repo_root, monkeypatch, tmp_path):
        """Should raise when VLFS_NO_DRIVE is set."""
        user_config = tmp_path / 'user_config'
        user_config.mkdir()
        monkeypatch.setenv('VLFS_USER_CONFIG', str(user_config))
        monkeypatch.setenv('VLFS_NO_DRIVE', '1')
        
        with pytest.raises(RuntimeError, match='not available'):
            vlfs.has_drive_token(repo_root / '.vlfs')
    
    def test_returns_true_in_ci_with_token(self, repo_root, monkeypatch, tmp_path):
        """Should return True in CI if token exists."""
        user_config = tmp_path / 'user_config'
        user_config.mkdir()
        monkeypatch.setenv('VLFS_USER_CONFIG', str(user_config))
        monkeypatch.setenv('CI', 'true')
        
        token_file = user_config / 'gdrive-token.json'
        token_file.write_text('{"token": "test"}')
        
        result = vlfs.has_drive_token(repo_root / '.vlfs')
        
        assert result is True


class TestWriteRcloneDriveConfig:
    """Test rclone Drive config generation."""
    
    def test_writes_config_file(self, repo_root):
        """Should write rclone.conf with Drive settings."""
        config = {
            'client_id': 'test-id',
            'client_secret': 'test-secret',
        }
        
        vlfs.write_rclone_drive_config(repo_root / '.vlfs', config)
        
        config_path = repo_root / '.vlfs' / 'rclone.conf'
        assert config_path.exists()
        content = config_path.read_text()
        assert '[gdrive]' in content
        assert 'type = drive' in content
        assert 'client_id = test-id' in content
        assert 'client_secret = test-secret' in content


class TestAuthGdrive:
    """Test auth gdrive command."""

    def test_auth_gdrive_success(self, repo_root, monkeypatch, tmp_path, rclone_mock, capsys):
        """Should extract token and write gdrive-token.json."""
        user_config = tmp_path / 'user_config'
        user_config.mkdir()
        monkeypatch.setenv('VLFS_USER_CONFIG', str(user_config))

        # Setup user config with creds
        (user_config / 'config.toml').write_text(
            '[drive]\nclient_id="cid"\nclient_secret="sec"'
        )

        # Mock rclone config to write a dummy config file with token
        def handler(cmd):
            # When rclone config is called, write the config file it's supposed to generate
            # In real life, rclone interactive auth does this.
            # Here we simulate it by writing the file directly.
            config_file = Path(cmd[3]) # rclone config --config <file>
            content = config_file.read_text()
            # Replace empty token with valid one
            if 'token = \n' in content:
                new_content = content.replace('token = \n', 'token = {"access_token":"valid"}\n')
                config_file.write_text(new_content)
            else:
                # Fallback
                config_file.write_text(content + 'token = {"access_token":"valid"}\n')
            return 0, '', ''

        rclone_mock({'_handler': handler})

        # Run auth
        result = vlfs.auth_gdrive(repo_root / '.vlfs')

        assert result == 0
        token_file = user_config / 'gdrive-token.json'
        assert token_file.exists()
        assert 'access_token' in token_file.read_text()
        assert 'valid' in token_file.read_text()

    def test_auth_gdrive_no_creds(self, repo_root, monkeypatch, tmp_path, capsys):
        """Should fail if no creds in user config."""
        user_config = tmp_path / 'user_config'
        user_config.mkdir()
        monkeypatch.setenv('VLFS_USER_CONFIG', str(user_config))

        result = vlfs.auth_gdrive(repo_root / '.vlfs')

        assert result == 1
        captured = capsys.readouterr()
        assert 'Drive credentials not found' in captured.out


class TestGroupObjectsByRemote:
    """Test grouping index entries by remote."""
    
    def test_groups_by_remote(self):
        """Should group objects by their remote field."""
        index = {
            'version': 1,
            'entries': {
                'file1.txt': {'object_key': 'a/b/1', 'remote': 'r2'},
                'file2.txt': {'object_key': 'c/d/2', 'remote': 'gdrive'},
                'file3.txt': {'object_key': 'e/f/3', 'remote': 'r2'},
                'file4.txt': {'object_key': 'g/h/4'},  # No remote, defaults to r2
            }
        }
        
        groups = vlfs.group_objects_by_remote(index)
        
        assert len(groups['r2']) == 3
        assert len(groups['gdrive']) == 1
        assert ('a/b/1', 'file1.txt') in groups['r2']
        assert ('c/d/2', 'file2.txt') in groups['gdrive']
    
    def test_empty_index(self):
        """Should handle empty index."""
        index = {'version': 1, 'entries': {}}
        
        groups = vlfs.group_objects_by_remote(index)
        
        assert groups == {}
    
    def test_skips_entries_without_object_key(self):
        """Should skip entries missing object_key."""
        index = {
            'version': 1,
            'entries': {
                'file1.txt': {'object_key': 'a/b/1', 'remote': 'r2'},
                'file2.txt': {'remote': 'r2'},  # Missing object_key
            }
        }
        
        groups = vlfs.group_objects_by_remote(index)
        
        assert len(groups['r2']) == 1


class TestUploadToDrive:
    """Test upload_to_drive function."""
    
    def test_uploads_file(self, tmp_path, rclone_mock):
        """Should upload file to Drive."""
        local_file = tmp_path / 'test.txt'
        local_file.write_bytes(b'test content')
        
        mock = rclone_mock({
            'copy': (0, '', ''),
        })
        
        result = vlfs.upload_to_drive(local_file, 'ab/cd/abcdef')
        
        assert result is True
        copy_calls = [c for c in mock['calls'] if c[1] == 'copy']
        assert len(copy_calls) == 1
        assert '--transfers' in copy_calls[0]
        assert '1' in copy_calls[0]
    
    def test_dry_run_does_not_upload(self, tmp_path, rclone_mock, capsys):
        """Dry run should not upload."""
        local_file = tmp_path / 'test.txt'
        local_file.write_bytes(b'test content')
        
        rclone_mock({})
        
        result = vlfs.upload_to_drive(local_file, 'ab/cd/abcdef', dry_run=True)
        captured = capsys.readouterr()
        
        assert result is True
        assert '[DRY-RUN]' in captured.out
    
    def test_retries_on_rate_limit(self, tmp_path, rclone_mock):
        """Should retry on 403/429 errors."""
        local_file = tmp_path / 'test.txt'
        local_file.write_bytes(b'test content')
        
        call_count = [0]
        
        def handler(cmd):
            call_count[0] += 1
            if call_count[0] < 2:
                raise vlfs.RcloneError("rate limit", 1, "", "403 Forbidden")
            return (0, '', '')
        
        rclone_mock({'_handler': handler})
        
        result = vlfs.upload_to_drive(local_file, 'ab/cd/abcdef')
        
        assert result is True
        assert call_count[0] == 2


class TestDownloadFromDrive:
    """Test download_from_drive function."""
    
    def test_downloads_objects(self, tmp_path, rclone_mock):
        """Should download objects with rate limiting."""
        cache_dir = tmp_path / 'cache'
        
        mock = rclone_mock({
            'copy': (0, '', ''),
        })
        
        object_keys = ['ab/cd/obj1', 'ef/gh/obj2']
        result = vlfs.download_from_drive(object_keys, cache_dir)
        
        assert result == 2
        copy_calls = [c for c in mock['calls'] if c[1] == 'copy']
        assert len(copy_calls) == 1
        assert '--transfers' in copy_calls[0]
        assert '1' in copy_calls[0]
    
    def test_empty_list_returns_zero(self, tmp_path, rclone_mock):
        """Empty list should return 0."""
        cache_dir = tmp_path / 'cache'
        
        result = vlfs.download_from_drive([], cache_dir)
        
        assert result == 0
    
    def test_dry_run_does_not_download(self, tmp_path, rclone_mock, capsys):
        """Dry run should not download."""
        cache_dir = tmp_path / 'cache'
        
        rclone_mock({})
        
        object_keys = ['ab/cd/obj1']
        result = vlfs.download_from_drive(object_keys, cache_dir, dry_run=True)
        captured = capsys.readouterr()
        
        assert result == 1
        assert '[DRY-RUN]' in captured.out


class TestCmdPushPrivate:
    """Test push command with --private flag."""
    
    def test_private_flag_uploads_to_drive(self, repo_root, monkeypatch, rclone_mock, capsys, tmp_path):
        """--private should upload to Drive."""
        # Setup user config with token
        user_config = tmp_path / 'user_config'
        user_config.mkdir()
        monkeypatch.setenv('VLFS_USER_CONFIG', str(user_config))
        (user_config / 'gdrive-token.json').write_text('{"token": "test"}')
        
        test_file = repo_root / 'private' / 'secret.txt'
        test_file.parent.mkdir(exist_ok=True)
        test_file.write_bytes(b'secret content')
        
        rclone_mock({
            'copy': (0, '', ''),
        })
        
        monkeypatch.chdir(repo_root)
        result = vlfs.main(['push', '--private', 'private/secret.txt'])
        
        assert result == 0
        
        # Check index has gdrive remote
        index = vlfs.read_index(repo_root / '.vlfs')
        assert index['entries']['private/secret.txt']['remote'] == 'gdrive'
    
    def test_private_without_token_fails(self, repo_root, monkeypatch, capsys, tmp_path):
        """--private should fail without Drive token."""
        # Setup empty user config (no token)
        user_config = tmp_path / 'user_config'
        user_config.mkdir()
        monkeypatch.setenv('VLFS_USER_CONFIG', str(user_config))

        test_file = repo_root / 'private' / 'secret.txt'
        test_file.parent.mkdir(exist_ok=True)
        test_file.write_bytes(b'secret content')
        
        monkeypatch.chdir(repo_root)
        result = vlfs.main(['push', '--private', 'private/secret.txt'])
        captured = capsys.readouterr()
        
        assert result == 1
        assert 'auth gdrive' in captured.err.lower() or 'Error' in captured.err
    
    def test_default_pushes_to_r2(self, repo_root, monkeypatch, rclone_mock):
        """Default push should go to R2."""
        monkeypatch.setattr(vlfs, 'ensure_r2_auth', lambda: 0)
        monkeypatch.setattr(vlfs, 'validate_r2_connection', lambda *args, **kwargs: True)

        test_file = repo_root / 'public' / 'file.txt'
        test_file.parent.mkdir(exist_ok=True)
        test_file.write_bytes(b'public content')
        
        rclone_mock({
            'copy': (0, '', ''),
        })
        
        monkeypatch.chdir(repo_root)
        result = vlfs.main(['push', 'public/file.txt'])
        
        assert result == 0
        
        # Check index has r2 remote
        index = vlfs.read_index(repo_root / '.vlfs')
        assert index['entries']['public/file.txt']['remote'] == 'r2'


class TestCmdPullMixedRemotes:
    """Test pull command with mixed R2 and Drive remotes."""
    
    def test_pulls_from_both_remotes(self, repo_root, monkeypatch, tmp_path):
        """Should pull from both R2 and Drive."""
        monkeypatch.setattr(vlfs, 'validate_r2_connection', lambda *args, **kwargs: True)

        # Setup user config with token
        user_config = tmp_path / 'user_config'
        user_config.mkdir()
        monkeypatch.setenv('VLFS_USER_CONFIG', str(user_config))
        (user_config / 'gdrive-token.json').write_text('{"token": "test"}')
        
        # Create index with mixed remotes
        index = {
            'version': 1,
            'entries': {
                'r2-file.txt': {'object_key': 'aa/bb/r2', 'remote': 'r2', 'hash': 'abc'},
                'drive-file.txt': {'object_key': 'cc/dd/drive', 'remote': 'gdrive', 'hash': 'def'},
            }
        }
        vlfs.write_index(repo_root / '.vlfs', index)
        
        # Mock downloads - just track calls
        r2_downloaded = []
        drive_downloaded = []
        
        def mock_r2_download(keys, cache_dir, bucket='vlfs', dry_run=False):
            r2_downloaded.extend(keys)
            return len(keys)
        
        def mock_drive_download(keys, cache_dir, bucket='vlfs', dry_run=False):
            drive_downloaded.extend(keys)
            return len(keys)
        
        monkeypatch.setattr(vlfs, 'download_from_r2', mock_r2_download)
        monkeypatch.setattr(vlfs, 'download_from_drive', mock_drive_download)
        
        # Mock materialize to skip actual file writing
        monkeypatch.setattr(vlfs, 'materialize_workspace', lambda *args, **kwargs: (0, 0, []))
        
        monkeypatch.chdir(repo_root)
        result = vlfs.main(['pull'])
        
        assert result == 0
        assert 'aa/bb/r2' in r2_downloaded
        assert 'cc/dd/drive' in drive_downloaded
    
    def test_skips_drive_in_ci(self, repo_root, monkeypatch):
        """Should skip Drive downloads in CI."""
        monkeypatch.setattr(vlfs, 'validate_r2_connection', lambda *args, **kwargs: True)
        monkeypatch.setenv('CI', 'true')
        
        # Create index with Drive file
        index = {
            'version': 1,
            'entries': {
                'drive-file.txt': {'object_key': 'aa/bb/cc', 'remote': 'gdrive', 'hash': 'def'},
            }
        }
        vlfs.write_index(repo_root / '.vlfs', index)
        
        # Mock materialize
        monkeypatch.setattr(vlfs, 'materialize_workspace', lambda *args, **kwargs: (0, 0, []))
        
        monkeypatch.chdir(repo_root)
        result = vlfs.main(['pull'])
        
        # Should succeed but skip Drive
        assert result == 0

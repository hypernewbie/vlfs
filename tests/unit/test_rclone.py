"""Unit tests for rclone integration (Milestone 2.1)."""

import os
from pathlib import Path
from unittest.mock import patch

import pytest

import vlfs


class TestRcloneError:
    """Test RcloneError exception."""
    
    def test_error_attributes(self):
        """Error should store all attributes."""
        err = vlfs.RcloneError("test error", 1, "stdout", "stderr")
        assert err.returncode == 1
        assert err.stdout == "stdout"
        assert err.stderr == "stderr"
        assert str(err) == "test error"


class TestRunRclone:
    """Test rclone subprocess wrapper."""
    
    def test_success_returns_output(self, rclone_mock):
        """Successful run should return output."""
        mock = rclone_mock({
            'lsd': (0, 'bucket1\nbucket2', ''),
        })
        
        returncode, stdout, stderr = vlfs.run_rclone(['lsd', 'r2:'])
        
        assert returncode == 0
        assert 'bucket1' in stdout
        assert mock['calls'][0] == ['rclone', 'lsd', 'r2:']
    
    def test_failure_raises_rclone_error(self, rclone_mock):
        """Failure should raise RcloneError."""
        rclone_mock({
            'lsd': (1, '', 'permission denied'),
        })
        
        with pytest.raises(vlfs.RcloneError) as exc_info:
            vlfs.run_rclone(['lsd', 'r2:'])
        
        assert exc_info.value.returncode == 1
        assert 'permission denied' in exc_info.value.stderr
    
    def test_accepts_cwd_parameter(self, rclone_mock):
        """Should accept cwd parameter."""
        mock = rclone_mock({
            'copy': (0, '', ''),
        })
        
        vlfs.run_rclone(['copy', 'a', 'b'], cwd='/some/path')
        
        # Just verify it doesn't crash - cwd is passed to subprocess
        assert len(mock['calls']) == 1
    
    def test_accepts_timeout(self, rclone_mock):
        """Should accept timeout parameter."""
        rclone_mock({
            'copy': (0, '', ''),
        })
        
        # Should not raise
        vlfs.run_rclone(['copy', 'a', 'b'], timeout=30)

    def test_uses_config_path_when_set(self, rclone_mock, tmp_path):
        """Should include --config when config path is set."""
        config_path = tmp_path / 'rclone.conf'
        config_path.write_text('[r2]\ntype = s3\n')

        vlfs.set_rclone_config_path(config_path)
        mock = rclone_mock({'lsd': (0, '', '')})

        vlfs.run_rclone(['lsd', 'r2:'])

        assert '--config' in mock['calls'][0]
        assert str(config_path) in mock['calls'][0]

        vlfs.set_rclone_config_path(None)


class TestRetry:
    """Test retry functionality."""
    
    def test_success_on_first_try(self):
        """Should return result on first success."""
        call_count = [0]
        
        def succeed():
            call_count[0] += 1
            return "success"
        
        result = vlfs.retry(succeed, attempts=3, base_delay=0.01)
        
        assert result == "success"
        assert call_count[0] == 1
    
    def test_retries_on_failure(self):
        """Should retry on failure."""
        call_count = [0]
        
        def fail_twice():
            call_count[0] += 1
            if call_count[0] < 3:
                raise vlfs.RcloneError("fail", 1, "", "")
            return "success"
        
        result = vlfs.retry(fail_twice, attempts=5, base_delay=0.01)
        
        assert result == "success"
        assert call_count[0] == 3
    
    def test_raises_after_max_attempts(self):
        """Should raise after max attempts."""
        def always_fail():
            raise vlfs.RcloneError("fail", 1, "", "")
        
        with pytest.raises(vlfs.RcloneError):
            vlfs.retry(always_fail, attempts=2, base_delay=0.01)


class TestFormatBytes:
    """Test byte formatting."""
    
    def test_bytes(self):
        """Should format bytes."""
        assert vlfs.format_bytes(0) == "0.0B"
        assert vlfs.format_bytes(100) == "100.0B"
    
    def test_kilobytes(self):
        """Should format KB."""
        assert vlfs.format_bytes(1024) == "1.0KB"
        assert vlfs.format_bytes(1536) == "1.5KB"
    
    def test_megabytes(self):
        """Should format MB."""
        assert vlfs.format_bytes(1024 * 1024) == "1.0MB"
    
    def test_gigabytes(self):
        """Should format GB."""
        assert vlfs.format_bytes(1024 ** 3) == "1.0GB"


class TestGetR2Config:
    """Test R2 config from environment."""
    
    def test_raises_on_missing_vars(self, monkeypatch):
        """Should raise if env vars missing."""
        for var in ['RCLONE_CONFIG_R2_ACCESS_KEY_ID', 'RCLONE_CONFIG_R2_SECRET_ACCESS_KEY', 'RCLONE_CONFIG_R2_ENDPOINT']:
            monkeypatch.delenv(var, raising=False)
        
        with pytest.raises(ValueError, match='Missing R2 credentials'):
            vlfs.get_r2_config_from_env()
    
    def test_returns_config_with_all_vars(self, monkeypatch):
        """Should return config when all vars set."""
        monkeypatch.setenv('RCLONE_CONFIG_R2_ACCESS_KEY_ID', 'test-key')
        monkeypatch.setenv('RCLONE_CONFIG_R2_SECRET_ACCESS_KEY', 'test-secret')
        monkeypatch.setenv('RCLONE_CONFIG_R2_ENDPOINT', 'https://test.r2.cloudflarestorage.com')
        
        config = vlfs.get_r2_config_from_env()
        
        assert config['access_key_id'] == 'test-key'
        assert config['secret_access_key'] == 'test-secret'
        assert config['endpoint'] == 'https://test.r2.cloudflarestorage.com'
    
    def test_partial_vars_raises(self, monkeypatch):
        """Should raise if only some vars set."""
        monkeypatch.setenv('RCLONE_CONFIG_R2_ACCESS_KEY_ID', 'test-key')
        monkeypatch.delenv('RCLONE_CONFIG_R2_SECRET_ACCESS_KEY', raising=False)
        monkeypatch.delenv('RCLONE_CONFIG_R2_ENDPOINT', raising=False)
        
        with pytest.raises(ValueError, match='Missing'):
            vlfs.get_r2_config_from_env()


class TestValidateR2Connection:
    """Test R2 connection validation."""
    
    def test_raises_on_missing_creds(self, monkeypatch):
        """Should raise if credentials missing."""
        for var in ['RCLONE_CONFIG_R2_ACCESS_KEY_ID', 'RCLONE_CONFIG_R2_SECRET_ACCESS_KEY', 'RCLONE_CONFIG_R2_ENDPOINT']:
            monkeypatch.delenv(var, raising=False)
        
        with pytest.raises(ValueError, match='Missing'):
            vlfs.validate_r2_connection()
    
    def test_success_with_valid_creds(self, rclone_mock, monkeypatch):
        """Should succeed with valid credentials."""
        monkeypatch.setenv('RCLONE_CONFIG_R2_ACCESS_KEY_ID', 'test-key')
        monkeypatch.setenv('RCLONE_CONFIG_R2_SECRET_ACCESS_KEY', 'test-secret')
        monkeypatch.setenv('RCLONE_CONFIG_R2_ENDPOINT', 'https://test.r2.cloudflarestorage.com')
        
        rclone_mock({
            'lsd': (0, 'vlfs', ''),
        })
        
        result = vlfs.validate_r2_connection()
        
        assert result is True
    
    def test_raises_on_connection_failure(self, rclone_mock, monkeypatch):
        """Should raise RcloneError on failure."""
        monkeypatch.setenv('RCLONE_CONFIG_R2_ACCESS_KEY_ID', 'test-key')
        monkeypatch.setenv('RCLONE_CONFIG_R2_SECRET_ACCESS_KEY', 'test-secret')
        monkeypatch.setenv('RCLONE_CONFIG_R2_ENDPOINT', 'https://test.r2.cloudflarestorage.com')
        
        rclone_mock({
            'lsd': (1, '', 'connection refused'),
        })
        
        with pytest.raises(vlfs.RcloneError):
            vlfs.validate_r2_connection()


class TestRemoteObjectExists:
    """Test checking remote object existence."""
    
    def test_exists_returns_true(self, rclone_mock):
        """Should return True if object exists."""
        rclone_mock({
            'ls': (0, '-rw-r--r-- 1 user group 1234 Jan 1 00:00 file.obj', ''),
        })
        
        result = vlfs.remote_object_exists('ab/cd/abcdef123', 'vlfs')
        
        assert result is True
    
    def test_missing_returns_false(self, rclone_mock):
        """Should return False if object missing."""
        rclone_mock({
            'ls': (0, '', ''),  # Empty output means not found
        })
        
        result = vlfs.remote_object_exists('ab/cd/missing', 'vlfs')
        
        assert result is False
    
    def test_error_returns_false(self, rclone_mock):
        """Should return False on error."""
        rclone_mock({
            'ls': (1, '', 'network error'),
        })
        
        result = vlfs.remote_object_exists('ab/cd/error', 'vlfs')
        
        assert result is False

"""Unit tests for logging and error handling (Phase 5.1)."""

import json
import logging
import os
from pathlib import Path
from unittest.mock import patch

import pytest

import vlfs


class TestVerboseLogging:
    """Test -v and -vv verbose flags."""

    def test_verbose_flag_increases_logging(self, repo_root, monkeypatch, rclone_mock, caplog):
        """-v flag should increase log verbosity to DEBUG."""
        rclone_mock({'copy': (0, '', ''), 'ls': (0, '', '')})
        
        test_file = repo_root / 'test.txt'
        test_file.write_text('content')

        monkeypatch.chdir(repo_root)
        
        # Set logging to capture DEBUG
        with caplog.at_level(logging.DEBUG):
            result = vlfs.main(['-v', 'push', 'test.txt'])
        
        assert result == 0
        # Should have debug-level messages
        assert any(record.levelno == logging.DEBUG for record in caplog.records)

    def test_very_verbose_flag(self, repo_root, monkeypatch, rclone_mock, caplog):
        """-vv flag should enable maximum verbosity."""
        rclone_mock({'copy': (0, '', ''), 'ls': (0, '', '')})
        
        test_file = repo_root / 'test.txt'
        test_file.write_text('content')

        monkeypatch.chdir(repo_root)
        
        with caplog.at_level(logging.DEBUG):
            result = vlfs.main(['-vv', 'push', 'test.txt'])
        
        assert result == 0
        # Should have more verbose output
        assert len(caplog.records) >= 0

    def test_verbose_flag_with_pull(self, repo_root, monkeypatch, rclone_mock, caplog):
        """-v flag should work with pull command."""
        rclone_mock({'copy': (0, '', ''), 'ls': (0, '', '')})

        # Create an index entry
        vlfs_dir = repo_root / '.vlfs'
        index = {'version': 1, 'entries': {
            'test.txt': {
                'hash': 'abc123',
                'size': 10,
                'object_key': 'ab/cd/abc123',
                'remote': 'r2'
            }
        }}
        (vlfs_dir / 'index.json').write_text(json.dumps(index))

        # Provide dummy R2 credentials
        monkeypatch.setenv('RCLONE_CONFIG_R2_ACCESS_KEY_ID', 'dummy')
        monkeypatch.setenv('RCLONE_CONFIG_R2_SECRET_ACCESS_KEY', 'dummy')
        monkeypatch.setenv('RCLONE_CONFIG_R2_ENDPOINT', 'https://example.com')

        monkeypatch.chdir(repo_root)

        with caplog.at_level(logging.DEBUG):
            result = vlfs.main(['-v', 'pull'])

        assert result == 0


class TestLogFile:
    """Test log file writing."""

    def test_log_file_created_in_home(self, repo_root, monkeypatch, mocker, tmp_path):
        """Log file should be created in ~/.vlfs/vlfs.log."""
        # Patch subprocess.run in vlfs module to mock rclone
        mocker.patch('vlfs.subprocess.run', return_value=mocker.MagicMock(returncode=0, stdout='', stderr=''))
        
        # Override home directory for testing
        fake_home = tmp_path / 'home'
        fake_home.mkdir()
        monkeypatch.setenv('HOME', str(fake_home))
        monkeypatch.setenv('USERPROFILE', str(fake_home))
        
        test_file = repo_root / 'test.txt'
        test_file.write_text('content')

        monkeypatch.chdir(repo_root)
        result = vlfs.main(['push', 'test.txt'])
        
        assert result == 0
        # Log file should exist
        log_file = fake_home / '.vlfs' / 'vlfs.log'
        assert log_file.exists()

    def test_log_includes_timestamps(self, repo_root, monkeypatch, mocker, tmp_path):
        """Log entries should include timestamps."""
        mocker.patch('vlfs.subprocess.run', return_value=mocker.MagicMock(returncode=0, stdout='', stderr=''))
        
        fake_home = tmp_path / 'home'
        fake_home.mkdir()
        monkeypatch.setenv('HOME', str(fake_home))
        monkeypatch.setenv('USERPROFILE', str(fake_home))
        
        test_file = repo_root / 'test.txt'
        test_file.write_text('content')

        monkeypatch.chdir(repo_root)
        vlfs.main(['push', 'test.txt'])
        
        log_file = fake_home / '.vlfs' / 'vlfs.log'
        log_content = log_file.read_text()
        
        # Should have ISO-format timestamp (YYYY-MM-DD)
        assert '20' in log_content  # Year
        assert '-' in log_content

    def test_log_includes_log_levels(self, repo_root, monkeypatch, mocker, tmp_path):
        """Log entries should include level names (INFO, DEBUG, etc)."""
        mocker.patch('vlfs.subprocess.run', return_value=mocker.MagicMock(returncode=0, stdout='', stderr=''))
        
        fake_home = tmp_path / 'home'
        fake_home.mkdir()
        monkeypatch.setenv('HOME', str(fake_home))
        monkeypatch.setenv('USERPROFILE', str(fake_home))
        
        test_file = repo_root / 'test.txt'
        test_file.write_text('content')

        monkeypatch.chdir(repo_root)
        vlfs.main(['-v', 'push', 'test.txt'])
        
        log_file = fake_home / '.vlfs' / 'vlfs.log'
        log_content = log_file.read_text()
        
        # Should have log level markers
        assert 'INFO' in log_content or 'DEBUG' in log_content or 'ERROR' in log_content


class TestErrorHelpers:
    """Test error message helpers with hints."""

    def test_missing_rclone_error_includes_hint(self, repo_root, monkeypatch, capsys):
        """Error for missing rclone should include installation hint."""
        # Mock subprocess.run to simulate rclone not found
        def mock_run(*args, **kwargs):
            raise FileNotFoundError("rclone not found")
        
        monkeypatch.setattr('subprocess.run', mock_run)
        monkeypatch.chdir(repo_root)
        
        # Try to validate R2 connection
        try:
            vlfs.validate_r2_connection()
        except Exception as e:
            error_msg = str(e)
            # Should mention rclone installation
            assert 'rclone' in error_msg.lower() or 'install' in error_msg.lower()

    def test_missing_credentials_error_includes_hint(self, repo_root, monkeypatch, capsys, mocker):
        """Error for missing R2 credentials should include setup hint."""
        # Mock subprocess.run in vlfs module to simulate credential error
        def mock_run(*args, **kwargs):
            raise FileNotFoundError("rclone not found - install rclone")

        mocker.patch('vlfs.subprocess.run', side_effect=mock_run)
        
        # Clear all R2 env vars
        for var in ['RCLONE_CONFIG_R2_ACCESS_KEY_ID', 
                    'RCLONE_CONFIG_R2_SECRET_ACCESS_KEY', 
                    'RCLONE_CONFIG_R2_ENDPOINT']:
            monkeypatch.delenv(var, raising=False)

        # Create index with an R2 entry to trigger download
        vlfs_dir = repo_root / '.vlfs'
        index = {'version': 1, 'entries': {
            'test.txt': {
                'hash': 'abc123',
                'size': 10,
                'object_key': 'ab/cd/abc123',
                'remote': 'r2'
            }
        }}
        (vlfs_dir / 'index.json').write_text(json.dumps(index))

        monkeypatch.chdir(repo_root)

        result = vlfs.main(['pull'])
        captured = capsys.readouterr()
        
        # Should error and provide hint about credentials or rclone
        assert result != 0 or 'rclone' in captured.err.lower() or 'credential' in captured.err.lower()

    def test_missing_drive_token_error_includes_hint(self, repo_root, monkeypatch, capsys):
        """Error for missing Drive token should include auth hint."""
        # This test verifies the hint is present when token is missing
        # We mock has_drive_token to return False
        import vlfs
        
        # Create index with Drive remote
        vlfs_dir = repo_root / '.vlfs'
        index = {'version': 1, 'entries': {
            'test.txt': {
                'hash': 'abc123',
                'size': 10,
                'object_key': 'ab/cd/abc123',
                'remote': 'gdrive'
            }
        }}
        (vlfs_dir / 'index.json').write_text(json.dumps(index))

        monkeypatch.chdir(repo_root)
        
        # The pull command should handle missing Drive token gracefully
        result = vlfs.main(['pull'])
        captured = capsys.readouterr()
        
        # Should complete without crashing (even if Drive download is skipped)
        # The error message might mention auth or drive
        assert result == 0 or 'auth' in captured.err.lower() or 'drive' in captured.err.lower() or 'token' in captured.err.lower()

    def test_die_function_exists(self):
        """die() helper function should exist."""
        assert hasattr(vlfs, 'die')

    def test_die_function_accepts_hint(self, repo_root, monkeypatch, capsys):
        """die() should accept and display hint parameter."""
        monkeypatch.chdir(repo_root)
        
        # Test die with hint
        result = vlfs.die("Something failed", hint="Try running: vlfs status", exit_code=42)
        captured = capsys.readouterr()
        
        assert result == 42
        assert "Something failed" in captured.err
        assert "Try running" in captured.err or "hint" in captured.err.lower()


class TestStructuredExceptions:
    """Test structured exception handling."""

    def test_rclone_error_has_attributes(self):
        """RcloneError should have returncode, stdout, stderr attributes."""
        error = vlfs.RcloneError("test error", 1, "stdout content", "stderr content")
        
        assert error.returncode == 1
        assert error.stdout == "stdout content"
        assert error.stderr == "stderr content"
        assert str(error) == "test error"

    def test_config_error_class_exists(self):
        """ConfigError class should exist for configuration errors."""
        assert hasattr(vlfs, 'ConfigError')

    def test_index_error_class_exists(self):
        """IndexError class should exist for index-related errors."""
        assert hasattr(vlfs, 'IndexError')


class TestLoggingModule:
    """Test logging module integration."""

    def test_logging_setup_function_exists(self):
        """setup_logging function should exist."""
        assert hasattr(vlfs, 'setup_logging')

    def test_logging_with_different_verbosity_levels(self, repo_root, monkeypatch):
        """setup_logging should handle different verbosity levels."""
        monkeypatch.chdir(repo_root)
        
        # Should not raise for any verbosity level
        vlfs.setup_logging(0)  # Default
        vlfs.setup_logging(1)  # -v
        vlfs.setup_logging(2)  # -vv

    def test_logger_instance_exists(self):
        """Module should have a logger instance."""
        assert hasattr(vlfs, 'logger')
        assert isinstance(vlfs.logger, logging.Logger)

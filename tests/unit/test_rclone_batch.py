from pathlib import Path
from unittest.mock import MagicMock

import pytest

import vlfs


class TestBatchDownload:
    """Test batch download logic."""

    def test_r2_files_from_content(self, rclone_mock, tmp_path, monkeypatch):
        """R2 download should write bare keys to files-from."""
        cache_dir = tmp_path / 'cache'
        cache_dir.mkdir()
        rclone_mock({'copy': (0, '', '')})
        
        # Ensure credentials check passes
        monkeypatch.setenv('RCLONE_CONFIG_R2_ACCESS_KEY_ID', 'test')
        monkeypatch.setenv('RCLONE_CONFIG_R2_SECRET_ACCESS_KEY', 'test')
        monkeypatch.setenv('RCLONE_CONFIG_R2_ENDPOINT', 'test')

        # Mock tempfile to capture content
        written_data = []
        mock_temp = MagicMock()
        mock_temp.name = str(tmp_path / 'mock_r2.txt')
        mock_temp.write = written_data.append
        
        # Create file for unlink
        Path(mock_temp.name).touch()

        ctx = MagicMock()
        ctx.__enter__.return_value = mock_temp
        
        monkeypatch.setattr(vlfs.tempfile, 'NamedTemporaryFile', lambda **kw: ctx)

        vlfs.download_from_r2(['obj1', 'obj2'], cache_dir, bucket='bk')
        
        # Check that it wrote newline joined keys
        assert written_data == ['obj1\nobj2']

    def test_drive_files_from_content(self, rclone_mock, tmp_path, monkeypatch):
        """Drive download should write bare keys to files-from."""
        cache_dir = tmp_path / 'cache'
        cache_dir.mkdir()
        rclone_mock({'copy': (0, '', '')})

        written_data = []
        mock_temp = MagicMock()
        mock_temp.name = str(tmp_path / 'mock_drive.txt')
        mock_temp.write = written_data.append
        
        Path(mock_temp.name).touch()

        ctx = MagicMock()
        ctx.__enter__.return_value = mock_temp
        
        monkeypatch.setattr(vlfs.tempfile, 'NamedTemporaryFile', lambda **kw: ctx)

        vlfs.download_from_drive(['obj1', 'obj2'], cache_dir, bucket='bk')
        
        assert written_data == ['obj1\nobj2']
import pytest
from pathlib import Path
import vlfs


@pytest.fixture(autouse=True)
def cleanup_vlfs_state():
    """Clean up vlfs global state before and after each test."""
    vlfs.set_rclone_config_path(None)
    yield
    vlfs.set_rclone_config_path(None)


class TestR2Auth:
    def test_ensure_r2_auth_with_env_vars(self, monkeypatch, tmp_path):
        """Should succeed and write config if env vars present."""
        user_config = tmp_path / "user_config"
        user_config.mkdir()
        monkeypatch.setenv("VLFS_USER_CONFIG", str(user_config))

        monkeypatch.setenv("RCLONE_CONFIG_R2_ACCESS_KEY_ID", "key")
        monkeypatch.setenv("RCLONE_CONFIG_R2_SECRET_ACCESS_KEY", "secret")
        monkeypatch.setenv("RCLONE_CONFIG_R2_ENDPOINT", "endpoint")

        assert vlfs.ensure_r2_auth() == 0
        assert (user_config / "rclone.conf").exists()
        assert "[r2]" in (user_config / "rclone.conf").read_text()

    def test_ensure_r2_auth_with_config_file(self, monkeypatch, tmp_path):
        """Should succeed if config file exists and has r2 section."""
        user_config = tmp_path / "user_config"
        user_config.mkdir()
        monkeypatch.setenv("VLFS_USER_CONFIG", str(user_config))

        # Clear env vars (set to empty to override autouse fixture)
        monkeypatch.setenv("RCLONE_CONFIG_R2_ACCESS_KEY_ID", "")
        monkeypatch.setenv("RCLONE_CONFIG_R2_SECRET_ACCESS_KEY", "")

        (user_config / "rclone.conf").write_text("[r2]\ntype=s3\n")

        assert vlfs.ensure_r2_auth() == 0
        assert vlfs.get_rclone_config_path() == user_config / "rclone.conf"

    def test_ensure_r2_auth_fails_without_creds(self, monkeypatch, tmp_path, capsys):
        """Should fail if neither env vars nor config file present."""
        user_config = tmp_path / "user_config"
        user_config.mkdir()
        monkeypatch.setenv("VLFS_USER_CONFIG", str(user_config))

        # Clear env vars
        monkeypatch.setenv("RCLONE_CONFIG_R2_ACCESS_KEY_ID", "")
        monkeypatch.setenv("RCLONE_CONFIG_R2_SECRET_ACCESS_KEY", "")
        monkeypatch.setenv("RCLONE_CONFIG_R2_ENDPOINT", "")

        # Ensure no config file
        if (user_config / "rclone.conf").exists():
            (user_config / "rclone.conf").unlink()

        assert vlfs.ensure_r2_auth() == 1
        captured = capsys.readouterr()
        assert "R2 credentials required" in captured.err

    def test_rclone_config_has_section(self, tmp_path):
        """Test rclone_config_has_section helper function."""
        config_path = tmp_path / "rclone.conf"

        # Non-existent config should return False
        assert vlfs.rclone_config_has_section(config_path, "r2") is False

        # Config without the section should return False
        config_path.write_text("[other]\ntype = drive\n")
        assert vlfs.rclone_config_has_section(config_path, "r2") is False

        # Config with the section should return True
        config_path.write_text("[r2]\ntype = s3\n")
        assert vlfs.rclone_config_has_section(config_path, "r2") is True

        # Config with multiple sections including r2 should return True
        config_path.write_text("[gdrive]\ntype = drive\n\n[r2]\ntype = s3\n")
        assert vlfs.rclone_config_has_section(config_path, "r2") is True

    def test_write_rclone_r2_config(self, monkeypatch, tmp_path):
        """Test write_rclone_r2_config writes correct config format."""
        dest_dir = tmp_path / "config"
        dest_dir.mkdir()

        # Set up env vars
        monkeypatch.setenv("RCLONE_CONFIG_R2_ACCESS_KEY_ID", "test_key_123")
        monkeypatch.setenv("RCLONE_CONFIG_R2_SECRET_ACCESS_KEY", "test_secret_456")
        monkeypatch.setenv(
            "RCLONE_CONFIG_R2_ENDPOINT", "https://test.r2.cloudflarestorage.com"
        )

        # Write config
        vlfs.write_rclone_r2_config(dest_dir)

        # Verify file exists
        config_path = dest_dir / "rclone.conf"
        assert config_path.exists()

        # Verify content
        content = config_path.read_text()
        assert "[r2]" in content
        assert "type = s3" in content
        assert "provider = Cloudflare" in content
        assert "endpoint = https://test.r2.cloudflarestorage.com" in content
        assert "access_key_id = test_key_123" in content
        assert "secret_access_key = test_secret_456" in content

    def test_write_rclone_r2_config_without_endpoint(self, monkeypatch, tmp_path):
        """Test write_rclone_r2_config works without endpoint (optional)."""
        dest_dir = tmp_path / "config"
        dest_dir.mkdir()

        # Set up env vars without endpoint
        monkeypatch.setenv("RCLONE_CONFIG_R2_ACCESS_KEY_ID", "key")
        monkeypatch.setenv("RCLONE_CONFIG_R2_SECRET_ACCESS_KEY", "secret")
        monkeypatch.setenv("RCLONE_CONFIG_R2_ENDPOINT", "http://example.com")

        # Write config
        vlfs.write_rclone_r2_config(dest_dir)

        # Verify file exists and has basic structure
        config_path = dest_dir / "rclone.conf"
        assert config_path.exists()
        content = config_path.read_text()
        assert "[r2]" in content
        assert "type = s3" in content

    def test_validate_r2_connection_uses_existing_config(self, monkeypatch, tmp_path):
        """Test validate_r2_connection doesn't require env vars if config path already set."""
        user_config = tmp_path / "user_config"
        user_config.mkdir()

        # Create a valid config file
        config_path = user_config / "rclone.conf"
        config_path.write_text("[r2]\ntype = s3\nprovider = Cloudflare\n")

        # Set the config path
        vlfs.set_rclone_config_path(config_path)

        # Clear env vars to ensure we're not relying on them
        monkeypatch.setenv("RCLONE_CONFIG_R2_ACCESS_KEY_ID", "")
        monkeypatch.setenv("RCLONE_CONFIG_R2_SECRET_ACCESS_KEY", "")
        monkeypatch.setenv("RCLONE_CONFIG_R2_ENDPOINT", "")

        # Mock run_rclone to avoid actual network call
        def mock_run_rclone(args, **kwargs):
            return (0, "", "")

        monkeypatch.setattr(vlfs, "run_rclone", mock_run_rclone)

        # Should succeed without env vars since config is already set
        assert vlfs.validate_r2_connection() is True

        # Clean up
        vlfs.set_rclone_config_path(None)

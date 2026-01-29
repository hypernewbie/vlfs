import pytest
from pathlib import Path
import vlfs


class TestCmdPushR2:
    """Test push command for R2 remote."""

    def test_push_succeeds_with_config_only(
        self, repo_root, monkeypatch, tmp_path, rclone_mock
    ):
        """Push should succeed when env vars missing but config file exists."""
        user_config = tmp_path / "user_config"
        user_config.mkdir()
        monkeypatch.setenv("VLFS_USER_CONFIG", str(user_config))

        # Create valid rclone.conf
        config_path = user_config / "rclone.conf"
        config_path.write_text("[r2]\ntype = s3\nprovider = Cloudflare\n")

        # Clear env vars
        monkeypatch.setenv("RCLONE_CONFIG_R2_ACCESS_KEY_ID", "")
        monkeypatch.setenv("RCLONE_CONFIG_R2_SECRET_ACCESS_KEY", "")
        monkeypatch.setenv("RCLONE_CONFIG_R2_ENDPOINT", "")

        # Mock rclone
        rclone_mock(
            {
                "lsd": (0, "", ""),  # validate_r2_connection
                "ls": (0, "", ""),  # remote_object_exists (returns empty = not exists)
                "copy": (0, "", ""),  # upload_to_r2
            }
        )

        # Create a file to push
        test_file = repo_root / "test.txt"
        test_file.write_bytes(b"content")

        monkeypatch.chdir(repo_root)
        result = vlfs.main(["push", "test.txt"])

        assert result == 0

        # Verify rclone was called with config
        # rclone_mock records calls. We can check if --config was passed if we want,
        # but the main thing is it succeeded despite missing env vars.

    def test_push_fails_without_auth(self, repo_root, monkeypatch, tmp_path, capsys):
        """Push should fail when both env vars and config are missing."""
        user_config = tmp_path / "user_config"
        user_config.mkdir()
        monkeypatch.setenv("VLFS_USER_CONFIG", str(user_config))

        # Clear env vars
        monkeypatch.setenv("RCLONE_CONFIG_R2_ACCESS_KEY_ID", "")
        monkeypatch.setenv("RCLONE_CONFIG_R2_SECRET_ACCESS_KEY", "")
        monkeypatch.setenv("RCLONE_CONFIG_R2_ENDPOINT", "")

        # Create a file to push
        test_file = repo_root / "test.txt"
        test_file.write_bytes(b"content")

        monkeypatch.chdir(repo_root)
        result = vlfs.main(["push", "test.txt"])

        assert result == 1
        captured = capsys.readouterr()
        assert "R2 credentials required" in captured.err

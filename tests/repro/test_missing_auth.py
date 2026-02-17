
import os
import sys
from pathlib import Path
from unittest.mock import patch, MagicMock
import pytest
import vlfs

def test_missing_config_and_env_fails_auth(tmp_path):
    """
    Reproduction test:
    Scenario: User has no environment variables set and no config file exists.
    Expected: ensure_r2_auth should fail (return non-zero) and NOT set the config path.
    """
    # 1. Mock Environment (Clear relevant vars)
    with patch.dict(os.environ, {}, clear=True):
        # 2. Mock User Config Directory (point to empty temp dir)
        with patch("vlfs.get_user_config_dir", return_value=tmp_path):
            # Ensure no config file exists
            config_file = tmp_path / "rclone.conf"
            assert not config_file.exists()

            # 3. Call ensure_r2_auth
            # We verify it calls 'die' or returns non-zero
            # We also mock 'print' and 'sys.stderr' to suppress output
            with patch("builtins.print"), patch("sys.stderr"):
                exit_code = vlfs.ensure_r2_auth()

            # 4. Assert Failure
            assert exit_code != 0, "ensure_r2_auth should fail when no config/env exists"

            # 5. Assert Config Path NOT set
            assert vlfs.get_rclone_config_path() is None

def test_push_aborts_if_no_auth(tmp_path):
    """
    Reproduction test:
    Scenario: cmd_push is called, but auth fails.
    Expected: It should return 1 and NEVER call validate_r2_connection (which runs rclone).
    """
    repo_root = tmp_path / "repo"
    repo_root.mkdir()
    vlfs_dir = repo_root / ".vlfs"
    vlfs_dir.mkdir()
    cache_dir = repo_root / ".vlfs-cache"
    
    # Mock Config Loading to return empty
    with patch("vlfs.load_merged_config", return_value={}):
        # Mock ensure_r2_auth to fail (simulate no creds)
        with patch("vlfs.ensure_r2_auth", return_value=1):
            # Mock validate_r2_connection to fail if called
            with patch("vlfs.validate_r2_connection") as mock_validate:
                exit_code = vlfs.cmd_push(
                    repo_root, 
                    vlfs_dir, 
                    cache_dir, 
                    paths=["somefile"], 
                    private=False, 
                    dry_run=False
                )

                assert exit_code == 1
                mock_validate.assert_not_called()

def test_push_fails_fast_with_read_only_config(tmp_path):
    """
    Reproduction test:
    Scenario: User has a valid rclone.conf (e.g., for read access), but NO write credentials.
    vlfs.ensure_r2_auth might succeed if it finds *any* r2 section, but validation/push should fail.
    
    If ensure_r2_auth checks for SPECIFIC keys (access_key_id), it should fail.
    If it just checks for section existence, it might pass, leading to rclone prompt hang.
    """
    # 1. Create a dummy config with [r2] but NO secrets
    config_dir = tmp_path / "vlfs"
    config_dir.mkdir()
    config_file = config_dir / "rclone.conf"
    config_file.write_text("[r2]\ntype = s3\nprovider = Cloudflare\n")

    # 2. Mock environment (no secrets)
    with patch.dict(os.environ, {}, clear=True):
        # 3. Mock get_user_config_dir to return our temp dir
        with patch("vlfs.get_user_config_dir", return_value=config_dir):
            # 4. Run ensure_r2_auth
            # IT SHOULD FAIL because we need credentials to push
            # Current implementation: 
            #   - Checks env vars -> fails
            #   - Checks config file -> finds [r2] -> SUCCEEDS (returns 0)
            
            # This is the BUG: It succeeds just because [r2] exists, even if empty/invalid.
            exit_code = vlfs.ensure_r2_auth()

            # We expect it to succeed (0) currently, which confirms why it tries to run rclone
            # and potentially hangs if rclone prompts for missing keys.
            assert exit_code == 0

def test_push_uses_interactive_rclone_on_partial_auth(tmp_path):
    """
    Reproduction test:
    Scenario: User has a valid rclone.conf (e.g., for read access), but NO write credentials.
    Verify that when we proceed to 'validate_r2_connection', we call rclone with capture_output=False.
    This ensures the user sees the password/config prompt instead of a silent hang.
    """
    repo_root = tmp_path / "repo"
    repo_root.mkdir()
    vlfs_dir = repo_root / ".vlfs"
    vlfs_dir.mkdir()
    cache_dir = repo_root / ".vlfs-cache"
    
    # Setup partial config
    config_dir = tmp_path / "vlfs"
    config_dir.mkdir()
    config_file = config_dir / "rclone.conf"
    config_file.write_text("[r2]\ntype = s3\nprovider = Cloudflare\n")

    # Mock environment (no secrets)
    with patch.dict(os.environ, {}, clear=True):
        # Mock config location
        with patch("vlfs.get_user_config_dir", return_value=config_dir):
            # Mock subprocess.run so we don't actually hang or run rclone
            with patch("subprocess.run") as mock_run:
                mock_run.return_value.returncode = 0
                
                # Mock ensure_r2_auth to succeed (as it does for partial config)
                # We can rely on the real one, but mocking is safer for unit test isolation 
                # if we want to focus on the subprocess call. 
                # But let's use the real one to prove the flow works.
                
                # Call validate_r2_connection directly or via cmd_push
                # Let's call validate_r2_connection as that's where the call happens
                try:
                    vlfs.validate_r2_connection("test-bucket")
                except Exception:
                    pass # We don't care if it fails later, we care about the call

                # VERIFY: capture_output must be False
                assert mock_run.call_count >= 1
                args, kwargs = mock_run.call_args
                
                # Check the args passed to subprocess.run
                assert kwargs.get("capture_output") is False, "rclone must be interactive (capture_output=False) to avoid silent hangs"
                assert "ls" in args[0]

def test_push_fails_with_empty_config_file(tmp_path):
    """
    Reproduction test:
    Scenario: User has an explicitly EMPTY rclone.conf file (0 bytes).
    Expected: ensure_r2_auth should fail because it cannot find the [r2] section.
    Consequently, cmd_push should abort and NOT run rclone.
    """
    config_dir = tmp_path / "vlfs"
    config_dir.mkdir()
    config_file = config_dir / "rclone.conf"
    config_file.write_text("") # Explicitly empty

    with patch.dict(os.environ, {}, clear=True):
        with patch("vlfs.get_user_config_dir", return_value=config_dir):
            # Mock subprocess to ensure we absolutely do not call it
            with patch("subprocess.run") as mock_run:
                
                # 1. Test ensure_r2_auth directly
                with patch("builtins.print"), patch("sys.stderr"):
                    auth_exit_code = vlfs.ensure_r2_auth()
                
                assert auth_exit_code != 0, "Auth should fail with empty config file"

                # 2. Test cmd_push flow
                # (Need to mock other things cmd_push needs like repo layout)
                repo_root = tmp_path / "repo"
                repo_root.mkdir()
                (repo_root / ".vlfs").mkdir()
                (repo_root / ".vlfs-cache").mkdir()

                with patch("vlfs.load_merged_config", return_value={}):
                    # We expect cmd_push to call ensure_r2_auth, see it fail, and return 1
                    # It should NOT call validate_r2_connection or run_rclone
                    push_exit_code = vlfs.cmd_push(
                        repo_root,
                        repo_root / ".vlfs",
                        repo_root / ".vlfs-cache",
                        paths=["."],
                        private=False,
                        dry_run=False
                    )
                    
                    assert push_exit_code == 1
                    mock_run.assert_not_called()


"""
VLFS - Vibecoded Large File Storage CLI

Copyright 2026 UAA Software

Permission is hereby granted, free of charge, to any person obtaining a copy of
this software and associated documentation files (the “Software”), to deal in
the Software without restriction, including without limitation the rights to
use, copy, modify, merge, publish, distribute, sublicense, and/or sell copies of
the Software, and to permit persons to whom the Software is furnished to do so,
subject to the following conditions:

The above copyright notice and this permission notice shall be included in all
copies or substantial portions of the Software.

THE SOFTWARE IS PROVIDED “AS IS”, WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY, FITNESS
FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE AUTHORS OR
COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER LIABILITY, WHETHER
IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM, OUT OF OR IN
CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE SOFTWARE.
"""

import argparse
import fnmatch
import hashlib
from concurrent.futures import ThreadPoolExecutor, as_completed
import json
import logging
import os
import subprocess
import sys
import tempfile
import time
from pathlib import Path
from typing import Any

import zstandard
from filelock import FileLock as _FileLock


# Module-level logger
logger = logging.getLogger("vlfs")


_RCLONE_CONFIG_PATH: Path | None = None


# =============================================================================
# Exceptions
# =============================================================================


class RcloneError(Exception):
    """Error from rclone subprocess."""

    def __init__(self, message: str, returncode: int, stdout: str, stderr: str):
        super().__init__(message)
        self.returncode = returncode
        self.stdout = stdout
        self.stderr = stderr


class ConfigError(Exception):
    """Error in configuration."""


class VLFSIndexError(Exception):
    """Error in index operations."""


# =============================================================================
# Output Helpers
# =============================================================================


class Colors:
    """ANSI color codes for terminal output."""

    RESET = "\033[0m"
    BOLD = "\033[1m"
    RED = "\033[31m"
    GREEN = "\033[32m"
    YELLOW = "\033[33m"
    BLUE = "\033[34m"
    MAGENTA = "\033[35m"
    CYAN = "\033[36m"
    GRAY = "\033[90m"


def use_color() -> bool:
    """Check if color output should be used.

    Colors are disabled if:
    - NO_COLOR environment variable is set
    - CI environment variable is set
    - stdout is not a TTY

    Returns:
        True if color should be used
    """
    if os.environ.get("NO_COLOR"):
        return False
    if os.environ.get("CI"):
        return False
    if not sys.stdout.isatty():
        return False
    return True


def colorize(text: str, color: str, force: bool = False) -> str:
    """Wrap text in ANSI color codes if appropriate.

    Args:
        text: Text to colorize
        color: Color name (e.g., 'RED', 'GREEN')
        force: Force color even if normally disabled

    Returns:
        Colorized text or plain text
    """
    if not force and not use_color():
        return text
    color_code = getattr(Colors, color.upper(), "")
    if color_code:
        return f"{color_code}{text}{Colors.RESET}"
    return text


def format_bytes(size: int) -> str:
    """Format byte size as human-readable string."""
    for unit in ["B", "KB", "MB", "GB", "TB"]:
        if size < 1024:
            return f"{size:.1f}{unit}"
        size /= 1024
    return f"{size:.1f}PB"


def die(message: str, hint: str | None = None, exit_code: int = 1) -> int:
    """Print error message and exit with optional hint.

    Args:
        message: Error message to display
        hint: Optional remediation hint
        exit_code: Exit code to return

    Returns:
        Exit code (for testing purposes)
    """
    error_msg = f"Error: {message}"
    if use_color():
        error_msg = f"{Colors.RED}{error_msg}{Colors.RESET}"
    print(error_msg, file=sys.stderr)

    if hint:
        hint_msg = f"Hint: {hint}"
        if use_color():
            hint_msg = f"{Colors.YELLOW}{hint_msg}{Colors.RESET}"
        print(hint_msg, file=sys.stderr)

    logger.error(f"Exited with code {exit_code}: {message}")
    if hint:
        logger.error(f"Hint: {hint}")

    return exit_code


# =============================================================================
# Logging
# =============================================================================


def setup_logging(verbosity: int = 0, log_file: bool = True) -> None:
    """Set up logging with console and file handlers.

    Args:
        verbosity: 0=INFO, 1=DEBUG, 2=TRACE (mapped to DEBUG with more detail)
        log_file: Whether to write to log file
    """
    # Determine log level
    if verbosity >= 2:
        level = logging.DEBUG
        fmt = "%(asctime)s - %(name)s - %(levelname)s - %(message)s"
    elif verbosity >= 1:
        level = logging.DEBUG
        fmt = "%(asctime)s - %(levelname)s - %(message)s"
    else:
        level = logging.INFO
        fmt = "%(asctime)s - %(levelname)s - %(message)s"

    # Clear existing handlers
    logger.handlers = []
    logger.setLevel(level)

    # Console handler (only warnings and above for non-verbose)
    console_handler = logging.StreamHandler(sys.stderr)
    console_handler.setLevel(logging.WARNING if verbosity == 0 else level)
    console_handler.setFormatter(logging.Formatter(fmt))
    logger.addHandler(console_handler)

    # File handler
    if log_file:
        log_dir = Path.home() / ".vlfs"
        log_dir.mkdir(parents=True, exist_ok=True)
        log_path = log_dir / "vlfs.log"

        file_handler = logging.FileHandler(log_path, mode="a")
        file_handler.setLevel(logging.DEBUG)  # Always log DEBUG to file
        file_handler.setFormatter(
            logging.Formatter("%(asctime)s - %(name)s - %(levelname)s - %(message)s")
        )
        logger.addHandler(file_handler)

        logger.debug(f"Logging initialized (verbosity={verbosity})")


# =============================================================================
# Low-level Utilities
# =============================================================================


def with_file_lock(path: Path, timeout: float = 10.0):
    """Context manager for cross-platform file locking.

    Uses the filelock package for robust locking with timeout support.

    Args:
        path: Path to lock file
        timeout: Seconds to wait for lock (default 10s, -1 for infinite)

    Returns:
        Context manager that acquires/releases lock
    """
    path.parent.mkdir(parents=True, exist_ok=True)
    return _FileLock(path, timeout=timeout)


def atomic_write_bytes(dest: Path, data: bytes) -> None:
    """Write bytes to dest atomically via temp file + rename."""
    dest.parent.mkdir(parents=True, exist_ok=True)
    fd, temp_path = tempfile.mkstemp(dir=dest.parent)
    try:
        with os.fdopen(fd, "wb") as f:
            f.write(data)
        os.replace(temp_path, dest)
    except Exception:
        try:
            os.close(fd)
        except OSError:
            pass
        if os.path.exists(temp_path):
            os.unlink(temp_path)
        raise


def atomic_write_text(dest: Path, text: str, encoding: str = "utf-8") -> None:
    """Write text to dest atomically."""
    atomic_write_bytes(dest, text.encode(encoding))


def retry(
    callable_fn,
    *,
    attempts: int = 3,
    base_delay: float = 1.0,
    max_delay: float = 30.0,
    exceptions: tuple = (RcloneError,),
):
    """Retry a callable with exponential backoff.

    Args:
        callable_fn: Function to call
        attempts: Maximum number of attempts
        base_delay: Initial delay between retries in seconds
        max_delay: Maximum delay between retries
        exceptions: Tuple of exceptions to catch and retry

    Returns:
        Result of callable_fn

    Raises:
        Last exception if all attempts fail
    """
    last_exception = None

    for attempt in range(attempts):
        try:
            return callable_fn()
        except exceptions as e:
            last_exception = e
            if attempt < attempts - 1:
                delay = min(base_delay * (2**attempt), max_delay)
                time.sleep(delay)

    raise last_exception


# =============================================================================
# Hashing & Compression
# =============================================================================


def hash_file(path: Path) -> tuple[str, int, float]:
    """Compute SHA256 hash of file, return (hex_digest, size, mtime)."""
    sha256 = hashlib.sha256()
    size = 0

    with path.open("rb") as f:
        while True:
            chunk = f.read(65536)  # 64KB chunks
            if not chunk:
                break
            sha256.update(chunk)
            size += len(chunk)

    mtime = path.stat().st_mtime
    return sha256.hexdigest().lower(), size, mtime


def hash_files_parallel(
    paths: list[Path], max_workers: int | None = None
) -> tuple[dict[Path, tuple[str, int, float]], dict[Path, Exception]]:
    """Hash files in parallel using a thread pool.

    Returns:
        Tuple of (results, errors) where results maps Path -> (hash, size, mtime)
        and errors maps Path -> Exception.
    """
    if not paths:
        return {}, {}

    if max_workers is None:
        cpu_count = os.cpu_count() or 4
        max_workers = min(32, cpu_count * 2)

    results: dict[Path, tuple[str, int, float]] = {}
    errors: dict[Path, Exception] = {}

    with ThreadPoolExecutor(max_workers=max_workers) as executor:
        future_map = {executor.submit(hash_file, path): path for path in paths}
        for future in as_completed(future_map):
            path = future_map[future]
            try:
                results[path] = future.result()
            except (OSError, IOError) as exc:
                errors[path] = exc

    return results, errors


def shard_path(hex_digest: str) -> str:
    """Convert hex digest to sharded path (ab/cd/abcdef...)."""
    hex_lower = hex_digest.lower()
    if len(hex_lower) < 4:
        return hex_lower
    return f"{hex_lower[:2]}/{hex_lower[2:4]}/{hex_lower}"


def compress_bytes(data: bytes, level: int = 3) -> bytes:
    """Compress data using zstandard."""
    cctx = zstandard.ZstdCompressor(level=level)
    return cctx.compress(data)


def decompress_bytes(data: bytes) -> bytes:
    """Decompress zstandard data."""
    dctx = zstandard.ZstdDecompressor()
    return dctx.decompress(data)


# =============================================================================
# Cache Operations
# =============================================================================


def store_object(src_path: Path, cache_dir: Path, compression_level: int = 3) -> str:
    """Store file in cache, return object key."""
    hex_digest, _, _ = hash_file(src_path)
    object_key = shard_path(hex_digest)
    object_path = cache_dir / "objects" / object_key

    # If already exists, skip
    if object_path.exists():
        return object_key

    # Read, compress, and store atomically
    data = src_path.read_bytes()
    compressed = compress_bytes(data, level=compression_level)

    atomic_write_bytes(object_path, compressed)

    return object_key


def load_object(object_key: str, cache_dir: Path) -> bytes:
    """Load and decompress object from cache."""
    object_path = cache_dir / "objects" / object_key
    compressed = object_path.read_bytes()
    return decompress_bytes(compressed)


# =============================================================================
# Index Operations
# =============================================================================


def read_index(vlfs_dir: Path) -> dict[str, Any]:
    """Read index.json, return entries dict."""
    index_path = vlfs_dir / "index.json"
    if not index_path.exists():
        return {"version": 1, "entries": {}}

    with index_path.open("r") as f:
        data = json.load(f)

    # Version guard
    if data.get("version") != 1:
        raise VLFSIndexError(f"Unsupported index version: {data.get('version')}")

    return data


def write_index(vlfs_dir: Path, data: dict[str, Any]) -> None:
    """Write index.json atomically."""
    index_path = vlfs_dir / "index.json"
    atomic_write_text(index_path, json.dumps(data, indent=2))


def update_index_entries(vlfs_dir: Path, updates: dict[str, dict[str, Any]]) -> None:
    """Update index entries and write once atomically."""
    if not updates:
        return

    with with_file_lock(vlfs_dir / "index.lock"):
        index = read_index(vlfs_dir)
        index_entries = index.get("entries", {})
        index_entries.update(updates)
        index["entries"] = index_entries
        write_index(vlfs_dir, index)


# =============================================================================
# Configuration
# =============================================================================


def get_user_config_dir() -> Path:
    """Return ~/.config/vlfs/, creating if needed."""
    env_override = os.environ.get("VLFS_USER_CONFIG")
    if env_override:
        config_dir = Path(env_override)
    elif os.name == "nt":
        base = Path(os.environ.get("APPDATA", Path.home() / "AppData" / "Roaming"))
        config_dir = base / "vlfs"
    else:
        base = Path(os.environ.get("XDG_CONFIG_HOME", Path.home() / ".config"))
        config_dir = base / "vlfs"

    config_dir.mkdir(parents=True, exist_ok=True)
    return config_dir


def resolve_paths(repo_root: Path) -> tuple[Path, Path]:
    """Resolve VLFS directories, honoring environment overrides."""
    config_path = os.environ.get("VLFS_CONFIG")
    if config_path:
        vlfs_dir = Path(config_path).parent
    else:
        vlfs_dir = repo_root / ".vlfs"

    cache_path = os.environ.get("VLFS_CACHE")
    if cache_path:
        cache_dir = Path(cache_path)
    else:
        cache_dir = repo_root / ".vlfs-cache"

    return vlfs_dir, cache_dir


def ensure_dirs(vlfs_dir: Path, cache_dir: Path) -> None:
    """Create required directory structure if missing."""
    vlfs_dir.mkdir(parents=True, exist_ok=True)
    (cache_dir / "objects").mkdir(parents=True, exist_ok=True)


def ensure_gitignore(repo_root: Path) -> None:
    """Ensure .gitignore has required entries."""
    gitignore = repo_root / ".gitignore"

    required_entries = [
        ".vlfs-cache/",
    ]

    existing_content = ""
    if gitignore.exists():
        existing_content = gitignore.read_text()

    entries_to_add = []
    for entry in required_entries:
        if entry not in existing_content:
            entries_to_add.append(entry)

    if entries_to_add:
        with gitignore.open("a") as f:
            if existing_content and not existing_content.endswith("\n"):
                f.write("\n")
            for entry in entries_to_add:
                f.write(f"{entry}\n")


def load_config(vlfs_dir: Path) -> dict[str, Any]:
    """Load configuration from TOML file."""
    config_file = vlfs_dir / "config.toml"
    if not config_file.exists():
        return {}

    try:
        import tomllib
    except ImportError:
        import tomli as tomllib

    with config_file.open("rb") as f:
        return tomllib.load(f)


def deep_merge(target: dict, source: dict) -> dict:
    """Deep merge two dictionaries."""
    result = target.copy()
    for key, value in source.items():
        if key in result and isinstance(result[key], dict) and isinstance(value, dict):
            result[key] = deep_merge(result[key], value)
        else:
            result[key] = value
    return result


def load_merged_config(vlfs_dir: Path) -> dict[str, Any]:
    """Load repo config, then overlay user config."""
    repo_config = load_config(vlfs_dir)

    user_config_path = get_user_config_dir() / "config.toml"
    user_config = {}
    if user_config_path.exists():
        import tomllib

        with user_config_path.open("rb") as f:
            user_config = tomllib.load(f)

    return deep_merge(repo_config, user_config)


def warn_if_secrets_in_repo(vlfs_dir: Path) -> None:
    """Warn if secrets detected in repo config."""
    config_path = vlfs_dir / "config.toml"
    if not config_path.exists():
        return
    content = config_path.read_text()
    if "client_secret" in content or "secret_access_key" in content:
        print(
            colorize("Warning: Secrets detected in .vlfs/config.toml", "YELLOW"),
            file=sys.stderr,
        )
        print("Move secrets to ~/.config/vlfs/config.toml", file=sys.stderr)


# =============================================================================
# Rclone
# =============================================================================


def set_rclone_config_path(path: Path | None) -> None:
    """Set global rclone config path for this run."""
    global _RCLONE_CONFIG_PATH
    if path and path.exists():
        _RCLONE_CONFIG_PATH = path
    else:
        _RCLONE_CONFIG_PATH = None


def get_rclone_config_path() -> Path | None:
    """Get the current rclone config path."""
    return _RCLONE_CONFIG_PATH


def run_rclone(
    args: list[str],
    *,
    env: dict[str, str] | None = None,
    cwd: Path | str | None = None,
    timeout: float | None = None,
) -> tuple[int, str, str]:
    """Run rclone subprocess and return (returncode, stdout, stderr).

    Args:
        args: Command line arguments for rclone (not including 'rclone')
        env: Optional environment variables to add/override
        cwd: Optional working directory
        timeout: Optional timeout in seconds

    Returns:
        Tuple of (returncode, stdout, stderr)

    Raises:
        RcloneError: If returncode is non-zero
    """
    cmd = ["rclone"] + args
    config_path = get_rclone_config_path()
    if config_path:
        cmd += ["--config", str(config_path)]

    run_env = None
    if env:
        run_env = os.environ.copy()
        run_env.update(env)

    try:
        result = subprocess.run(
            cmd, capture_output=True, text=True, env=run_env, cwd=cwd, timeout=timeout
        )
    except FileNotFoundError as exc:
        raise RcloneError(
            "rclone not found in PATH. Install from https://rclone.org/downloads/",
            127,
            "",
            str(exc),
        )

    if result.returncode != 0:
        raise RcloneError(
            f"rclone failed with code {result.returncode}: {result.stderr}",
            result.returncode,
            result.stdout,
            result.stderr,
        )

    return result.returncode, result.stdout, result.stderr


def rclone_config_has_section(path: Path, section: str) -> bool:
    """Check if rclone config file has a specific section."""
    import configparser

    if not path.exists():
        return False
    parser = configparser.ConfigParser()
    try:
        parser.read(str(path))
        return parser.has_section(section)
    except Exception:
        return False


def write_rclone_drive_config(config_dir: Path, config: dict[str, str]) -> None:
    """Write rclone config for Google Drive.

    Args:
        config_dir: Directory to write rclone.conf to (typically user config dir)
        config: Dict with client_id, client_secret, etc.
    """
    config_path = config_dir / "rclone.conf"
    config_lines = ["[gdrive]", "type = drive"]

    for key, value in config.items():
        config_lines.append(f"{key} = {value}")

    atomic_write_text(config_path, "\n".join(config_lines) + "\n")


def write_rclone_r2_config(dest_dir: Path) -> None:
    """Generate rclone.conf with R2 settings in dest_dir."""
    # Load config from repo (for provider settings etc)
    # We assume we are in a repo, so find .vlfs
    try:
        vlfs_dir, _ = resolve_paths(Path.cwd())
        repo_config = load_merged_config(vlfs_dir)
    except:
        repo_config = {}

    config_path = dest_dir / "rclone.conf"

    # Read existing config if present
    existing_lines = []
    if config_path.exists():
        existing_lines = config_path.read_text().splitlines()

    new_lines = []
    in_r2 = False

    # Copy everything except [r2] section
    for line in existing_lines:
        stripped = line.strip()
        if stripped == "[r2]":
            in_r2 = True
            continue
        if in_r2 and stripped.startswith("["):
            in_r2 = False

        if not in_r2:
            new_lines.append(line)

    # Add/Update [r2] section
    r2_config = repo_config.get("remotes", {}).get("r2", {})

    # Get secrets from env
    try:
        env_config = get_r2_config_from_env()
    except ConfigError:
        # If pushing, we need creds. But this might be called just to ensure config exists.
        env_config = {}

    if env_config:
        new_lines.append("")
        new_lines.append("[r2]")
        new_lines.append("type = s3")

        # Add provider specific settings
        provider = r2_config.get("provider", "Cloudflare")
        new_lines.append(f"provider = {provider}")

        endpoint = r2_config.get("endpoint")
        if not endpoint and "endpoint" in env_config:
            endpoint = env_config["endpoint"]
        if endpoint:
            new_lines.append(f"endpoint = {endpoint}")

        new_lines.append(f"access_key_id = {env_config['access_key_id']}")
        new_lines.append(f"secret_access_key = {env_config['secret_access_key']}")

        # Write back
        atomic_write_text(config_path, "\n".join(new_lines) + "\n")


# =============================================================================
# R2 Operations
# =============================================================================


def get_r2_config_from_env() -> dict[str, str]:
    """Get R2 configuration from environment variables.

    Required env vars:
        RCLONE_CONFIG_R2_ACCESS_KEY_ID
        RCLONE_CONFIG_R2_SECRET_ACCESS_KEY
        RCLONE_CONFIG_R2_ENDPOINT

    Returns:
        Dict with rclone config keys

    Raises:
        ConfigError: If required env vars are missing
    """
    required = [
        "RCLONE_CONFIG_R2_ACCESS_KEY_ID",
        "RCLONE_CONFIG_R2_SECRET_ACCESS_KEY",
        "RCLONE_CONFIG_R2_ENDPOINT",
    ]

    config = {}
    missing = []

    for var in required:
        value = os.environ.get(var)
        if not value:
            missing.append(var)
        else:
            # Convert env var name to rclone config key
            # RCLONE_CONFIG_R2_ACCESS_KEY_ID -> access_key_id
            key = var.replace("RCLONE_CONFIG_R2_", "").lower()
            config[key] = value

    if missing:
        raise ConfigError(
            f"Missing R2 credentials. Set these environment variables:\n"
            + "\n".join(f"  {var}" for var in missing)
        )

    return config


def ensure_r2_auth() -> int:
    """Ensure R2 authentication is available via env vars or config file.

    Returns:
        0 on success, calls die() on failure.
    """
    user_dir = get_user_config_dir()
    config_path = user_dir / "rclone.conf"

    # Check environment variables first (legacy/CI priority)
    try:
        # If env vars are present, generate config from them
        get_r2_config_from_env()
        write_rclone_r2_config(user_dir)
        set_rclone_config_path(config_path)
        return 0
    except ConfigError:
        pass

    # Check for existing config file with [r2] section
    if rclone_config_has_section(config_path, "r2"):
        set_rclone_config_path(config_path)
        return 0

    return die(
        "R2 credentials required for push",
        hint="Set RCLONE_CONFIG_R2_* env vars or create ~/.config/vlfs/rclone.conf with [r2] section",
    )


def validate_r2_connection(bucket: str = "vlfs") -> bool:
    """Validate R2 connection by listing bucket.

    Args:
        bucket: Bucket name to test

    Returns:
        True if connection succeeds

    Raises:
        RcloneError: If connection fails
        ConfigError: If credentials missing
    """
    # Ensure config is available (will raise ConfigError if not)
    if not get_rclone_config_path():
        try:
            get_r2_config_from_env()
        except ConfigError:
            # If no env vars and no config path set, check if we can set it from default user location
            user_config = get_user_config_dir() / "rclone.conf"
            if rclone_config_has_section(user_config, "r2"):
                set_rclone_config_path(user_config)
            else:
                # Re-raise original error to prompt for env vars or config
                raise

    # Test with lsd command
    run_rclone(["lsd", f"r2:{bucket}"])
    return True


def remote_object_exists(object_key: str, bucket: str = "vlfs") -> bool:
    """Check if object exists in remote R2 bucket.

    Args:
        object_key: The object key (sharded path)
        bucket: Bucket name

    Returns:
        True if object exists
    """
    try:
        # Use rclone ls to check existence
        # ls returns 0 with empty output if path doesn't exist
        returncode, stdout, _ = run_rclone(["ls", f"r2:{bucket}/{object_key}"])
        return returncode == 0 and stdout.strip() != ""
    except RcloneError:
        return False


def upload_to_r2(
    local_path: Path, object_key: str, bucket: str = "vlfs", dry_run: bool = False
) -> bool:
    """Upload a local file to R2.

    Args:
        local_path: Path to local file
        object_key: Destination object key in R2
        bucket: Bucket name
        dry_run: If True, don't actually upload

    Returns:
        True if upload succeeded or object already exists
    """
    if dry_run:
        print(f"[DRY-RUN] Would upload {local_path} -> r2:{bucket}/{object_key}")
        return True

    # Check if already exists
    if remote_object_exists(object_key, bucket):
        return True

    # Upload using rclone copy
    remote_path = f"r2:{bucket}/{object_key}"

    def do_upload():
        run_rclone(["copy", str(local_path), remote_path])

    retry(do_upload, attempts=3, base_delay=1.0)
    return True


def download_from_r2(
    object_keys: list[str], cache_dir: Path, bucket: str = "vlfs", dry_run: bool = False
) -> int:
    """Download multiple objects from R2 to cache.

    Args:
        object_keys: List of object keys to download
        cache_dir: Local cache directory
        bucket: Bucket name
        dry_run: If True, don't actually download

    Returns:
        Number of objects downloaded
    """
    if not object_keys:
        return 0

    # Validate R2 credentials before attempting download
    if not dry_run:
        get_r2_config_from_env()

    if dry_run:
        for key in object_keys:
            print(f"[DRY-RUN] Would download r2:{bucket}/{key}")
        return len(object_keys)

    # Build files-from list for batch download
    files_list = object_keys

    # Write to temp file
    with tempfile.NamedTemporaryFile(mode="w", suffix=".txt", delete=False) as f:
        f.write("\n".join(files_list))
        files_from_path = f.name

    try:
        # Download with rclone copy using --files-from
        def do_download():
            run_rclone(
                [
                    "copy",
                    f"r2:{bucket}",
                    str(cache_dir / "objects"),
                    "--files-from",
                    files_from_path,
                    "--transfers",
                    "8",
                ]
            )

        retry(do_download, attempts=3, base_delay=1.0)
        return len(object_keys)
    finally:
        os.unlink(files_from_path)


def download_http(url: str, dest: Path, timeout: float = 60) -> None:
    """Download URL to dest atomically."""
    import urllib.request
    import urllib.error

    dest.parent.mkdir(parents=True, exist_ok=True)
    fd, temp_path = tempfile.mkstemp(dir=dest.parent)
    try:
        with urllib.request.urlopen(url, timeout=timeout) as resp:
            while chunk := resp.read(65536):
                os.write(fd, chunk)
        os.close(fd)
        os.replace(temp_path, dest)
    except Exception:
        try:
            os.close(fd)
        except OSError:
            pass
        if os.path.exists(temp_path):
            os.unlink(temp_path)
        raise


def download_from_r2_http(
    object_keys: list[str], cache_dir: Path, base_url: str, dry_run: bool = False
) -> int:
    """Download objects via HTTP (no auth required)."""
    downloaded = 0
    # Use ThreadPool for parallel downloads

    def _download_one(key: str) -> bool:
        dest = cache_dir / "objects" / key
        if dest.exists():
            return False

        url = f"{base_url.rstrip('/')}/{key}"
        if dry_run:
            print(f"[DRY-RUN] Would download {url}")
            return True

        try:
            download_http(url, dest)
            return True
        except Exception as e:
            print(f"Error downloading {url}: {e}", file=sys.stderr)
            return False

    with ThreadPoolExecutor(max_workers=8) as executor:
        futures = [executor.submit(_download_one, key) for key in object_keys]
        for future in as_completed(futures):
            if future.result():
                downloaded += 1

    return downloaded


# =============================================================================
# Google Drive Operations
# =============================================================================


def has_drive_token() -> bool:
    """Check if Google Drive token exists.

    In CI environments (CI env var or VLFS_NO_DRIVE set), fails gracefully.

    Returns:
        True if token file exists

    Raises:
        RuntimeError: In CI environments without token
    """
    user_dir = get_user_config_dir()
    token_path = user_dir / "gdrive-token.json"

    # Check for CI environment
    if os.environ.get("CI") or os.environ.get("VLFS_NO_DRIVE"):
        if not token_path.exists():
            raise RuntimeError(
                "Google Drive is not available in CI environments.\n"
                "Use R2 for CI-friendly storage or set up Drive auth locally first."
            )

    return token_path.exists()


def auth_gdrive(vlfs_dir: Path) -> int:
    """Interactive Google Drive authentication using rclone's built-in OAuth.

    Args:
        vlfs_dir: Path to .vlfs directory (unused, kept for compat)

    Returns:
        Exit code (0 for success)
    """
    user_dir = get_user_config_dir()
    config_file = user_dir / "rclone.conf"
    token_file = user_dir / "gdrive-token.json"

    print("Setting up Google Drive authentication...")
    print("A browser will open for you to authorise access to Google Drive.")
    print()

    try:
        # Use rclone config create with built-in OAuth
        # This creates a remote named 'gdrive' of type 'drive'
        subprocess.run(
            [
                "rclone",
                "config",
                "create",
                "gdrive",
                "drive",
                "--config",
                str(config_file),
            ],
            check=True,
        )

        # Extract token from generated config
        import configparser

        parser = configparser.ConfigParser()
        parser.read(str(config_file))

        token = parser.get("gdrive", "token", fallback="")
        if not token:
            return die(
                "Drive token not found in rclone.conf",
                hint="Complete rclone auth and retry",
            )

        # If token is quoted JSON, unquote it
        if token.startswith('"') and token.endswith('"'):
            token = json.loads(token)

        # Write token file atomically (raw JSON content)
        atomic_write_text(token_file, token)

        print()
        print("Google Drive authentication complete!")
        print(f"Token saved to: {token_file}")
        return 0

    except subprocess.CalledProcessError as e:
        return die("rclone auth failed", hint=str(e))
    except FileNotFoundError:
        return die(
            "rclone not found in PATH",
            hint="Install from https://rclone.org/downloads/",
        )


def upload_to_drive(
    local_path: Path, object_key: str, bucket: str = "vlfs", dry_run: bool = False
) -> bool:
    """Upload a local file to Google Drive with rate limiting.

    Args:
        local_path: Path to local file
        object_key: Destination object key in Drive
        bucket: Bucket/path name in Drive
        dry_run: If True, don't actually upload

    Returns:
        True if upload succeeded
    """
    if dry_run:
        print(f"[DRY-RUN] Would upload {local_path} -> gdrive:{bucket}/{object_key}")
        return True

    remote_path = f"gdrive:{bucket}/{object_key}"

    def do_upload():
        run_rclone(
            [
                "copy",
                str(local_path),
                remote_path,
                "--transfers",
                "1",
                "--drive-chunk-size",
                "8M",
            ]
        )

    # Use more retries for Drive due to rate limiting
    def do_upload_with_drive_retry():
        last_exception = None
        for attempt in range(5):
            try:
                do_upload()
                return
            except RcloneError as e:
                last_exception = e
                # Check for rate limit errors (403/429)
                if (
                    "403" in e.stderr
                    or "429" in e.stderr
                    or "rateLimitExceeded" in e.stderr
                ):
                    delay = min(2**attempt * 2, 60)  # Max 60s delay
                    print(f"Rate limited, waiting {delay}s...")
                    time.sleep(delay)
                else:
                    raise
        raise last_exception

    do_upload_with_drive_retry()
    return True


def download_from_drive(
    object_keys: list[str], cache_dir: Path, bucket: str = "vlfs", dry_run: bool = False
) -> int:
    """Download multiple objects from Drive to cache with rate limiting.

    Args:
        object_keys: List of object keys to download
        cache_dir: Local cache directory
        bucket: Bucket/path name in Drive
        dry_run: If True, don't actually download

    Returns:
        Number of objects downloaded
    """
    if not object_keys:
        return 0

    if dry_run:
        for key in object_keys:
            print(f"[DRY-RUN] Would download gdrive:{bucket}/{key}")
        return len(object_keys)

    # Build files-from list for batch download
    files_list = object_keys

    # Write to temp file
    with tempfile.NamedTemporaryFile(mode="w", suffix=".txt", delete=False) as f:
        f.write("\n".join(files_list))
        files_from_path = f.name

    try:
        # Download with rclone copy using --files-from, limited to 1 transfer
        def do_download():
            run_rclone(
                [
                    "copy",
                    f"gdrive:{bucket}",
                    str(cache_dir / "objects"),
                    "--files-from",
                    files_from_path,
                    "--transfers",
                    "1",
                    "--drive-chunk-size",
                    "8M",
                ]
            )

        retry(do_download, attempts=5, base_delay=2.0)
        return len(object_keys)
    finally:
        os.unlink(files_from_path)


# =============================================================================
# Workspace Operations
# =============================================================================


def compute_missing_objects(index: dict[str, Any], cache_dir: Path) -> list[str]:
    """Compute list of objects that need to be downloaded.

    Args:
        index: Index dict with entries
        cache_dir: Local cache directory

    Returns:
        List of object keys not in local cache
    """
    missing = []
    entries = index.get("entries", {})

    for rel_path, entry in entries.items():
        object_key = entry.get("object_key")
        if not object_key:
            continue

        object_path = cache_dir / "objects" / object_key
        if not object_path.exists():
            missing.append(object_key)

    # Remove duplicates while preserving order
    seen = set()
    unique_missing = []
    for key in missing:
        if key not in seen:
            seen.add(key)
            unique_missing.append(key)

    return unique_missing


def materialize_workspace(
    index: dict[str, Any],
    repo_root: Path,
    cache_dir: Path,
    force: bool = False,
    dry_run: bool = False,
) -> tuple[int, int, list[str]]:
    """Decompress objects from cache into workspace.

    Args:
        index: Index dict with entries
        repo_root: Repository root path
        cache_dir: Local cache directory
        force: If True, overwrite modified files
        dry_run: If True, don't actually write files

    Returns:
        Tuple of (files_written, bytes_written, skipped_files)
    """
    entries = index.get("entries", {})
    files_written = 0
    bytes_written = 0
    skipped_files = []

    for rel_path, entry in entries.items():
        object_key = entry.get("object_key")
        if not object_key:
            continue

        # Target path in workspace
        file_path = repo_root / rel_path.replace("/", os.sep)

        # Check if file exists
        if file_path.exists():
            try:
                hex_digest, _, _ = hash_file(file_path)
                # If matches target, we are good (already up to date)
                if hex_digest == entry.get("hash"):
                    continue

                # If different, and NOT force, skip
                if not force:
                    skipped_files.append(rel_path)
                    continue
            except (OSError, IOError):
                pass  # Will overwrite if we can't read/hash

        # Load from cache
        try:
            data = load_object(object_key, cache_dir)
        except (OSError, IOError):
            continue  # Will be missing

        if dry_run:
            print(f"[DRY-RUN] Would write {rel_path} ({format_bytes(len(data))})")
            files_written += 1
            bytes_written += len(data)
            continue

        # Write atomically
        atomic_write_bytes(file_path, data)
        files_written += 1
        bytes_written += len(data)

    return files_written, bytes_written, skipped_files


def _find_untracked_files(
    repo_root: Path, entries: dict[str, Any], patterns: list[str]
) -> list[str]:
    """Find files matching patterns that are not in the index."""
    extra = []
    ignored_dirs = {
        ".git",
        ".vlfs",
        ".vlfs-cache",
        "__pycache__",
        "node_modules",
        "venv",
        ".env",
    }

    for root, dirs, files in os.walk(repo_root):
        dirs[:] = [d for d in dirs if d not in ignored_dirs]

        for file in files:
            file_path = Path(root) / file
            rel_path = file_path.relative_to(repo_root)
            rel_str = str(rel_path).replace(os.sep, "/")

            # Skip if already in index
            if rel_str in entries:
                continue

            # Check if matches tracked patterns
            if any(fnmatch.fnmatch(file, p) for p in patterns):
                extra.append(rel_str)

    return extra


def compute_status(index: dict[str, Any], repo_root: Path) -> dict[str, list[str]]:
    """Compare workspace against index, return categorized lists."""
    entries = index.get("entries", {})

    missing = []
    modified = []

    # Check indexed files
    to_hash: list[tuple[str, Path, dict[str, Any]]] = []
    for rel_path, entry in entries.items():
        file_path = repo_root / rel_path.replace("/", os.sep)
        if not file_path.exists():
            missing.append(rel_path)
            continue

        # Check if modified (size or mtime changed)
        stat = file_path.stat()
        if stat.st_size != entry.get("size") or stat.st_mtime != entry.get("mtime"):
            to_hash.append((rel_path, file_path, entry))

    if to_hash:
        paths_to_hash = [item[1] for item in to_hash]
        if len(paths_to_hash) >= 8:
            results, errors = hash_files_parallel(paths_to_hash)
        else:
            results = {}
            errors = {}
            for path in paths_to_hash:
                try:
                    results[path] = hash_file(path)
                except (OSError, IOError) as exc:
                    errors[path] = exc

        for rel_path, file_path, entry in to_hash:
            if file_path in errors:
                modified.append(rel_path)
                continue
            current_hash = results[file_path][0]
            if current_hash != entry.get("hash"):
                modified.append(rel_path)

    # Find extra files
    # Load config for patterns
    config = load_config(repo_root / ".vlfs")  # Assuming standard location
    tracking = config.get("tracking", {})
    patterns = tracking.get("patterns", [])

    # Default to common large file types if no patterns configured
    if not patterns:
        patterns = ["*.psd", "*.zip", "*.exe", "*.dll", "*.lib", "*.iso", "*.mp4"]

    extra = _find_untracked_files(repo_root, entries, patterns)

    return {"missing": missing, "modified": modified, "extra": extra}


def group_objects_by_remote(index: dict[str, Any]) -> dict[str, list[tuple[str, str]]]:
    """Group index entries by remote backend.

    Args:
        index: Index dict with entries

    Returns:
        Dict mapping remote name -> list of (object_key, rel_path) tuples
    """
    groups: dict[str, list[tuple[str, str]]] = {}
    entries = index.get("entries", {})

    for rel_path, entry in entries.items():
        object_key = entry.get("object_key")
        remote = entry.get("remote", "r2")  # Default to r2 for backwards compatibility

        if not object_key:
            continue

        if remote not in groups:
            groups[remote] = []
        groups[remote].append((object_key, rel_path))

    return groups


# =============================================================================
# Commands
# =============================================================================


def cmd_status(
    repo_root: Path,
    vlfs_dir: Path,
    dry_run: bool = False,
    json_output: bool = False,
    force_color: bool = False,
) -> int:
    """Execute status command with enhanced output."""
    try:
        index = read_index(vlfs_dir)
    except VLFSIndexError as e:
        print(f"Error: {e}", file=sys.stderr)
        return 1

    status = compute_status(index, repo_root)

    if json_output:
        print(json.dumps(status, indent=2))
        return 0

    total_changes = (
        len(status["missing"]) + len(status["modified"]) + len(status["extra"])
    )

    if total_changes == 0:
        print(colorize("Workspace is up to date", "GREEN", force_color))
    else:
        if status["missing"]:
            print(
                f"{colorize('Missing:', 'RED', force_color)} {len(status['missing'])}"
            )
            for path in status["missing"][:10]:  # Show first 10
                print(f"  {colorize(path, 'RED', force_color)}")
            if len(status["missing"]) > 10:
                print(f"  ... and {len(status['missing']) - 10} more")
        if status["modified"]:
            print(
                f"{colorize('Modified:', 'YELLOW', force_color)} {len(status['modified'])}"
            )
            for path in status["modified"][:10]:
                print(f"  {colorize(path, 'YELLOW', force_color)}")
            if len(status["modified"]) > 10:
                print(f"  ... and {len(status['modified']) - 10} more")
        if status["extra"]:
            print(
                f"{colorize('Extra (untracked):', 'MAGENTA', force_color)} {len(status['extra'])}"
            )
            for path in status["extra"][:10]:
                print(f"  {colorize(path, 'MAGENTA', force_color)}")
            if len(status["extra"]) > 10:
                print(f"  ... and {len(status['extra']) - 10} more")
            print(f"  (Add with: vlfs push --glob ...)")

    return 0


def cmd_pull(
    repo_root: Path,
    vlfs_dir: Path,
    cache_dir: Path,
    force: bool = False,
    dry_run: bool = False,
) -> int:
    """Execute pull command with support for mixed remotes."""
    try:
        index = read_index(vlfs_dir)
    except VLFSIndexError as e:
        print(f"Error: {e}", file=sys.stderr)
        return 1

    if not index.get("entries"):
        print("No files to pull (index is empty)")
        return 0

    # Load merged config
    config = load_merged_config(vlfs_dir)

    # Check if we can use HTTP download for R2
    r2_public_url = config.get("remotes", {}).get("r2", {}).get("public_base_url")
    r2_bucket = config.get("remotes", {}).get("r2", {}).get("bucket", "vlfs")
    drive_bucket = config.get("remotes", {}).get("gdrive", {}).get("bucket", "vlfs")

    # Update rclone config with R2 settings only if needed (push or no public URL)
    # But for pull, if we have public URL, we don't strictly need rclone config

    # Group objects by remote
    remote_groups = group_objects_by_remote(index)

    # Validate connection only if not using HTTP
    if not dry_run and "r2" in remote_groups and not r2_public_url:
        try:
            validate_r2_connection(bucket=r2_bucket)
        except (RcloneError, ConfigError) as e:
            print(f"Error: {e}", file=sys.stderr)
            if isinstance(e, ConfigError):
                print(
                    "Hint: Set R2 credentials via RCLONE_CONFIG_R2_* env vars",
                    file=sys.stderr,
                )
            return 1

    total_downloaded = 0
    total_objects = 0

    # Build map of object key -> compressed size for progress reporting
    key_sizes = {}
    for entry in index.get("entries", {}).values():
        k = entry.get("object_key")
        s = entry.get("compressed_size", 0)
        if k:
            key_sizes[k] = s

    for remote, objects in remote_groups.items():
        object_keys = [obj[0] for obj in objects]
        total_objects += len(object_keys)

        try:
            downloaded = _download_remote_group(
                remote,
                object_keys,
                cache_dir,
                key_sizes,
                r2_public_url,
                dry_run,
                r2_bucket=r2_bucket,
                drive_bucket=drive_bucket,
            )
        except (RcloneError, ConfigError) as e:
            print(f"Error downloading from {remote}: {e}", file=sys.stderr)
            if isinstance(e, ConfigError):
                print(
                    "Hint: Set R2 credentials via RCLONE_CONFIG_R2_* env vars",
                    file=sys.stderr,
                )
            return 1

        total_downloaded += downloaded

    # Materialize workspace
    files_written, bytes_written, skipped = materialize_workspace(
        index, repo_root, cache_dir, force, dry_run
    )

    if skipped:
        print(
            f"Skipped {len(skipped)} files due to local modifications (use --force to overwrite):"
        )
        for path in skipped[:10]:
            print(f"  {path}")
        if len(skipped) > 10:
            print(f"  ... and {len(skipped) - 10} more")

    if dry_run:
        print(
            f"[DRY-RUN] Would write {files_written} files ({format_bytes(bytes_written)})"
        )
    else:
        print(f"Wrote {files_written} files ({format_bytes(bytes_written)})")

    return 0


def cmd_push(
    repo_root: Path,
    vlfs_dir: Path,
    cache_dir: Path,
    path: str,
    private: bool,
    dry_run: bool = False,
) -> int:
    """Execute push command. Handles both files and directories."""
    # Load merged config
    config = load_merged_config(vlfs_dir)
    r2_bucket = config.get("remotes", {}).get("r2", {}).get("bucket", "vlfs")
    drive_bucket = config.get("remotes", {}).get("gdrive", {}).get("bucket", "vlfs")

    # Validate connection before starting (unless dry run)
    if not dry_run and not private:
        if ensure_r2_auth() != 0:
            return 1

        try:
            validate_r2_connection(bucket=r2_bucket)
        except (RcloneError, ConfigError) as e:
            print(f"Error: {e}", file=sys.stderr)
            return 1

    src_path = Path(path)
    # Resolve relative to repo_root if not absolute
    if not src_path.is_absolute():
        src_path = repo_root / src_path
    src_path = src_path.resolve()

    if not src_path.exists():
        print(f"Error: File not found: {path}", file=sys.stderr)
        return 1

    # Load config for compression level
    compression_level = config.get("defaults", {}).get("compression_level", 3)

    # Handle directory
    if src_path.is_dir():
        files = _find_files_recursive(repo_root, src_path)
        if not files:
            print(f"No files found in directory: {path}")
            return 0

        print(f"Pushing {len(files)} files from {path}...")
        failed = []
        updates: dict[str, dict[str, Any]] = {}
        for file_path in files:
            result, entry = _push_single_file_collect(
                repo_root,
                vlfs_dir,
                cache_dir,
                file_path,
                private,
                dry_run,
                compression_level,
                r2_bucket=r2_bucket,
                drive_bucket=drive_bucket,
            )
            if result != 0:
                failed.append(str(file_path.relative_to(repo_root)))
            elif entry:
                updates.update(entry)

        if failed:
            print(f"Failed to push {len(failed)} files")
            return 1

        if not dry_run and updates:
            update_index_entries(vlfs_dir, updates)
        return 0

    # Handle single file
    result, entry = _push_single_file_collect(
        repo_root,
        vlfs_dir,
        cache_dir,
        src_path,
        private,
        dry_run,
        compression_level,
        r2_bucket=r2_bucket,
        drive_bucket=drive_bucket,
    )
    if not dry_run and entry:
        update_index_entries(vlfs_dir, entry)
    return result


def cmd_push_all(
    repo_root: Path, vlfs_dir: Path, cache_dir: Path, private: bool, dry_run: bool
) -> int:
    """Push all new or modified files."""
    try:
        index = read_index(vlfs_dir)
    except VLFSIndexError as e:
        print(f"Error: {e}", file=sys.stderr)
        return 1

    # Find modified files
    status = compute_status(index, repo_root)
    files_to_push = status["missing"] + status["modified"]

    if not files_to_push:
        print("All files are up to date")
        return 0

    # Load config for compression level
    config = load_merged_config(vlfs_dir)
    compression_level = config.get("defaults", {}).get("compression_level", 3)
    r2_bucket = config.get("remotes", {}).get("r2", {}).get("bucket", "vlfs")
    drive_bucket = config.get("remotes", {}).get("gdrive", {}).get("bucket", "vlfs")

    print(f"Pushing {len(files_to_push)} files...")

    failed = []
    updates: dict[str, dict[str, Any]] = {}
    for rel_path in files_to_push:
        file_path = repo_root / rel_path.replace("/", os.sep)
        if not file_path.exists():
            continue

        result, entry = _push_single_file_collect(
            repo_root,
            vlfs_dir,
            cache_dir,
            file_path,
            private,
            dry_run,
            compression_level,
            r2_bucket=r2_bucket,
            drive_bucket=drive_bucket,
        )
        if result != 0:
            failed.append(rel_path)
        elif entry:
            updates.update(entry)

    if failed:
        print(f"Failed to push {len(failed)} files")
        return 1

    if not dry_run and updates:
        update_index_entries(vlfs_dir, updates)

    print(f"Successfully pushed {len(files_to_push)} files")
    return 0


def cmd_push_glob(
    repo_root: Path,
    vlfs_dir: Path,
    cache_dir: Path,
    pattern: str,
    private: bool,
    dry_run: bool,
) -> int:
    """Push files matching a glob pattern."""
    matched_files = _collect_glob_matches(repo_root, pattern)

    if not matched_files:
        print(f"No files match pattern: {pattern}")
        return 0

    print(f"Found {len(matched_files)} files matching '{pattern}'")

    # Load config for compression level
    config = load_merged_config(vlfs_dir)
    compression_level = config.get("defaults", {}).get("compression_level", 3)
    r2_bucket = config.get("remotes", {}).get("r2", {}).get("bucket", "vlfs")
    drive_bucket = config.get("remotes", {}).get("gdrive", {}).get("bucket", "vlfs")

    failed = []
    updates: dict[str, dict[str, Any]] = {}
    for file_path in matched_files:
        result, entry = _push_single_file_collect(
            repo_root,
            vlfs_dir,
            cache_dir,
            file_path,
            private,
            dry_run,
            compression_level,
            r2_bucket=r2_bucket,
            drive_bucket=drive_bucket,
        )
        if result != 0:
            failed.append(str(file_path.relative_to(repo_root)))
        elif entry:
            updates.update(entry)

    if failed:
        print(f"Failed to push {len(failed)} files")
        return 1

    if not dry_run and updates:
        update_index_entries(vlfs_dir, updates)

    return 0


def cmd_verify(
    repo_root: Path, vlfs_dir: Path, dry_run: bool = False, json_output: bool = False
) -> int:
    """Execute verify command that re-hashes workspace files."""
    try:
        index = read_index(vlfs_dir)
    except VLFSIndexError as e:
        print(f"Error: {e}", file=sys.stderr)
        return 1

    entries = index.get("entries", {})
    corrupted = []
    missing = []
    valid = []

    to_hash: list[tuple[str, Path, dict[str, Any]]] = []

    for rel_path, entry in entries.items():
        file_path = repo_root / rel_path.replace("/", os.sep)

        if not file_path.exists():
            missing.append(rel_path)
            continue

        # Check size and mtime first (shortcut)
        stat = file_path.stat()
        indexed_size = entry.get("size", 0)
        indexed_mtime = entry.get("mtime", 0)

        # If size and mtime match, assume unchanged
        if stat.st_size == indexed_size and stat.st_mtime == indexed_mtime:
            valid.append(rel_path)
            continue

        to_hash.append((rel_path, file_path, entry))

    if to_hash:
        paths_to_hash = [item[1] for item in to_hash]
        if len(paths_to_hash) >= 8:
            logger.debug("Hashing %d files in parallel", len(paths_to_hash))
            results, errors = hash_files_parallel(paths_to_hash)
        else:
            results = {}
            errors = {}
            for path in paths_to_hash:
                try:
                    results[path] = hash_file(path)
                except (OSError, IOError) as exc:
                    errors[path] = exc

        for rel_path, file_path, entry in to_hash:
            if file_path in errors:
                missing.append(rel_path)
                continue
            current_hash = results[file_path][0]
            if current_hash != entry.get("hash"):
                corrupted.append(rel_path)
            else:
                valid.append(rel_path)

    if json_output:
        result = {
            "valid": valid,
            "corrupted": corrupted,
            "missing": missing,
            "total": len(entries),
            "issues": len(corrupted) + len(missing),
        }
        print(json.dumps(result, indent=2))
    else:
        total = len(entries)
        issues = len(corrupted) + len(missing)

        if issues == 0:
            print(colorize(f"All {total} files verified OK", "GREEN"))
        else:
            print(
                f"Verification: {colorize(f'{total - issues} OK', 'GREEN')}, "
                + f"{colorize(f'{len(corrupted)} corrupted', 'RED')}, "
                + f"{colorize(f'{len(missing)} missing', 'YELLOW')}"
            )

            for path in corrupted[:10]:
                print(f"  {colorize('CORRUPTED', 'RED')} {path}")
            for path in missing[:10]:
                print(f"  {colorize('MISSING', 'YELLOW')} {path}")
            if len(corrupted) + len(missing) > 10:
                print(f"  ... and {len(corrupted) + len(missing) - 10} more")

    return 1 if (corrupted or missing) else 0


def cmd_clean(
    repo_root: Path,
    vlfs_dir: Path,
    cache_dir: Path,
    dry_run: bool = False,
    yes: bool = False,
) -> int:
    """Execute clean command to remove unreferenced cache objects."""
    try:
        index = read_index(vlfs_dir)
    except VLFSIndexError as e:
        print(f"Error: {e}", file=sys.stderr)
        return 1

    # Get all referenced object keys
    entries = index.get("entries", {})
    referenced_keys = set()
    for entry in entries.values():
        object_key = entry.get("object_key")
        if object_key:
            referenced_keys.add(object_key)

    # Scan cache directory for all objects
    objects_dir = cache_dir / "objects"
    if not objects_dir.exists():
        print("Cache directory is empty")
        return 0

    to_delete = []
    total_size = 0

    for obj_path in objects_dir.rglob("*"):
        if obj_path.is_file():
            # Compute relative path from objects dir
            rel_key = str(obj_path.relative_to(objects_dir)).replace(os.sep, "/")
            if rel_key not in referenced_keys:
                to_delete.append(obj_path)
                total_size += obj_path.stat().st_size

    if not to_delete:
        print("No orphaned cache objects found")
        return 0

    if dry_run:
        print(
            f"[DRY-RUN] Would delete {len(to_delete)} orphaned objects ({format_bytes(total_size)})"
        )
        for obj_path in to_delete[:10]:
            print(f"  {obj_path.relative_to(objects_dir)}")
        if len(to_delete) > 10:
            print(f"  ... and {len(to_delete) - 10} more")
        return 0

    # Confirmation prompt
    if not yes:
        print(f"Found {len(to_delete)} orphaned objects ({format_bytes(total_size)})")
        response = input("Delete these files? [y/N] ").strip().lower()
        if response not in ("y", "yes"):
            print("Aborted")
            return 0

    # Delete files
    deleted_count = 0
    freed_bytes = 0
    for obj_path in to_delete:
        try:
            size = obj_path.stat().st_size
            obj_path.unlink()
            deleted_count += 1
            freed_bytes += size
        except (OSError, IOError) as e:
            print(f"Warning: Failed to delete {obj_path}: {e}", file=sys.stderr)

    # Clean up empty directories
    _cleanup_empty_dirs(objects_dir)

    print(f"Deleted {deleted_count} objects, freed {format_bytes(freed_bytes)}")
    return 0


# =============================================================================
# Private Helpers
# =============================================================================


def _push_single_file_collect(
    repo_root: Path,
    vlfs_dir: Path,
    cache_dir: Path,
    src_path: Path,
    private: bool,
    dry_run: bool,
    compression_level: int = 3,
    r2_bucket: str = "vlfs",
    drive_bucket: str = "vlfs",
) -> tuple[int, dict[str, dict[str, Any]] | None]:
    """Push a single file to remote and return index entry update."""
    # Ensure file is within repo
    try:
        rel_path = str(src_path.relative_to(repo_root)).replace(os.sep, "/")
    except ValueError:
        print(f"Error: File must be within repository: {src_path}", file=sys.stderr)
        return 1, None

    logger.info(f"Pushing file: {rel_path}")
    logger.debug(f"Source path: {src_path}")

    # Store in local cache
    object_key = store_object(src_path, cache_dir, compression_level=compression_level)
    logger.debug(f"Stored in cache with key: {object_key}")

    # Compute hash and size
    hex_digest, size, mtime = hash_file(src_path)
    compressed_size = (cache_dir / "objects" / object_key).stat().st_size
    logger.debug(f"Hash: {hex_digest}, Size: {size}, Compressed: {compressed_size}")

    # Determine remote
    remote = "gdrive" if private else "r2"
    logger.debug(f"Target remote: {remote}")

    if dry_run:
        print(f"[DRY-RUN] Would upload {rel_path} to {remote} ({format_bytes(size)})")
        logger.info(f"[DRY-RUN] Would upload {rel_path} to {remote}")
    else:
        # Upload to remote
        try:
            if private:
                # Check Drive token
                if not has_drive_token():
                    logger.error("Google Drive token not found")
                    print("Error: Google Drive token not found.", file=sys.stderr)
                    print("Set up Drive auth with: vlfs auth gdrive", file=sys.stderr)
                    return 1, None

                upload_to_drive(
                    cache_dir / "objects" / object_key,
                    object_key,
                    bucket=drive_bucket,
                    dry_run=False,
                )
                logger.info(f"Uploaded to Drive: {rel_path}")
            else:
                upload_to_r2(
                    cache_dir / "objects" / object_key,
                    object_key,
                    bucket=r2_bucket,
                    dry_run=False,
                )
                logger.info(f"Uploaded to R2: {rel_path}")
            print(
                f"Uploaded: {rel_path} ({format_bytes(size)} -> {format_bytes(compressed_size)})"
            )
        except RcloneError as e:
            print(f"Error uploading {rel_path}: {e}", file=sys.stderr)
            return 1, None
        except ConfigError as e:
            print(f"Error: {e}", file=sys.stderr)
            return 1, None

    entry = {
        rel_path: {
            "hash": hex_digest,
            "size": size,
            "compressed_size": compressed_size,
            "mtime": mtime,
            "object_key": object_key,
            "remote": remote,
        }
    }

    return 0, entry


def _download_remote_group(
    remote: str,
    object_keys: list[str],
    cache_dir: Path,
    key_sizes: dict[str, int],
    r2_public_url: str | None,
    dry_run: bool,
    r2_bucket: str = "vlfs",
    drive_bucket: str = "vlfs",
) -> int:
    """Download missing objects for a remote group and return count."""
    missing = [key for key in object_keys if not (cache_dir / "objects" / key).exists()]
    if not missing:
        return 0

    missing_size = sum(key_sizes.get(k, 0) for k in missing)

    if remote == "r2" and r2_public_url:
        if dry_run:
            print(
                f"[DRY-RUN] Would download {len(missing)} objects ({format_bytes(missing_size)}) via HTTP from {r2_public_url}"
            )
            return len(missing)

        print(
            f"Downloading {len(missing)} objects ({format_bytes(missing_size)}) via HTTP..."
        )
        return download_from_r2_http(missing, cache_dir, r2_public_url, dry_run)

    if dry_run:
        print(
            f"[DRY-RUN] Would download {len(missing)} objects ({format_bytes(missing_size)}) from {remote}"
        )
        return len(missing)

    print(
        f"Downloading {len(missing)} objects ({format_bytes(missing_size)}) from {remote}..."
    )

    if remote == "gdrive":
        try:
            if not has_drive_token():
                print(
                    "Skipping Drive files (no auth). Run: vlfs auth gdrive",
                    file=sys.stderr,
                )
                return 0
        except RuntimeError:
            print(
                "Skipping Drive files (CI environment/no auth).",
                file=sys.stderr,
            )
            return 0

        return download_from_drive(
            missing, cache_dir, bucket=drive_bucket, dry_run=False
        )

    # Default to R2 (rclone)
    if not r2_public_url:
        return download_from_r2(missing, cache_dir, bucket=r2_bucket, dry_run=False)

    # Fallback to HTTP if configured
    return download_from_r2_http(missing, cache_dir, r2_public_url, dry_run)


def _match_recursive_glob(rel_path: str, pattern: str) -> bool:
    """Match a path against a ** glob pattern.

    Examples:
        "tools/compiler.exe" matches "tools/**/*.exe"
        "tools/sub/linker.exe" matches "tools/**/*.exe"
    """
    # Handle patterns like "tools/**/*.exe"
    if "**" not in pattern:
        return fnmatch.fnmatch(rel_path, pattern)

    # Split pattern by **
    parts = pattern.split("**/")
    if len(parts) != 2:
        return False

    prefix = parts[0].rstrip("/")  # e.g., "tools"
    suffix = parts[1]  # e.g., "*.exe"

    # Path must start with prefix
    if prefix and not rel_path.startswith(prefix + "/"):
        return False

    # Path must end with suffix match
    # Get the filename part
    filename = rel_path.split("/")[-1]
    return fnmatch.fnmatch(filename, suffix)


def _collect_glob_matches(repo_root: Path, pattern: str) -> list[Path]:
    """Collect files matching a glob pattern."""
    matched_files = []

    # Normalize pattern separators
    pattern_normalized = pattern.replace("/", os.sep)

    # Support both ** recursive patterns and simple globs
    if "**" in pattern:
        # For recursive patterns like "tools/**/*.exe", we need special handling
        # Split the pattern: prefix = "tools", suffix = "*.exe"
        parts = pattern.split("**/")
        if len(parts) == 2:
            prefix = parts[0].rstrip("/")  # "tools"
            suffix = parts[1]  # "*.exe"

            # Walk the directory tree starting from prefix
            start_dir = repo_root / prefix if prefix else repo_root
            if start_dir.exists():
                for root, dirs, files in os.walk(start_dir):
                    # Skip ignored directories
                    dirs[:] = [
                        d for d in dirs if d not in (".vlfs", ".vlfs-cache", ".git")
                    ]

                    for file in files:
                        if fnmatch.fnmatch(file, suffix):
                            file_path = Path(root) / file
                            matched_files.append(file_path)
        else:
            # Fallback: simple recursive walk with pattern matching
            for root, dirs, files in os.walk(repo_root):
                dirs[:] = [d for d in dirs if d not in (".vlfs", ".vlfs-cache", ".git")]

                for file in files:
                    file_path = Path(root) / file
                    rel_path = file_path.relative_to(repo_root)
                    rel_str = str(rel_path).replace(os.sep, "/")

                    # Convert ** pattern to a simpler check
                    # "tools/**/*.exe" should match "tools/compiler.exe", "tools/sub/linker.exe"
                    if _match_recursive_glob(rel_str, pattern):
                        matched_files.append(file_path)
    else:
        # Simple glob - use glob.glob
        import glob as glob_module

        search_path = repo_root / pattern_normalized
        for file_path in glob_module.glob(str(search_path), recursive=False):
            file_path = Path(file_path)
            if file_path.is_file():
                matched_files.append(file_path)

    return matched_files


def _find_files_recursive(repo_root: Path, directory: Path) -> list[Path]:
    """Find all files recursively, skipping ignored directories."""
    files = []
    ignore_dirs = {".vlfs", ".vlfs-cache", ".git"}

    for root, dirs, filenames in os.walk(directory):
        # Filter out ignored directories
        dirs[:] = [d for d in dirs if d not in ignore_dirs]

        for filename in filenames:
            files.append(Path(root) / filename)

    return files


def _cleanup_empty_dirs(directory: Path) -> None:
    """Remove empty directories recursively."""
    if not directory.exists():
        return

    for root, dirs, files in os.walk(str(directory), topdown=False):
        for dir_name in dirs:
            dir_path = Path(root) / dir_name
            try:
                if dir_path.exists() and not any(dir_path.iterdir()):
                    dir_path.rmdir()
            except (OSError, IOError):
                pass


# =============================================================================
# CLI Entry Point
# =============================================================================


def main(argv: list[str] | None = None) -> int:
    """Main entry point."""
    parser = argparse.ArgumentParser(
        prog="vlfs", description="Vibecoded Large File Storage", exit_on_error=False
    )

    # Global options
    parser.add_argument(
        "-v",
        "--verbose",
        action="count",
        default=0,
        help="Increase verbosity (use -v for DEBUG, -vv for TRACE)",
    )

    subparsers = parser.add_subparsers(dest="command", help="Available commands")

    # auth command
    auth_parser = subparsers.add_parser("auth", help="Authentication commands")
    auth_subparsers = auth_parser.add_subparsers(
        dest="auth_command", help="Auth subcommands"
    )
    auth_gdrive_parser = auth_subparsers.add_parser(
        "gdrive", help="Authenticate with Google Drive"
    )

    # pull command
    pull_parser = subparsers.add_parser("pull", help="Download files from remote")
    pull_parser.add_argument(
        "--force", action="store_true", help="Overwrite locally modified files"
    )
    pull_parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Show what would be done without doing it",
    )

    # push command
    push_parser = subparsers.add_parser("push", help="Upload file(s) to remote")
    push_parser.add_argument(
        "path", nargs="?", help="Path to file or directory to push"
    )
    push_parser.add_argument(
        "--private", action="store_true", help="Upload to private storage (Drive)"
    )
    push_parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Show what would be done without doing it",
    )
    push_parser.add_argument("--glob", help="Push files matching glob pattern")
    push_parser.add_argument(
        "--all", action="store_true", help="Push all new or modified files"
    )

    # status command
    status_parser = subparsers.add_parser("status", help="Show workspace status")
    status_parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Show what would be done without doing it",
    )
    status_parser.add_argument(
        "--json", action="store_true", help="Output in JSON format"
    )
    status_parser.add_argument(
        "--color", action="store_true", help="Force color output"
    )

    # verify command
    verify_parser = subparsers.add_parser(
        "verify", help="Verify workspace files against index"
    )
    verify_parser.add_argument(
        "--json", action="store_true", help="Output in JSON format"
    )
    verify_parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Show what would be done without doing it",
    )

    # clean command
    clean_parser = subparsers.add_parser(
        "clean", help="Remove unreferenced cache objects"
    )
    clean_parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Show what would be deleted without deleting",
    )
    clean_parser.add_argument(
        "--yes", "-y", action="store_true", help="Skip confirmation prompt"
    )

    try:
        args = parser.parse_args(argv)
    except SystemExit as e:
        # argparse calls sys.exit() on --help or errors
        return e.code if isinstance(e.code, int) else 1
    except Exception:
        return 1

    # Setup logging based on verbosity
    setup_logging(verbosity=args.verbose, log_file=True)

    if args.command is None:
        parser.print_help()
        return 0

    # Handle auth command separately (doesn't need repo structure)
    if args.command == "auth":
        if args.auth_command == "gdrive":
            repo_root = Path.cwd()
            vlfs_dir, _ = resolve_paths(repo_root)
            ensure_dirs(vlfs_dir, repo_root / ".vlfs-cache")
            return auth_gdrive(vlfs_dir)
        else:
            auth_parser.print_help()
            return 0

    dry_run = getattr(args, "dry_run", False)
    json_output = getattr(args, "json", False)

    # Resolve paths and ensure structure
    repo_root = Path.cwd()
    vlfs_dir, cache_dir = resolve_paths(repo_root)
    ensure_dirs(vlfs_dir, cache_dir)
    ensure_gitignore(repo_root)

    warn_if_secrets_in_repo(vlfs_dir)

    # Set rclone config path if available
    # Check user dir first, then legacy
    user_config_path = get_user_config_dir() / "rclone.conf"
    legacy_config_path = vlfs_dir / "rclone.conf"

    if user_config_path.exists():
        set_rclone_config_path(user_config_path)
    else:
        set_rclone_config_path(None)

    if args.command == "status":
        return cmd_status(repo_root, vlfs_dir, dry_run, json_output, args.color)
    elif args.command == "verify":
        return cmd_verify(repo_root, vlfs_dir, dry_run, json_output)
    elif args.command == "clean":
        return cmd_clean(repo_root, vlfs_dir, cache_dir, dry_run, args.yes)
    elif args.command == "pull":
        return cmd_pull(
            repo_root, vlfs_dir, cache_dir, getattr(args, "force", False), dry_run
        )
    elif args.command == "push":
        if args.all:
            return cmd_push_all(repo_root, vlfs_dir, cache_dir, args.private, dry_run)
        elif args.glob:
            return cmd_push_glob(
                repo_root, vlfs_dir, cache_dir, args.glob, args.private, dry_run
            )
        elif args.path:
            return cmd_push(
                repo_root, vlfs_dir, cache_dir, args.path, args.private, dry_run
            )
        else:
            print("Error: push requires a path, --glob, or --all", file=sys.stderr)
            return 1

    return 0


if __name__ == "__main__":
    sys.exit(main())

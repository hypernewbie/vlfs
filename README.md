# Vlfs

[![Download vlfs.py](https://img.shields.io/badge/Download-vlfs.py-blue?logo=python)](https://raw.githubusercontent.com/hypernewbie/vlfs/main/vlfs.py)
[![CI](https://github.com/hypernewbie/vlfs/actions/workflows/ci.yml/badge.svg)](https://github.com/hypernewbie/vlfs/actions/workflows/ci.yml)

AI Slop single-script Python script Git LFS alternative. Public files via HTTP, private files via Google Drive.

> Vlfs is vibe coded by AI with only minor review. It's intended to be a throaway script. Do not use.

## Quick Start

```bash
# Clone and pull — no setup required
git clone <repo>
python vlfs.py pull
```

## Architecture

Content-addressable storage: SHA256 hashing, 2-level sharding (`ab/cd/hash`), zstd compression.

```
.vlfs/
    config.toml       # Repo config (public_base_url, compression)
    index.json        # File manifest (committed)
~/.config/vlfs/
    config.toml       # User secrets (Drive OAuth)
    rclone.conf       # Generated rclone config
    gdrive-token.json # OAuth token
.vlfs-cache/
    objects/          # Local cache
```

## Usage

```bash
# Pull (no auth needed for public files)
python vlfs.py pull

# Push to R2 (requires credentials)
python vlfs.py push tools/clang.exe
python vlfs.py push tools/
python vlfs.py push --glob "**/*.dll"
python vlfs.py push --all

# Push to Drive (private)
python vlfs.py push --private assets/art.psd

# Status
python vlfs.py status
python vlfs.py verify
python vlfs.py clean
```

## Configuration

`.vlfs/config.toml` (committed):
```toml
[remotes.r2]
public_base_url = "https://pub-abc123.r2.dev/vlfs"

[defaults]
compression_level = 3
```

## Google Drive Setup

```bash
python vlfs.py auth gdrive
```

This opens a browser for you to authorise access. Done.

## CMake Integration

```cmake
include(VLFSSync.cmake)
set(VLFSSYNC_AUTO ON)  # Auto-pull on configure
```

## How It Works

| Operation | Auth | Method |
|-----------|------|--------|
| `pull` (R2) | None | HTTP GET |
| `pull` (Drive) | Token | rclone |
| `push` (R2) | Env vars | rclone |
| `push --private` | Token | rclone |

## Dependencies

- Python 3.10+
- `zstandard`
- `rclone` (push only)

### Installing rclone

```bash
# Windows
winget install Rclone.Rclone

# macOS
brew install rclone

# Linux
curl https://rclone.org/install.sh | sudo bash
```

## Testing

```bash
pip install -e ".[dev]"
pytest
```

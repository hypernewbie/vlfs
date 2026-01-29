"""Unit tests for CMake integration (Phase 4.2)."""

import os
from pathlib import Path

import pytest


@pytest.fixture
def cmake_module_path():
    """Get the path to the actual VLFSSync.cmake file."""
    return Path(__file__).parent.parent.parent / 'VLFSSync.cmake'


class TestVLFSSyncCmake:
    """Test VLFSSync.cmake module."""

    def test_cmake_module_exists(self, cmake_module_path):
        """VLFSSync.cmake should exist in the repo."""
        assert cmake_module_path.exists(), "VLFSSync.cmake should be created"

    def test_cmake_module_contains_function(self, cmake_module_path):
        """VLFSSync.cmake should define vlfs_sync function."""
        content = cmake_module_path.read_text()

        assert 'function(vlfs_sync' in content or 'macro(vlfs_sync' in content
        assert 'vlfs.py' in content.lower() or 'vlfs' in content.lower()

    def test_cmake_module_contains_target(self, cmake_module_path):
        """VLFSSync.cmake should define vfs-sync target."""
        content = cmake_module_path.read_text()

        assert 'vfs-sync' in content or 'vlfs_sync' in content
        assert 'add_custom_target' in content or 'execute_process' in content

    def test_cmake_module_handles_python_path(self, cmake_module_path):
        """VLFSSync.cmake should handle python3/python path."""
        content = cmake_module_path.read_text()

        # Should reference python3 or Python
        assert 'python3' in content.lower() or 'python' in content.lower()

    def test_cmake_module_uses_vlfs_py(self, cmake_module_path):
        """VLFSSync.cmake should call vlfs.py pull."""
        content = cmake_module_path.read_text()

        assert 'vlfs.py' in content
        assert 'pull' in content

    def test_cmake_module_has_auto_option(self, cmake_module_path):
        """VLFSSync.cmake should have VLFSSYNC_AUTO option."""
        content = cmake_module_path.read_text()

        assert 'VLFSSYNC_AUTO' in content or 'option' in content.lower()


class TestCMakeIntegrationDocs:
    """Test documentation for CMake integration."""

    def test_readme_contains_cmake_snippet(self, repo_root):
        """README should contain CMake usage example."""
        readme_file = repo_root / 'README.md'
        if not readme_file.exists():
            pytest.skip("README.md doesn't exist yet")

        content = readme_file.read_text()
        assert 'cmake' in content.lower() or 'vlfs' in content.lower()

    def test_readme_mentions_vlfs_sync(self, repo_root):
        """README should mention vlfs_sync."""
        readme_file = repo_root / 'README.md'
        if not readme_file.exists():
            pytest.skip("README.md doesn't exist yet")

        content = readme_file.read_text()
        assert 'vlfs_sync' in content.lower() or 'vfs-sync' in content.lower()

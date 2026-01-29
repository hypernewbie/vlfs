"""Unit tests for example repo fixture (Phase 5.3)."""

from pathlib import Path


def test_example_repo_fixture_exists():
    """Example repo fixture should exist with basic files."""
    repo_root = Path(__file__).parent.parent
    fixture_root = repo_root / 'fixtures' / 'example_repo'

    assert fixture_root.exists()
    assert (fixture_root / 'CMakeLists.txt').exists()
    assert (fixture_root / 'main.cpp').exists()
    assert (fixture_root / '.vlfs' / 'index.json').exists()

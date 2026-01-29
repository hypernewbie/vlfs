"""Unit tests for content-addressable storage (Milestone 1.2)."""

import hashlib
from pathlib import Path

import pytest

import vlfs


class TestHashing:
    """Test file hashing functionality."""
    
    def test_empty_file_hash(self, tmp_path):
        """Empty file should produce known SHA256."""
        test_file = tmp_path / 'empty.txt'
        test_file.write_bytes(b'')
        
        hex_digest, size, mtime = vlfs.hash_file(test_file)
        
        expected_hash = hashlib.sha256(b'').hexdigest()
        assert hex_digest == expected_hash
        assert size == 0
    
    def test_small_file_hash(self, tmp_path):
        """Small file hashing should match SHA256."""
        content = b'Hello, VLFS!'
        test_file = tmp_path / 'small.txt'
        test_file.write_bytes(content)
        
        hex_digest, size, mtime = vlfs.hash_file(test_file)
        
        expected_hash = hashlib.sha256(content).hexdigest()
        assert hex_digest == expected_hash
        assert size == len(content)
    
    def test_large_file_hash(self, tmp_path):
        """Large file (multi-MB) hashing should work."""
        content = b'x' * (5 * 1024 * 1024)  # 5MB
        test_file = tmp_path / 'large.bin'
        test_file.write_bytes(content)
        
        hex_digest, size, mtime = vlfs.hash_file(test_file)
        
        expected_hash = hashlib.sha256(content).hexdigest()
        assert hex_digest == expected_hash
        assert size == len(content)
    
    def test_returns_mtime(self, tmp_path):
        """Should return file mtime."""
        test_file = tmp_path / 'file.txt'
        test_file.write_bytes(b'content')
        
        _, _, mtime = vlfs.hash_file(test_file)
        
        assert mtime > 0
        assert abs(mtime - test_file.stat().st_mtime) < 0.01
    
    def test_hash_is_lowercase(self, tmp_path):
        """Hash should be lowercase."""
        test_file = tmp_path / 'file.txt'
        test_file.write_bytes(b'content')
        
        hex_digest, _, _ = vlfs.hash_file(test_file)
        
        assert hex_digest == hex_digest.lower()


class TestSharding:
    """Test path sharding from hex digest."""
    
    def test_sharding_produces_correct_layout(self):
        """Should produce ab/cd/abcdef... layout."""
        hex_digest = 'abcdef1234567890'
        
        result = vlfs.shard_path(hex_digest)
        
        assert result == 'ab/cd/abcdef1234567890'
    
    def test_sharding_normalizes_lowercase(self):
        """Should normalize to lowercase."""
        hex_digest = 'ABCDEF123456'
        
        result = vlfs.shard_path(hex_digest)
        
        assert result == 'ab/cd/abcdef123456'
    
    def test_short_digest(self):
        """Should handle short digests gracefully."""
        hex_digest = 'ab'
        
        result = vlfs.shard_path(hex_digest)
        
        assert result == 'ab'


class TestCompression:
    """Test compression and decompression."""
    
    def test_roundtrip_empty(self):
        """Empty data should roundtrip."""
        original = b''
        
        compressed = vlfs.compress_bytes(original)
        decompressed = vlfs.decompress_bytes(compressed)
        
        assert decompressed == original
    
    def test_roundtrip_small_text(self):
        """Small text should roundtrip."""
        original = b'Hello, World!'
        
        compressed = vlfs.compress_bytes(original)
        decompressed = vlfs.decompress_bytes(compressed)
        
        assert decompressed == original
    
    def test_roundtrip_binary(self):
        """Binary data should roundtrip."""
        original = bytes(range(256))
        
        compressed = vlfs.compress_bytes(original)
        decompressed = vlfs.decompress_bytes(compressed)
        
        assert decompressed == original
    
    def test_roundtrip_large(self):
        """Large data should roundtrip."""
        original = b'x' * (1024 * 1024)  # 1MB
        
        compressed = vlfs.compress_bytes(original)
        decompressed = vlfs.decompress_bytes(compressed)
        
        assert decompressed == original
    
    def test_compression_actually_compresses(self):
        """Compression should reduce size for compressible data."""
        original = b'a' * 10000
        
        compressed = vlfs.compress_bytes(original)
        
        assert len(compressed) < len(original)


class TestCacheStorage:
    """Test cache storage operations."""
    
    def test_store_object_creates_file(self, tmp_path):
        """store_object should create compressed file."""
        cache_dir = tmp_path / 'cache'
        src_file = tmp_path / 'source.txt'
        src_file.write_bytes(b'Hello, World!')
        
        object_key = vlfs.store_object(src_file, cache_dir)
        
        object_path = cache_dir / 'objects' / object_key
        assert object_path.exists()
    
    def test_store_object_deterministic(self, tmp_path):
        """Same content should produce same object key."""
        cache_dir = tmp_path / 'cache'
        src_file1 = tmp_path / 'file1.txt'
        src_file2 = tmp_path / 'file2.txt'
        src_file1.write_bytes(b'content')
        src_file2.write_bytes(b'content')
        
        key1 = vlfs.store_object(src_file1, cache_dir)
        key2 = vlfs.store_object(src_file2, cache_dir)
        
        assert key1 == key2
    
    def test_store_object_idempotent(self, tmp_path):
        """Storing same file twice should not error."""
        cache_dir = tmp_path / 'cache'
        src_file = tmp_path / 'source.txt'
        src_file.write_bytes(b'Hello')
        
        key1 = vlfs.store_object(src_file, cache_dir)
        key2 = vlfs.store_object(src_file, cache_dir)
        
        assert key1 == key2
        # Should still be readable
        data = vlfs.load_object(key1, cache_dir)
        assert data == b'Hello'
    
    def test_load_object_roundtrip(self, tmp_path):
        """Load should return original content."""
        cache_dir = tmp_path / 'cache'
        original = b'Test content for roundtrip'
        src_file = tmp_path / 'source.bin'
        src_file.write_bytes(original)
        
        object_key = vlfs.store_object(src_file, cache_dir)
        loaded = vlfs.load_object(object_key, cache_dir)
        
        assert loaded == original
    
    def test_store_creates_sharded_directories(self, tmp_path):
        """Should create sharded directory structure."""
        cache_dir = tmp_path / 'cache'
        src_file = tmp_path / 'source.txt'
        src_file.write_bytes(b'x')
        
        object_key = vlfs.store_object(src_file, cache_dir)
        
        # Check that intermediate dirs were created
        parts = object_key.split('/')
        assert len(parts) == 3
        assert (cache_dir / 'objects' / parts[0]).is_dir()
        assert (cache_dir / 'objects' / parts[0] / parts[1]).is_dir()

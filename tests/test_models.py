"""
test_models.py

Unit tests for PyCask models.

Tests for the core data structures: KVEntry and KVLocation.

Author: Santosh Golecha
Created: 2025-06-06
"""

import unittest
import time
from pycask.models import KVEntry, KVLocation


class TestKVLocation(unittest.TestCase):
    """Test cases for KVLocation dataclass."""
    
    def test_kvlocation_creation(self):
        """Test creating a KVLocation instance."""
        location = KVLocation(
            file_id=1,
            entry_offset=1024,
            entry_size=56,
            timestamp=1672531200
        )
        
        self.assertEqual(location.file_id, 1)
        self.assertEqual(location.entry_offset, 1024)
        self.assertEqual(location.entry_size, 56)
        self.assertEqual(location.timestamp, 1672531200)
    
    def test_kvlocation_equality(self):
        """Test KVLocation equality comparison."""
        location1 = KVLocation(1, 1024, 56, 1672531200)
        location2 = KVLocation(1, 1024, 56, 1672531200)
        location3 = KVLocation(2, 1024, 56, 1672531200)
        
        self.assertEqual(location1, location2)
        self.assertNotEqual(location1, location3)


class TestKVEntry(unittest.TestCase):
    """Test cases for KVEntry dataclass."""
    
    def test_kventry_creation(self):
        """Test creating a KVEntry instance."""
        entry = KVEntry(
            crc=123456,
            timestamp=1672531200,
            key_size=5,
            value_size=5,
            key="hello",
            value=b"world"
        )
        
        self.assertEqual(entry.crc, 123456)
        self.assertEqual(entry.timestamp, 1672531200)
        self.assertEqual(entry.key_size, 5)
        self.assertEqual(entry.value_size, 5)
        self.assertEqual(entry.key, "hello")
        self.assertEqual(entry.value, b"world")
    
    def test_total_size_ascii(self):
        """Test total_size calculation with ASCII strings."""
        entry = KVEntry(
            crc=0,
            timestamp=1672531200,
            key_size=5,
            value_size=5,
            key="hello",
            value=b"world"
        )
        
        # Header (20 bytes) + key (5 bytes) + value (5 bytes) = 30 bytes
        expected_size = 20 + 5 + 5
        self.assertEqual(entry.total_size(), expected_size)
    
    def test_total_size_unicode(self):
        """Test total_size calculation with Unicode strings."""
        entry = KVEntry(
            crc=0,
            timestamp=1672531200,
            key_size=6,  # "héllo" in UTF-8 is 6 bytes
            value_size=3,
            key="héllo",  # Contains Unicode character
            value=b"foo"
        )
        
        # Header (20 bytes) + key (6 bytes UTF-8) + value (3 bytes) = 29 bytes
        expected_size = 20 + 6 + 3
        self.assertEqual(entry.total_size(), expected_size)
    
    def test_header_size_constant(self):
        """Test that HEADER_SIZE constant is correct."""
        # CRC (4) + timestamp (8) + key_size (4) + value_size (4) = 20 bytes
        self.assertEqual(KVEntry.HEADER_SIZE, 20)
    
    def test_create_class_method(self):
        """Test the create class method."""
        key = "test_key"
        value = b"test_value"
        timestamp = 1672531200
        
        entry = KVEntry.create(key, value, timestamp)
        
        self.assertEqual(entry.crc, 0)  # CRC should be 0 initially
        self.assertEqual(entry.timestamp, timestamp)
        self.assertEqual(entry.key_size, len(key.encode('utf-8')))
        self.assertEqual(entry.value_size, len(value))
        self.assertEqual(entry.key, key)
        self.assertEqual(entry.value, value)
    
    def test_create_default_timestamp(self):
        """Test create method with default timestamp."""
        before_time = int(time.time())
        entry = KVEntry.create("key", b"value")
        after_time = int(time.time())
        
        # Timestamp should be within the test execution time
        self.assertGreaterEqual(entry.timestamp, before_time)
        self.assertLessEqual(entry.timestamp, after_time)
    
    def test_create_unicode_key(self):
        """Test create method with Unicode key."""
        key = "测试"  # Chinese characters
        value = b"test"
        
        entry = KVEntry.create(key, value)
        
        # UTF-8 encoding of "测试" should be 6 bytes
        self.assertEqual(entry.key_size, 6)
        self.assertEqual(entry.value_size, 4)
        self.assertEqual(entry.key, key)
        self.assertEqual(entry.value, value)
    
    def test_is_valid_sizes_true(self):
        """Test is_valid_sizes with correct sizes."""
        key = "hello"
        value = b"world"
        
        entry = KVEntry.create(key, value)
        self.assertTrue(entry.is_valid_sizes())
    
    def test_is_valid_sizes_false_key(self):
        """Test is_valid_sizes with incorrect key size."""
        entry = KVEntry(
            crc=0,
            timestamp=1672531200,
            key_size=10,  # Wrong size
            value_size=5,
            key="hello",  # Actually 5 bytes
            value=b"world"
        )
        
        self.assertFalse(entry.is_valid_sizes())
    
    def test_is_valid_sizes_false_value(self):
        """Test is_valid_sizes with incorrect value size."""
        entry = KVEntry(
            crc=0,
            timestamp=1672531200,
            key_size=5,
            value_size=10,  # Wrong size
            key="hello",
            value=b"world"  # Actually 5 bytes
        )
        
        self.assertFalse(entry.is_valid_sizes())
    
    def test_is_valid_sizes_unicode_key(self):
        """Test is_valid_sizes with Unicode key."""
        key = "café"  # 5 bytes in UTF-8
        value = b"test"
        
        entry = KVEntry.create(key, value)
        self.assertTrue(entry.is_valid_sizes())
        
        # Manually create with wrong size
        wrong_entry = KVEntry(
            crc=0,
            timestamp=1672531200,
            key_size=4,  # Wrong - should be 5 for UTF-8
            value_size=4,
            key=key,
            value=value
        )
        
        self.assertFalse(wrong_entry.is_valid_sizes())
    
    def test_empty_key_value(self):
        """Test with empty key and value."""
        entry = KVEntry.create("", b"")
        
        self.assertEqual(entry.key_size, 0)
        self.assertEqual(entry.value_size, 0)
        self.assertEqual(entry.total_size(), 20)  # Just header
        self.assertTrue(entry.is_valid_sizes())
    
    def test_large_key_value(self):
        """Test with large key and value."""
        large_key = "k" * 1000
        large_value = b"v" * 2000
        
        entry = KVEntry.create(large_key, large_value)
        
        self.assertEqual(entry.key_size, 1000)
        self.assertEqual(entry.value_size, 2000)
        self.assertEqual(entry.total_size(), 20 + 1000 + 2000)
        self.assertTrue(entry.is_valid_sizes())


if __name__ == '__main__':
    unittest.main()
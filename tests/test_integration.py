"""
test_integration.py

Integration tests for PyCask components.

Tests the interaction between KVWriter, KVReader, and models working together.

Author: Santosh Golecha
Created: 2025-06-06
"""

import unittest
import tempfile
import shutil
import os
import time
from pycask.writer import KVWriter
from pycask.reader import KVReader
from pycask.models import KVEntry, KVLocation


class TestWriterReaderIntegration(unittest.TestCase):
    """Integration tests for KVWriter and KVReader working together."""
    
    def setUp(self):
        """Set up test fixtures."""
        self.test_dir = tempfile.mkdtemp()
        self.writer = None
        self.reader = None
    
    def tearDown(self):
        """Clean up test fixtures."""
        if self.writer:
            self.writer.close()
        if self.reader:
            self.reader.close()
        shutil.rmtree(self.test_dir, ignore_errors=True)
    
    def test_write_and_read_single_entry(self):
        """Test writing and reading a single entry."""
        self.writer = KVWriter(self.test_dir)
        self.reader = KVReader(self.test_dir)
        
        # Write entry
        entry = KVEntry.create("hello", b"world")
        location = self.writer.write_entry(entry)
        
        # Read it back
        read_entry = self.reader.read_entry(location)
        
        # Verify content matches
        self.assertEqual(read_entry.key, entry.key)
        self.assertEqual(read_entry.value, entry.value)
        self.assertEqual(read_entry.timestamp, entry.timestamp)
        self.assertNotEqual(read_entry.crc, 0)  # CRC should be calculated
    
    def test_write_and_read_multiple_entries(self):
        """Test writing and reading multiple entries."""
        self.writer = KVWriter(self.test_dir)
        self.reader = KVReader(self.test_dir)
        
        # Test data
        test_entries = [
            ("user:1", b"Alice"),
            ("user:2", b"Bob"),
            ("config", b'{"timeout": 30}'),
            ("large_data", b"x" * 1000),
            ("unicode_key", "café".encode("utf-8")),
            ("", b""),  # Empty key and value
        ]
        
        # Write all entries
        locations = []
        for key, value in test_entries:
            entry = KVEntry.create(key, value)
            location = self.writer.write_entry(entry)
            locations.append((location, key, value))
        
        # Read all entries back
        for location, expected_key, expected_value in locations:
            read_entry = self.reader.read_entry(location)
            self.assertEqual(read_entry.key, expected_key)
            self.assertEqual(read_entry.value, expected_value)
    
    def test_write_read_with_file_rotation(self):
        """Test writing and reading across multiple files."""
        # Use small file size to force rotation
        self.writer = KVWriter(self.test_dir, max_file_size=200)
        self.reader = KVReader(self.test_dir)
        
        # Write entries that will span multiple files
        entries_and_locations = []
        for i in range(10):
            key = f"key_{i}"
            value = b"x" * 50  # Large enough to cause rotation
            entry = KVEntry.create(key, value)
            location = self.writer.write_entry(entry)
            entries_and_locations.append((entry, location))
        
        # Verify entries are in different files
        file_ids = {loc.file_id for _, loc in entries_and_locations}
        self.assertGreater(len(file_ids), 1, "Should have rotated to multiple files")
        
        # Read all entries back
        for original_entry, location in entries_and_locations:
            read_entry = self.reader.read_entry(location)
            self.assertEqual(read_entry.key, original_entry.key)
            self.assertEqual(read_entry.value, original_entry.value)
    
    def test_read_value_optimization(self):
        """Test that read_value returns correct data."""
        self.writer = KVWriter(self.test_dir)
        self.reader = KVReader(self.test_dir)
        
        # Write test entry
        test_value = b"test_value_data"
        entry = KVEntry.create("test_key", test_value)
        location = self.writer.write_entry(entry)
        
        # Read using read_value
        read_value = self.reader.read_value(location)
        
        # Should return exactly the original value
        self.assertEqual(read_value, test_value)
    
    def test_unicode_key_roundtrip(self):
        """Test Unicode key handling through write/read cycle."""
        self.writer = KVWriter(self.test_dir)
        self.reader = KVReader(self.test_dir)
        
        # Test various Unicode strings
        unicode_keys = [
            "café",  # Latin characters with accent
            "测试",   # Chinese characters
            "🚀🎉",   # Emoji
            "Ñoño",   # Spanish characters
            "Здравствуй",  # Cyrillic
        ]
        
        locations = []
        for key in unicode_keys:
            entry = KVEntry.create(key, b"test_data")
            location = self.writer.write_entry(entry)
            locations.append((location, key))
        
        # Read back and verify
        for location, expected_key in locations:
            read_entry = self.reader.read_entry(location)
            self.assertEqual(read_entry.key, expected_key)
    
    def test_large_entry_roundtrip(self):
        """Test large entry handling."""
        self.writer = KVWriter(self.test_dir)
        self.reader = KVReader(self.test_dir)
        
        # Create large entry
        large_key = "k" * 2000
        large_value = b"v" * 10000
        entry = KVEntry.create(large_key, large_value)
        location = self.writer.write_entry(entry)
        
        # Read back
        read_entry = self.reader.read_entry(location)
        
        self.assertEqual(read_entry.key, large_key)
        self.assertEqual(read_entry.value, large_value)
        self.assertEqual(len(read_entry.key), 2000)
        self.assertEqual(len(read_entry.value), 10000)
    
    def test_binary_data_handling(self):
        """Test handling of various binary data."""
        self.writer = KVWriter(self.test_dir)
        self.reader = KVReader(self.test_dir)
        
        # Test different types of binary data
        binary_values = [
            b"",  # Empty
            b"\x00",  # Null byte
            b"\x00\x01\x02\x03\xFF\xFE\xFD",  # Random bytes
            bytes(range(256)),  # All possible byte values
            b"Text with\nnewlines\tand\0nulls",  # Mixed text and control chars
        ]
        
        locations = []
        for i, value in enumerate(binary_values):
            entry = KVEntry.create(f"binary_{i}", value)
            location = self.writer.write_entry(entry)
            locations.append((location, value))
        
        # Read back and verify
        for location, expected_value in locations:
            read_entry = self.reader.read_entry(location)
            self.assertEqual(read_entry.value, expected_value)
    
    def test_timestamp_consistency(self):
        """Test that timestamps are preserved correctly."""
        self.writer = KVWriter(self.test_dir)
        self.reader = KVReader(self.test_dir)
        
        # Create entry with specific timestamp
        specific_timestamp = 1672531200  # 2023-01-01 00:00:00 UTC
        entry = KVEntry.create("test", b"data", specific_timestamp)
        location = self.writer.write_entry(entry)
        
        # Read back
        read_entry = self.reader.read_entry(location)
        
        self.assertEqual(read_entry.timestamp, specific_timestamp)
        self.assertEqual(location.timestamp, specific_timestamp)
    
    def test_crc_integrity_check(self):
        """Test that CRC integrity is maintained."""
        self.writer = KVWriter(self.test_dir)
        self.reader = KVReader(self.test_dir)
        
        # Write entry
        entry = KVEntry.create("integrity_test", b"sensitive_data")
        location = self.writer.write_entry(entry)
        
        # Read back - should succeed with valid CRC
        read_entry = self.reader.read_entry(location)
        self.assertEqual(read_entry.key, "integrity_test")
        self.assertEqual(read_entry.value, b"sensitive_data")
        
        # Verify CRC is non-zero and consistent
        self.assertNotEqual(read_entry.crc, 0)
    
    def test_file_handle_management(self):
        """Test file handle management across operations."""
        # Use small file size for rotation
        self.writer = KVWriter(self.test_dir, max_file_size=100)
        self.reader = KVReader(self.test_dir)
        
        # Write to multiple files
        locations = []
        for i in range(5):
            entry = KVEntry.create(f"key_{i}", b"x" * 30)
            location = self.writer.write_entry(entry)
            locations.append(location)
        
        # Read from different files
        for location in locations:
            value = self.reader.read_value(location)
            self.assertEqual(value, b"x" * 30)
        
        # Check that reader has cached multiple handles
        unique_file_ids = {loc.file_id for loc in locations}
        self.assertEqual(len(self.reader.file_handles), len(unique_file_ids))
    
    def test_concurrent_writer_reader(self):
        """Test reader can read from files while writer is active."""
        self.writer = KVWriter(self.test_dir)
        self.reader = KVReader(self.test_dir)
        
        # Write first entry
        entry1 = KVEntry.create("first", b"data1")
        location1 = self.writer.write_entry(entry1)
        
        # Read first entry while writer is still active
        read_entry1 = self.reader.read_entry(location1)
        self.assertEqual(read_entry1.key, "first")
        self.assertEqual(read_entry1.value, b"data1")
        
        # Write second entry
        entry2 = KVEntry.create("second", b"data2")
        location2 = self.writer.write_entry(entry2)
        
        # Read both entries
        read_entry1_again = self.reader.read_entry(location1)
        read_entry2 = self.reader.read_entry(location2)
        
        self.assertEqual(read_entry1_again.key, "first")
        self.assertEqual(read_entry2.key, "second")
    
    def test_writer_restart_continuation(self):
        """Test that a new writer can continue from existing files."""
        # Write some data with first writer
        self.writer = KVWriter(self.test_dir, max_file_size=1024)
        entry1 = KVEntry.create("key1", b"value1")
        location1 = self.writer.write_entry(entry1)
        
        # Get file size before closing
        file_path = os.path.join(self.test_dir, "data_0.dat")
        initial_size = os.path.getsize(file_path)
        
        # Close first writer
        self.writer.close()
        
        # Create new writer - should continue from existing file
        writer2 = KVWriter(self.test_dir, max_file_size=1024)
        
        # Verify it continues from the right position
        self.assertEqual(writer2.active_file_id, 0)
        self.assertEqual(writer2.current_offset, initial_size)
        
        # Write another entry
        entry2 = KVEntry.create("key2", b"value2")
        location2 = writer2.write_entry(entry2)
        
        # Should be in same file, after first entry
        self.assertEqual(location2.file_id, 0)
        self.assertEqual(location2.entry_offset, initial_size)
        
        # Read both entries
        self.reader = KVReader(self.test_dir)
        read_entry1 = self.reader.read_entry(location1)
        read_entry2 = self.reader.read_entry(location2)
        
        self.assertEqual(read_entry1.key, "key1")
        self.assertEqual(read_entry2.key, "key2")
        
        writer2.close()
    
    def test_stress_write_read_cycle(self):
        """Stress test with many write/read operations."""
        self.writer = KVWriter(self.test_dir, max_file_size=2048)
        self.reader = KVReader(self.test_dir)
        
        # Write many entries
        num_entries = 100
        locations = []
        
        for i in range(num_entries):
            key = f"stress_key_{i:03d}"
            value = f"stress_value_{i:03d}_{'x' * (i % 50)}".encode()
            entry = KVEntry.create(key, value)
            location = self.writer.write_entry(entry)
            locations.append((location, key, value))
        
        # Read all entries back in random order
        import random
        random.shuffle(locations)
        
        for location, expected_key, expected_value in locations:
            read_entry = self.reader.read_entry(location)
            self.assertEqual(read_entry.key, expected_key)
            self.assertEqual(read_entry.value, expected_value)
    
    def test_entry_validation_roundtrip(self):
        """Test that entry validation works after roundtrip."""
        self.writer = KVWriter(self.test_dir)
        self.reader = KVReader(self.test_dir)
        
        # Write entry
        entry = KVEntry.create("validation_test", b"test_data")
        location = self.writer.write_entry(entry)
        
        # Read back
        read_entry = self.reader.read_entry(location)
        
        # Validation should pass
        self.assertTrue(read_entry.is_valid_sizes())
        
        # Total size should match
        self.assertEqual(read_entry.total_size(), location.entry_size)


class TestErrorHandlingIntegration(unittest.TestCase):
    """Integration tests for error handling scenarios."""
    
    def setUp(self):
        """Set up test fixtures."""
        self.test_dir = tempfile.mkdtemp()
    
    def tearDown(self):
        """Clean up test fixtures."""
        shutil.rmtree(self.test_dir, ignore_errors=True)
    
    def test_read_from_invalid_location(self):
        """Test reading from invalid location."""
        reader = KVReader(self.test_dir)
        
        # Try to read from non-existent file
        invalid_location = KVLocation(
            file_id=999,
            entry_offset=0,
            entry_size=20,
            timestamp=int(time.time())
        )
        
        with self.assertRaises(Exception):  # Should raise some error
            reader.read_entry(invalid_location)
        
        reader.close()
    
    def test_context_managers_error_handling(self):
        """Test context managers handle errors properly."""
        # Test writer context manager with error
        try:
            with KVWriter(self.test_dir) as writer:
                entry = KVEntry.create("test", b"data")
                location = writer.write_entry(entry)
                raise ValueError("Simulated error")
        except ValueError:
            pass  # Expected
        
        # Writer should be closed despite error
        self.assertIsNone(writer.active_file_handle)
        
        # Test reader context manager
        try:
            with KVReader(self.test_dir) as reader:
                # Reader should work normally after writer closed
                stats = reader.get_stats()
                self.assertIsInstance(stats, dict)
                raise ValueError("Simulated error")
        except ValueError:
            pass  # Expected
        
        # Reader should be closed
        self.assertEqual(len(reader.file_handles), 0)


if __name__ == '__main__':
    unittest.main()
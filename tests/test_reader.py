"""
test_reader.py

Unit tests for PyCask KVReader.

Tests for the KVReader class including reading entries, error handling, and edge cases.

Author: Santosh Golecha
Created: 2025-06-06
"""

import unittest
import tempfile
import shutil
import os
import struct
import zlib
from unittest.mock import patch, mock_open, MagicMock
from pycask.reader import KVReader
from pycask.writer import KVWriter
from pycask.models import KVEntry, KVLocation
from pycask.exceptions import CorruptedEntryError, ReaderError, StorageError


class TestKVReader(unittest.TestCase):
    """Test cases for KVReader."""
    
    def setUp(self):
        """Set up test fixtures."""
        self.test_dir = tempfile.mkdtemp()
        self.reader = None
        self.writer = None
    
    def tearDown(self):
        """Clean up test fixtures."""
        if self.reader:
            self.reader.close()
        if self.writer:
            self.writer.close()
        shutil.rmtree(self.test_dir, ignore_errors=True)
    
    def _write_test_entry(self, key: str, value: bytes) -> KVLocation:
        """Helper method to write a test entry and return its location."""
        if not self.writer:
            self.writer = KVWriter(self.test_dir)
        
        entry = KVEntry.create(key, value)
        return self.writer.write_entry(entry)
    
    def test_reader_initialization(self):
        """Test KVReader initialization."""
        self.reader = KVReader(self.test_dir)
        
        self.assertEqual(self.reader.data_dir, self.test_dir)
        self.assertEqual(len(self.reader.file_handles), 0)
    
    def test_reader_initialization_nonexistent_directory(self):
        """Test KVReader initialization with non-existent directory."""
        nonexistent_dir = os.path.join(self.test_dir, "nonexistent")
        
        with self.assertRaises(FileNotFoundError) as context:
            self.reader = KVReader(nonexistent_dir)
        
        self.assertIn("Data directory not found", str(context.exception))
    
    def test_read_single_entry(self):
        """Test reading a single entry."""
        # Write test data
        location = self._write_test_entry("hello", b"world")
        
        # Read it back
        self.reader = KVReader(self.test_dir)
        entry = self.reader.read_entry(location)
        
        # Verify content
        self.assertEqual(entry.key, "hello")
        self.assertEqual(entry.value, b"world")
        self.assertEqual(entry.timestamp, location.timestamp)
        self.assertGreater(entry.crc, 0)  # Should have valid CRC
    
    def test_read_value_only(self):
        """Test reading only the value."""
        location = self._write_test_entry("test_key", b"test_value")
        
        self.reader = KVReader(self.test_dir)
        value = self.reader.read_value(location)
        
        self.assertEqual(value, b"test_value")
    
    def test_read_multiple_entries(self):
        """Test reading multiple entries from same file."""
        # Write multiple entries
        locations = []
        test_data = [
            ("key1", b"value1"),
            ("key2", b"value2"),
            ("key3", b"value3")
        ]
        
        for key, value in test_data:
            location = self._write_test_entry(key, value)
            locations.append((location, key, value))
        
        # Read them back
        self.reader = KVReader(self.test_dir)
        
        for location, expected_key, expected_value in locations:
            entry = self.reader.read_entry(location)
            self.assertEqual(entry.key, expected_key)
            self.assertEqual(entry.value, expected_value)
    
    def test_read_unicode_key(self):
        """Test reading entry with Unicode key."""
        unicode_key = "café"
        location = self._write_test_entry(unicode_key, b"coffee")
        
        self.reader = KVReader(self.test_dir)
        entry = self.reader.read_entry(location)
        
        self.assertEqual(entry.key, unicode_key)
        self.assertEqual(entry.value, b"coffee")
    
    def test_read_large_entry(self):
        """Test reading large entry."""
        large_key = "k" * 1000
        large_value = b"v" * 5000
        location = self._write_test_entry(large_key, large_value)
        
        self.reader = KVReader(self.test_dir)
        entry = self.reader.read_entry(location)
        
        self.assertEqual(entry.key, large_key)
        self.assertEqual(entry.value, large_value)
        self.assertEqual(len(entry.key), 1000)
        self.assertEqual(len(entry.value), 5000)
    
    def test_read_empty_key_value(self):
        """Test reading entry with empty key and value."""
        location = self._write_test_entry("", b"")
        
        self.reader = KVReader(self.test_dir)
        entry = self.reader.read_entry(location)
        
        self.assertEqual(entry.key, "")
        self.assertEqual(entry.value, b"")
        self.assertEqual(entry.key_size, 0)
        self.assertEqual(entry.value_size, 0)
    
    def test_file_handle_caching(self):
        """Test that file handles are cached."""
        location = self._write_test_entry("test", b"data")
        
        self.reader = KVReader(self.test_dir)
        
        # First read should create file handle
        self.assertEqual(len(self.reader.file_handles), 0)
        entry1 = self.reader.read_entry(location)
        self.assertEqual(len(self.reader.file_handles), 1)
        
        # Second read should reuse handle
        entry2 = self.reader.read_entry(location)
        self.assertEqual(len(self.reader.file_handles), 1)
        
        # Both reads should return same data
        self.assertEqual(entry1.key, entry2.key)
        self.assertEqual(entry1.value, entry2.value)
    
    def test_multiple_file_handles(self):
        """Test reading from multiple files."""
        # Force file rotation by using small max size
        self.writer = KVWriter(self.test_dir, max_file_size=100)
        
        # Write entries that will go to different files
        location1 = self.writer.write_entry(KVEntry.create("key1", b"x" * 50))
        location2 = self.writer.write_entry(KVEntry.create("key2", b"x" * 50))
        
        # Should be in different files
        self.assertNotEqual(location1.file_id, location2.file_id)
        
        # Read from both files
        self.reader = KVReader(self.test_dir)
        entry1 = self.reader.read_entry(location1)
        entry2 = self.reader.read_entry(location2)
        
        # Should have handles for both files
        self.assertEqual(len(self.reader.file_handles), 2)
        self.assertIn(location1.file_id, self.reader.file_handles)
        self.assertIn(location2.file_id, self.reader.file_handles)
        
        # Verify content
        self.assertEqual(entry1.key, "key1")
        self.assertEqual(entry2.key, "key2")
    
    def test_crc_verification(self):
        """Test CRC verification during read."""
        location = self._write_test_entry("test", b"data")
        
        self.reader = KVReader(self.test_dir)
        entry = self.reader.read_entry(location)
        
        # Entry should be valid (no exception raised)
        self.assertEqual(entry.key, "test")
        self.assertEqual(entry.value, b"data")
    
    def test_corrupted_crc_detection(self):
        """Test detection of corrupted CRC."""
        location = self._write_test_entry("test", b"data")
        
        # Corrupt the CRC in the file
        file_path = os.path.join(self.test_dir, f"data_{location.file_id}.dat")
        with open(file_path, 'r+b') as f:
            f.seek(0)
            f.write(b'\xFF\xFF\xFF\xFF')  # Write invalid CRC
        
        self.reader = KVReader(self.test_dir)
        
        with self.assertRaises(CorruptedEntryError) as context:
            self.reader.read_entry(location)
        
        self.assertIn("CRC check failed", str(context.exception))
    
    def test_incomplete_header_detection(self):
        """Test detection of incomplete header."""
        location = self._write_test_entry("test", b"data")
        
        # Truncate the file to have incomplete header
        file_path = os.path.join(self.test_dir, f"data_{location.file_id}.dat")
        with open(file_path, 'r+b') as f:
            f.truncate(10)  # Less than header size (20 bytes)
        
        # Update location to reflect truncated size
        location.entry_size = 10
        
        self.reader = KVReader(self.test_dir)
        
        with self.assertRaises(CorruptedEntryError) as context:
            self.reader.read_entry(location)
        
        self.assertIn("Incomplete header", str(context.exception))
    
    def test_size_mismatch_detection(self):
        """Test detection of size mismatch."""
        location = self._write_test_entry("test", b"data")
        
        # Modify the location to have wrong size
        wrong_location = KVLocation(
            file_id=location.file_id,
            entry_offset=location.entry_offset,
            entry_size=999,  # Wrong size
            timestamp=location.timestamp
        )
        
        self.reader = KVReader(self.test_dir)
        
        with self.assertRaises(CorruptedEntryError) as context:
            self.reader.read_entry(wrong_location)
        
        self.assertIn("Size mismatch", str(context.exception))
    
    def test_invalid_sizes_detection(self):
        """Test detection of invalid sizes in header."""
        location = self._write_test_entry("test", b"data")
        
        # Corrupt the sizes in header
        file_path = os.path.join(self.test_dir, f"data_{location.file_id}.dat")
        with open(file_path, 'r+b') as f:
            f.seek(12)  # Position of key_size
            f.write(struct.pack('!I', 0xFFFFFFFF))  # Invalid size
        
        self.reader = KVReader(self.test_dir)
        
        with self.assertRaises(CorruptedEntryError) as context:
            self.reader.read_entry(location)
        
        self.assertIn("Size mismatch", str(context.exception))
    
    def test_incomplete_key_data(self):
        """Test detection of incomplete key data."""
        location = self._write_test_entry("test", b"data")
        
        # Truncate file to cut off key data
        file_path = os.path.join(self.test_dir, f"data_{location.file_id}.dat")
        with open(file_path, 'r+b') as f:
            f.truncate(22)  # Header + 2 bytes of key (should be 4)
        
        self.reader = KVReader(self.test_dir)
        
        with self.assertRaises(CorruptedEntryError) as context:
            self.reader.read_entry(location)
        
        self.assertIn("Incomplete key data", str(context.exception))
    
    def test_incomplete_value_data(self):
        """Test detection of incomplete value data."""
        location = self._write_test_entry("test", b"data")
        
        # Truncate file to cut off value data
        file_path = os.path.join(self.test_dir, f"data_{location.file_id}.dat")
        with open(file_path, 'r+b') as f:
            f.truncate(26)  # Header + key + 2 bytes of value (should be 4)
        
        self.reader = KVReader(self.test_dir)
        
        with self.assertRaises(CorruptedEntryError) as context:
            self.reader.read_entry(location)
        
        self.assertIn("Incomplete value data", str(context.exception))
    
    def test_invalid_utf8_key(self):
        """Test detection of invalid UTF-8 key data."""
        location = self._write_test_entry("test", b"data")
        
        # Corrupt the key data with invalid UTF-8
        file_path = os.path.join(self.test_dir, f"data_{location.file_id}.dat")
        with open(file_path, 'r+b') as f:
            f.seek(20)  # Start of key data
            f.write(b'\xFF\xFE\xFD\xFC')  # Invalid UTF-8 sequence
        
        self.reader = KVReader(self.test_dir)
        
        with self.assertRaises(CorruptedEntryError) as context:
            self.reader.read_entry(location)
        
        self.assertIn("Invalid UTF-8 key data", str(context.exception))
    
    def test_nonexistent_file(self):
        """Test reading from non-existent file."""
        location = KVLocation(
            file_id=999,  # Non-existent file
            entry_offset=0,
            entry_size=20,
            timestamp=1672531200
        )
        
        self.reader = KVReader(self.test_dir)
        
        with self.assertRaises(FileNotFoundError) as context:
            self.reader.read_entry(location)
        
        self.assertIn("Data file not found", str(context.exception))
    
    def test_io_error_during_read(self):
        """Test IO error during read operation."""
        location = self._write_test_entry("test", b"data")
        
        self.reader = KVReader(self.test_dir)
        
        # Mock the file handle to raise IO error
        with patch.object(self.reader, '_get_file_handle') as mock_get_handle:
            mock_handle = MagicMock()
            mock_handle.read.side_effect = OSError("Disk error")
            mock_get_handle.return_value = mock_handle
            
            with self.assertRaises(CorruptedEntryError) as context:
                self.reader.read_entry(location)
            
            self.assertIn("IO error reading entry", str(context.exception))
    
    def test_reader_stats(self):
        """Test reader statistics."""
        # Write some test data
        self._write_test_entry("test1", b"data1")
        self._write_test_entry("test2", b"data2")
        
        self.reader = KVReader(self.test_dir)
        
        # Initial stats
        stats = self.reader.get_stats()
        self.assertEqual(stats['open_file_handles'], 0) #because we didn't read any keys yet
        self.assertEqual(stats['data_files_available'], 1)
        
        # Read from file to open handle
        location = KVLocation(0, 0, 30, 1672531200)  # Approximate values
        try:
            self.reader.read_entry(location)
        except CorruptedEntryError:
            pass  # Size might be wrong, but handle should be opened
        
        stats = self.reader.get_stats()
        self.assertEqual(stats['open_file_handles'], 1)
    
    def test_reader_close(self):
        """Test reader close functionality."""
        location = self._write_test_entry("test", b"data")
        
        self.reader = KVReader(self.test_dir)
        self.reader.read_entry(location)
        
        # Should have open handle
        self.assertEqual(len(self.reader.file_handles), 1)
        
        # Close reader
        self.reader.close()
        
        # Handles should be closed and cleared
        self.assertEqual(len(self.reader.file_handles), 0)
    
    def test_context_manager(self):
        """Test reader as context manager."""
        location = self._write_test_entry("test", b"data")
        
        with KVReader(self.test_dir) as reader:
            entry = reader.read_entry(location)
            self.assertEqual(entry.key, "test")
            self.assertEqual(len(reader.file_handles), 1)
        
        # Handles should be closed after context
        self.assertEqual(len(reader.file_handles), 0)
    
    def test_file_path_generation(self):
        """Test file path generation."""
        self.reader = KVReader(self.test_dir)
        
        path0 = self.reader._get_file_path(0)
        path1 = self.reader._get_file_path(1)
        path99 = self.reader._get_file_path(99)
        
        self.assertEqual(path0, os.path.join(self.test_dir, "data_0.dat"))
        self.assertEqual(path1, os.path.join(self.test_dir, "data_1.dat"))
        self.assertEqual(path99, os.path.join(self.test_dir, "data_99.dat"))
    
    def test_verify_crc_method(self):
        """Test the CRC verification method directly."""
        self.reader = KVReader(self.test_dir)
        
        # Create test entry with known CRC
        key = "test"
        value = b"data"
        timestamp = 1672531200
        
        # Calculate expected CRC
        data_for_crc = struct.pack('!QI I', timestamp, len(key.encode('utf-8')), len(value))
        data_for_crc += key.encode('utf-8') + value
        expected_crc = zlib.crc32(data_for_crc) & 0xffffffff
        
        # Create entry with correct CRC
        entry = KVEntry(
            crc=expected_crc,
            timestamp=timestamp,
            key_size=len(key.encode('utf-8')),
            value_size=len(value),
            key=key,
            value=value
        )
        
        # Should verify successfully
        self.assertTrue(self.reader._verify_crc(entry))
        
        # Create entry with wrong CRC
        wrong_entry = KVEntry(
            crc=12345,  # Wrong CRC
            timestamp=timestamp,
            key_size=len(key.encode('utf-8')),
            value_size=len(value),
            key=key,
            value=value
        )
        
        # Should fail verification
        self.assertFalse(self.reader._verify_crc(wrong_entry))


if __name__ == '__main__':
    unittest.main()
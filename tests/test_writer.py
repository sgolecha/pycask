"""
test_writer.py

Unit tests for PyCask KVWriter.

Tests for the KVWriter class including file operations, rotation, and error handling.

Author: Santosh Golecha
Created: 2025-06-06
"""

import unittest
import tempfile
import shutil
import os
import struct
import zlib
from unittest.mock import patch, mock_open
from pycask.writer import KVWriter
from pycask.models import KVEntry, KVLocation
from pycask.exceptions import WriterError, StorageError


class TestKVWriter(unittest.TestCase):
    """Test cases for KVWriter."""
    
    def setUp(self):
        """Set up test fixtures."""
        self.test_dir = tempfile.mkdtemp()
        self.writer = None
    
    def tearDown(self):
        """Clean up test fixtures."""
        if self.writer:
            self.writer.close()
        shutil.rmtree(self.test_dir, ignore_errors=True)
    
    def test_writer_initialization(self):
        """Test KVWriter initialization."""
        self.writer = KVWriter(self.test_dir, max_file_size=1024)
        
        self.assertEqual(self.writer.data_dir, self.test_dir)
        self.assertEqual(self.writer.max_file_size, 1024)
        self.assertEqual(self.writer.active_file_id, 0)
        self.assertEqual(self.writer.current_offset, 0)
        self.assertIsNotNone(self.writer.active_file_handle)
        
        # Check that data directory was created
        self.assertTrue(os.path.exists(self.test_dir))
    
    def test_writer_initialization_creates_directory(self):
        """Test that writer creates data directory if it doesn't exist."""
        new_dir = os.path.join(self.test_dir, "new_subdir")
        self.assertFalse(os.path.exists(new_dir))
        
        self.writer = KVWriter(new_dir)
        self.assertTrue(os.path.exists(new_dir))
    
    def test_write_single_entry(self):
        """Test writing a single entry."""
        self.writer = KVWriter(self.test_dir, max_file_size=1024)
        
        entry = KVEntry.create("hello", b"world")
        location = self.writer.write_entry(entry)
        
        # Verify location
        self.assertEqual(location.file_id, 0)
        self.assertEqual(location.entry_offset, 0)
        self.assertEqual(location.entry_size, entry.total_size())
        self.assertEqual(location.timestamp, entry.timestamp)
        
        # Verify file was created and has content
        file_path = os.path.join(self.test_dir, "data_0.dat")
        self.assertTrue(os.path.exists(file_path))
        
        file_size = os.path.getsize(file_path)
        self.assertEqual(file_size, entry.total_size())
        self.assertEqual(self.writer.current_offset, entry.total_size())
    
    def test_write_multiple_entries(self):
        """Test writing multiple entries to same file."""
        self.writer = KVWriter(self.test_dir, max_file_size=1024)
        
        entries = [
            KVEntry.create("key1", b"value1"),
            KVEntry.create("key2", b"value2"),
            KVEntry.create("key3", b"value3")
        ]
        
        locations = []
        expected_offset = 0
        
        for entry in entries:
            location = self.writer.write_entry(entry)
            locations.append(location)
            
            # Verify location
            self.assertEqual(location.file_id, 0)
            self.assertEqual(location.entry_offset, expected_offset)
            self.assertEqual(location.entry_size, entry.total_size())
            
            expected_offset += entry.total_size()
        
        # Verify final offset
        self.assertEqual(self.writer.current_offset, expected_offset)
    
    def test_file_rotation_on_size_limit(self):
        """Test file rotation when size limit is reached."""
        # Small file size to force rotation
        self.writer = KVWriter(self.test_dir, max_file_size=84)
        
        # Write entries that will exceed the limit
        entry1 = KVEntry.create("key1", b"value1" * 10)  # Large entry 4(key) + 60(value) + 20 bytes header = 84
        entry2 = KVEntry.create("key2", b"value2")
        
        location1 = self.writer.write_entry(entry1)
        location2 = self.writer.write_entry(entry2)
        
        # First entry should be in file 0
        self.assertEqual(location1.file_id, 0)
        self.assertEqual(location1.entry_offset, 0)
        
        # Second entry should trigger rotation to file 1
        self.assertEqual(location2.file_id, 1)
        self.assertEqual(location2.entry_offset, 0)
        self.assertEqual(self.writer.active_file_id, 1)
        
        # Verify both files exist
        file0_path = os.path.join(self.test_dir, "data_0.dat")
        file1_path = os.path.join(self.test_dir, "data_1.dat")
        self.assertTrue(os.path.exists(file0_path))
        self.assertTrue(os.path.exists(file1_path))
    
    def test_crc_calculation(self):
        """Test that CRC is calculated correctly."""
        self.writer = KVWriter(self.test_dir)
        
        entry = KVEntry.create("test", b"data")
        original_crc = entry.crc  # Should be 0
        
        location = self.writer.write_entry(entry)
        
        # Read the written data and verify CRC was calculated
        file_path = os.path.join(self.test_dir, "data_0.dat")
        with open(file_path, 'rb') as f:
            # Read header
            header_data = f.read(20)
            crc, timestamp, key_size, value_size = struct.unpack('!IQI I', header_data)
            
            # CRC should not be 0 (unless by coincidence)
            self.assertNotEqual(crc, original_crc)
            
            # Verify other fields
            self.assertEqual(timestamp, entry.timestamp)
            self.assertEqual(key_size, entry.key_size)
            self.assertEqual(value_size, entry.value_size)
    
    def test_entry_serialization(self):
        """Test entry serialization format."""
        self.writer = KVWriter(self.test_dir)
        
        entry = KVEntry.create("hello", b"world")
        location = self.writer.write_entry(entry)
        
        # Read and verify the serialized data
        file_path = os.path.join(self.test_dir, "data_0.dat")
        with open(file_path, 'rb') as f:
            # Read all data
            data = f.read()
            
            # Parse header
            crc, timestamp, key_size, value_size = struct.unpack('!IQI I', data[:20])
            
            # Parse key and value
            key_data = data[20:20+key_size]
            value_data = data[20+key_size:20+key_size+value_size]
            
            # Verify content
            self.assertEqual(key_data.decode('utf-8'), "hello")
            self.assertEqual(value_data, b"world")
            self.assertEqual(len(data), entry.total_size())
    
    def test_unicode_key_handling(self):
        """Test handling of Unicode keys."""
        self.writer = KVWriter(self.test_dir)
        
        unicode_key = "café"  # Contains non-ASCII character
        entry = KVEntry.create(unicode_key, b"coffee")
        location = self.writer.write_entry(entry)
        
        # Read and verify
        file_path = os.path.join(self.test_dir, "data_0.dat")
        with open(file_path, 'rb') as f:
            data = f.read()
            
            # Parse key
            key_size = struct.unpack('!I', data[16:20])[0]  # value_size field
            key_size = struct.unpack('!I', data[12:16])[0]  # key_size field
            key_data = data[20:20+key_size]
            
            # Verify Unicode key is correctly encoded/decoded
            self.assertEqual(key_data.decode('utf-8'), unicode_key)
            self.assertEqual(key_size, len(unicode_key.encode('utf-8')))
    
    def test_continue_existing_file(self):
        """Test continuing to write to an existing file."""
        # Create a file with some data
        file_path = os.path.join(self.test_dir, "data_0.dat")
        initial_data = b"some existing data"
        with open(file_path, 'wb') as f:
            f.write(initial_data)
        
        # Initialize writer - should continue from existing file
        self.writer = KVWriter(self.test_dir, max_file_size=1024)
        
        self.assertEqual(self.writer.active_file_id, 0)
        self.assertEqual(self.writer.current_offset, len(initial_data))
        
        # Write an entry
        entry = KVEntry.create("new", b"entry")
        location = self.writer.write_entry(entry)
        
        # Should write after existing data
        self.assertEqual(location.entry_offset, len(initial_data))
    
    def test_skip_full_existing_file(self):
        """Test skipping a full existing file and creating new one."""
        # Create a "full" file
        file_path = os.path.join(self.test_dir, "data_0.dat")
        max_size = 100
        with open(file_path, 'wb') as f:
            f.write(b"x" * max_size)  # Fill to max size
        
        # Initialize writer with same max size
        self.writer = KVWriter(self.test_dir, max_file_size=max_size)
        
        # Should create new file since existing one is full
        self.assertEqual(self.writer.active_file_id, 1)
        self.assertEqual(self.writer.current_offset, 0)
    
    def test_writer_stats(self):
        """Test writer statistics."""
        self.writer = KVWriter(self.test_dir, max_file_size=1024)
        
        stats = self.writer.get_stats()
        
        self.assertEqual(stats['active_file_id'], 0)
        self.assertEqual(stats['current_offset'], 0)
        self.assertEqual(stats['max_file_size'], 1024)
        self.assertEqual(stats['bytes_remaining'], 1024)
        self.assertTrue(stats['active_file_handle_open'])
    
    def test_writer_close(self):
        """Test writer close functionality."""
        self.writer = KVWriter(self.test_dir)
        
        # Verify handle is open
        self.assertIsNotNone(self.writer.active_file_handle)
        
        # Close writer
        self.writer.close()
        
        # Verify handle is closed
        self.assertIsNone(self.writer.active_file_handle)
    
    def test_context_manager(self):
        """Test writer as context manager."""
        with KVWriter(self.test_dir) as writer:
            entry = KVEntry.create("test", b"data")
            location = writer.write_entry(entry)
            self.assertIsNotNone(writer.active_file_handle)
        
        # Handle should be closed after context
        self.assertIsNone(writer.active_file_handle)
    
    @patch('builtins.open', side_effect=OSError("Permission denied"))
    def test_writer_initialization_error(self, mock_file_open):
        """Test writer initialization with file permission error."""
        with self.assertRaises(WriterError) as context:
            self.writer = KVWriter(self.test_dir)
        
        self.assertIn("Cannot open active file", str(context.exception))
    
    @patch('os.makedirs', side_effect=OSError("Permission denied"))
    def test_directory_creation_error(self, mock_makedirs):
        """Test directory creation error."""
        with self.assertRaises(StorageError) as context:
            self.writer = KVWriter("/invalid/path")
        
        self.assertIn("Cannot create data directory", str(context.exception))
    
    def test_write_after_close_error(self):
        """Test writing after writer is closed."""
        self.writer = KVWriter(self.test_dir)
        self.writer.close()
        
        entry = KVEntry.create("test", b"data")
        
        with self.assertRaises(WriterError) as context:
            self.writer.write_entry(entry)
        
        self.assertIn("No active file handle available", str(context.exception))
    
    @patch('builtins.open')
    def test_write_io_error(self, mock_open_func):
        """Test IO error during write."""
        # Mock file handle that raises error on write
        mock_file = mock_open_func.return_value
        mock_file.write.side_effect = OSError("Disk full")
        
        self.writer = KVWriter(self.test_dir)
        entry = KVEntry.create("test", b"data")
        
        with self.assertRaises(WriterError) as context:
            self.writer.write_entry(entry)
        
        self.assertIn("Failed to write entry", str(context.exception))
        self.assertEqual(context.exception.operation, "write_entry")
    
    def test_file_rotation_error(self):
        """Test error during file rotation."""
        self.writer = KVWriter(self.test_dir, max_file_size=50)
        
        # Write first entry
        entry1 = KVEntry.create("key1", b"value1" * 10)
        self.writer.write_entry(entry1)
        
        # Mock open to fail on rotation
        with patch('builtins.open', side_effect=OSError("No space left")):
            entry2 = KVEntry.create("key2", b"value2")
            
            with self.assertRaises(WriterError) as context:
                self.writer.write_entry(entry2)
            
            #self.assertIn("Failed to rotate to new file", str(context.exception))
            self.assertIn("No space left", str(context.exception))
            self.assertEqual(context.exception.operation, "open_active_file")
    
    def test_get_existing_file_ids(self):
        """Test getting existing file IDs."""
        # Create some test files
        test_files = ["data_0.dat", "data_5.dat", "data_10.log", "other.txt"]
        for filename in test_files:
            file_path = os.path.join(self.test_dir, filename)
            with open(file_path, 'w') as f:
                f.write("test")
        
        self.writer = KVWriter(self.test_dir)
        file_ids = self.writer._get_existing_file_ids()
        
        # Should only find the data_*.dat files, sorted
        self.assertEqual(file_ids, [0, 5])
    
    def test_get_existing_file_ids_empty_directory(self):
        """Test getting existing file IDs from empty directory."""
        self.writer = KVWriter(self.test_dir)
        file_ids = self.writer._get_existing_file_ids()
        
        # there should be only one file with id:0
        self.assertEqual(file_ids, [0])

    def test_should_rotate_file(self):
        """Test file rotation decision logic."""
        self.writer = KVWriter(self.test_dir, max_file_size=100)
        
        # Small entry should not cause rotation
        small_entry = KVEntry.create("small", b"data")
        self.assertFalse(self.writer._should_rotate_file(small_entry.total_size()))
        
        # Large entry should cause rotation
        large_entry = KVEntry.create("large", b"x" * 200)
        self.assertTrue(self.writer._should_rotate_file(large_entry.total_size()))
        
        # Write small entry and advance offset
        self.writer.write_entry(small_entry)
        
        # Now medium entry should cause rotation
        medium_entry = KVEntry.create("medium", b"x" * 50)
        self.assertTrue(self.writer._should_rotate_file(medium_entry.total_size()))
    
    def test_file_path_generation(self):
        """Test file path generation."""
        self.writer = KVWriter(self.test_dir)
        
        path0 = self.writer._get_file_path(0)
        path1 = self.writer._get_file_path(1)
        path99 = self.writer._get_file_path(99)
        
        self.assertEqual(path0, os.path.join(self.test_dir, "data_0.dat"))
        self.assertEqual(path1, os.path.join(self.test_dir, "data_1.dat"))
        self.assertEqual(path99, os.path.join(self.test_dir, "data_99.dat"))
    
    def test_large_entries(self):
        """Test writing large entries."""
        self.writer = KVWriter(self.test_dir, max_file_size=10*1024)  # 10KB
        
        # Create large entry
        large_key = "k" * 1000
        large_value = b"v" * 5000
        entry = KVEntry.create(large_key, large_value)
        
        location = self.writer.write_entry(entry)
        
        self.assertEqual(location.file_id, 0)
        self.assertEqual(location.entry_offset, 0)
        self.assertEqual(location.entry_size, entry.total_size())
        
        # Verify file size
        file_path = os.path.join(self.test_dir, "data_0.dat")
        file_size = os.path.getsize(file_path)
        self.assertEqual(file_size, entry.total_size())
    
    def test_empty_key_value(self):
        """Test writing entries with empty key and value."""
        self.writer = KVWriter(self.test_dir)
        
        entry = KVEntry.create("", b"")
        location = self.writer.write_entry(entry)
        
        self.assertEqual(location.entry_size, 20)  # Just header
        
        # Verify written data
        file_path = os.path.join(self.test_dir, "data_0.dat")
        with open(file_path, 'rb') as f:
            data = f.read()
            
            # Should be exactly header size
            self.assertEqual(len(data), 20)
            
            # Parse and verify
            crc, timestamp, key_size, value_size = struct.unpack('!IQI I', data)
            self.assertEqual(key_size, 0)
            self.assertEqual(value_size, 0)
    
    def test_default_max_file_size(self):
        """Test default max file size."""
        self.writer = KVWriter(self.test_dir)
        
        # Default should be 1GB
        expected_default = 1024 * 1024 * 1024
        self.assertEqual(self.writer.max_file_size, expected_default)
    
    def test_crc_verification_roundtrip(self):
        """Test that written CRC can be verified by reader logic."""
        self.writer = KVWriter(self.test_dir)
        
        entry = KVEntry.create("test_key", b"test_value")
        location = self.writer.write_entry(entry)
        
        # Read back the data and verify CRC
        file_path = os.path.join(self.test_dir, "data_0.dat")
        with open(file_path, 'rb') as f:
            # Read header
            header_data = f.read(20)
            crc, timestamp, key_size, value_size = struct.unpack('!IQI I', header_data)
            
            # Read key and value
            key_data = f.read(key_size)
            value_data = f.read(value_size)
            
            # Recreate CRC calculation
            data_for_crc = struct.pack('!QI I', timestamp, key_size, value_size)
            data_for_crc += key_data + value_data
            calculated_crc = zlib.crc32(data_for_crc) & 0xffffffff
            
            # CRC should match
            self.assertEqual(crc, calculated_crc)


if __name__ == '__main__':
    unittest.main()
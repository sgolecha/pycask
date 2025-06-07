"""
test_exceptions.py

Unit tests for PyCask exceptions.

Tests for all custom exception classes and their behavior.

Author: Santosh Golecha
Created: 2025-06-06
"""

import unittest
from pycask.exceptions import (
    PyCaskError,
    KeyNotFoundError,
    CorruptedEntryError,
    WriterError,
    ReaderError
)


class TestPyCaskError(unittest.TestCase):
    """Test cases for base PyCaskError."""
    
    def test_pycask_error_creation(self):
        """Test creating PyCaskError."""
        error = PyCaskError("Test error")
        self.assertEqual(str(error), "Test error")
        self.assertIsInstance(error, Exception)
    
    def test_inheritance_hierarchy(self):
        """Test that all custom exceptions inherit from PyCaskError."""
        exceptions = [
            KeyNotFoundError("test"),
            CorruptedEntryError("test"),
            WriterError("test"),
            ReaderError("test")
        ]
        
        for exc in exceptions:
            self.assertIsInstance(exc, PyCaskError)


class TestKeyNotFoundError(unittest.TestCase):
    """Test cases for KeyNotFoundError."""
    
    def test_key_not_found_error_creation(self):
        """Test creating KeyNotFoundError with key."""
        error = KeyNotFoundError("missing_key")
        
        self.assertEqual(error.key, "missing_key")
        self.assertIsInstance(error, KeyError)
        self.assertIsInstance(error, PyCaskError)
    
    def test_key_not_found_error_custom_message(self):
        """Test KeyNotFoundError with custom message."""
        error = KeyNotFoundError("missing_key", "Custom message")
        
        self.assertEqual(error.key, "missing_key")
        self.assertEqual(str(error), "'Custom message'")
    
    def test_key_not_found_error_inheritance(self):
        """Test that KeyNotFoundError inherits from both KeyError and PyCaskError."""
        error = KeyNotFoundError("test")
        
        self.assertIsInstance(error, KeyError)
        self.assertIsInstance(error, PyCaskError)
        self.assertIsInstance(error, Exception)


class TestCorruptedEntryError(unittest.TestCase):
    """Test cases for CorruptedEntryError."""
    
    def test_corrupted_entry_error_basic(self):
        """Test basic CorruptedEntryError creation."""
        error = CorruptedEntryError("Data is corrupted")
        
        self.assertEqual(str(error), "Data is corrupted")
        self.assertIsNone(error.location)
        self.assertIsNone(error.details)
    
    def test_corrupted_entry_error_with_location(self):
        """Test CorruptedEntryError with location."""
        error = CorruptedEntryError("Data is corrupted", location="file_1:offset_1024")
        
        self.assertEqual(error.location, "file_1:offset_1024")
        self.assertEqual(str(error), "Data is corrupted (location: file_1:offset_1024)")
    
    def test_corrupted_entry_error_with_details(self):
        """Test CorruptedEntryError with details."""
        error = CorruptedEntryError("CRC mismatch", details="Expected 123, got 456")
        
        self.assertEqual(error.details, "Expected 123, got 456")
        self.assertEqual(str(error), "CRC mismatch - Expected 123, got 456")
    
    def test_corrupted_entry_error_full(self):
        """Test CorruptedEntryError with all parameters."""
        error = CorruptedEntryError(
            "CRC mismatch",
            location="file_2:offset_2048",
            details="Expected 123, got 456"
        )
        
        expected_message = "CRC mismatch (location: file_2:offset_2048) - Expected 123, got 456"
        self.assertEqual(str(error), expected_message)
        self.assertEqual(error.location, "file_2:offset_2048")
        self.assertEqual(error.details, "Expected 123, got 456")


class TestWriterError(unittest.TestCase):
    """Test cases for WriterError."""
    
    def test_writer_error_basic(self):
        """Test basic WriterError creation."""
        error = WriterError("Write failed")
        
        self.assertEqual(str(error), "Write failed")
        self.assertIsNone(error.operation)
        self.assertIsNone(error.file_path)
    
    def test_writer_error_with_operation(self):
        """Test WriterError with operation."""
        error = WriterError("Disk full", operation="write_entry")
        
        self.assertEqual(error.operation, "write_entry")
        self.assertEqual(str(error), "write_entry: Disk full")
    
    def test_writer_error_with_file_path(self):
        """Test WriterError with file path."""
        error = WriterError("Permission denied", file_path="/data/file_1.log")
        
        self.assertEqual(error.file_path, "/data/file_1.log")
        self.assertEqual(str(error), "Permission denied (file: /data/file_1.log)")
    
    def test_writer_error_full(self):
        """Test WriterError with all parameters."""
        error = WriterError(
            "IO error",
            operation="file_rotation",
            file_path="/data/file_2.log"
        )
        
        expected_message = "file_rotation: IO error (file: /data/file_2.log)"
        self.assertEqual(str(error), expected_message)
        self.assertEqual(error.operation, "file_rotation")
        self.assertEqual(error.file_path, "/data/file_2.log")


class TestReaderError(unittest.TestCase):
    """Test cases for ReaderError."""
    
    def test_reader_error_basic(self):
        """Test basic ReaderError creation."""
        error = ReaderError("Read failed")
        
        self.assertEqual(str(error), "Read failed")
        self.assertIsNone(error.file_id)
        self.assertIsNone(error.offset)
    
    def test_reader_error_with_file_id(self):
        """Test ReaderError with file_id."""
        error = ReaderError("File not found", file_id=1)
        
        self.assertEqual(error.file_id, 1)
        self.assertEqual(str(error), "File not found (file_id: 1)")
    
    def test_reader_error_with_offset(self):
        """Test ReaderError with offset only."""
        error = ReaderError("Seek failed", offset=1024)
        
        self.assertEqual(error.offset, 1024)
        self.assertEqual(str(error), "Seek failed (offset: 1024)")
    
    def test_reader_error_with_file_id_and_offset(self):
        """Test ReaderError with both file_id and offset."""
        error = ReaderError("Read error", file_id=2, offset=2048)
        
        self.assertEqual(error.file_id, 2)
        self.assertEqual(error.offset, 2048)
        self.assertEqual(str(error), "Read error (file_id: 2, offset: 2048)")
    
    def test_reader_error_with_zero_values(self):
        """Test ReaderError with zero file_id and offset."""
        error = ReaderError("Error at start", file_id=0, offset=0)
        
        # Zero values should still be included
        self.assertEqual(str(error), "Error at start (file_id: 0, offset: 0)")


class TestExceptionCatching(unittest.TestCase):
    """Test exception catching behavior."""
    
    def test_catch_all_pycask_errors(self):
        """Test that all PyCask exceptions can be caught with PyCaskError."""
        exceptions = [
            KeyNotFoundError("test"),
            CorruptedEntryError("test"),
            WriterError("test"),
            ReaderError("test")
        ]
        
        for exc in exceptions:
            with self.assertRaises(PyCaskError):
                raise exc
    
    def test_catch_specific_exceptions(self):
        """Test catching specific exception types."""
        # Test KeyError compatibility
        with self.assertRaises(KeyError):
            raise KeyNotFoundError("test")
        
        # Test specific PyCask exceptions
        with self.assertRaises(CorruptedEntryError):
            raise CorruptedEntryError("test")
        
        with self.assertRaises(WriterError):
            raise WriterError("test")
        
        with self.assertRaises(ReaderError):
            raise ReaderError("test")

if __name__ == '__main__':
    unittest.main()
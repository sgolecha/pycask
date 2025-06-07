"""
writer.py

KV Writer for PyCask key-value store.

This file implements the KVWriter class which handles writing key-value entries
to data files on disk. It manages active file rotation when size limits are
reached and ensures data integrity through CRC checksums.

Classes:
    KVWriter: Handles writing entries to data files with automatic rotation

Author: Santosh Golecha  
Created: 2025-06-06
"""

import os
import struct
import zlib
from typing import Optional, IO
from .models import KVEntry, KVLocation
from .exceptions import WriterError, StorageError


class KVWriter:
    """
    Handles writing key-value entries to data files.
    
    The KVWriter manages the active data file and automatically rotates to
    new files when size limits are reached. It calculates CRC checksums for
    data integrity and returns location information for each written entry.
    
    Attributes:
        data_dir (str): Directory containing data files
        max_file_size (int): Maximum size per data file in bytes
        active_file_id (int): Current active file identifier
        active_file_handle (IO): File handle for the active data file
        current_offset (int): Current write position in active file
        
    Example:
        >>> writer = KVWriter("./data", max_file_size=1024*1024)  # 1MB files
        >>> entry = KVEntry.create("hello", b"world")
        >>> location = writer.write_entry(entry)
        >>> print(f"Written to file {location.file_id} at offset {location.entry_offset}")
    """
    
    def __init__(self, data_dir: str, max_file_size: int = 1024 * 1024 * 1024):
        """
        Initialize the KVWriter.
        
        Args:
            data_dir (str): Directory for data files (created if doesn't exist)
            max_file_size (int): Maximum size per file in bytes (default: 1GB)
            
        Raises:
            StorageError: If data directory cannot be created or accessed
            WriterError: If initial active file cannot be created
        """
        self.data_dir = data_dir
        self.max_file_size = max_file_size
        self.active_file_id = 0
        self.active_file_handle: Optional[IO] = None
        self.current_offset = 0
        
        # Ensure data directory exists
        self._ensure_data_directory()
        
        # Find the next available file ID and initialize active file
        self._initialize_active_file()
    
    def write_entry(self, entry: KVEntry) -> KVLocation:
        """
        Write a key-value entry to the active data file.
        
        This method calculates the CRC checksum, writes the entry to disk,
        and returns the location where it was stored. If the write would
        exceed the file size limit, it automatically rotates to a new file.
        
        Args:
            entry (KVEntry): The entry to write (CRC will be calculated)
            
        Returns:
            KVLocation: Location where the entry was written
            
        Raises:
            WriterError: If writing fails due to IO errors or disk space
            
        Example:
            >>> entry = KVEntry.create("user:123", b"John Doe")
            >>> location = writer.write_entry(entry)
        """
        if not self.active_file_handle:
            raise WriterError("No active file handle available")
        
        # Calculate entry size and check if rotation is needed
        entry_size = entry.total_size()
        if self._should_rotate_file(entry_size):
            self._rotate_to_new_file()
        
        # Calculate CRC32 checksum
        entry_with_crc = self._calculate_crc(entry)
        
        # Remember the write position
        write_offset = self.current_offset
        
        try:
            # Serialize and write the entry
            serialized_data = self._serialize_entry(entry_with_crc)
            self.active_file_handle.write(serialized_data)
            self.active_file_handle.flush()  # Ensure data is written to disk
            
            # Update current offset
            self.current_offset += len(serialized_data)
            
            # Create and return location
            return KVLocation(
                file_id=self.active_file_id,
                entry_offset=write_offset,
                entry_size=entry_size,
                timestamp=entry.timestamp
            )
            
        except (OSError, IOError) as e:
            raise WriterError(
                f"Failed to write entry: {e}",
                operation="write_entry",
                file_path=self._get_file_path(self.active_file_id)
            )
    
    def _calculate_crc(self, entry: KVEntry) -> KVEntry:
        """
        Calculate CRC32 checksum for an entry.
        
        The CRC is calculated over: timestamp + key_size + value_size + key + value
        
        Args:
            entry (KVEntry): Entry with CRC set to 0
            
        Returns:
            KVEntry: New entry with calculated CRC
        """
        # Pack the data for CRC calculation (excluding CRC field itself)
        data_for_crc = struct.pack('!QI I', entry.timestamp, entry.key_size, entry.value_size)
        data_for_crc += entry.key.encode('utf-8')
        data_for_crc += entry.value
        
        # Calculate CRC32
        crc = zlib.crc32(data_for_crc) & 0xffffffff
        
        # Return new entry with calculated CRC
        return KVEntry(
            crc=crc,
            timestamp=entry.timestamp,
            key_size=entry.key_size,
            value_size=entry.value_size,
            key=entry.key,
            value=entry.value
        )
    
    def _serialize_entry(self, entry: KVEntry) -> bytes:
        """
        Serialize an entry to bytes for writing to disk.
        
        Format: [CRC32][Timestamp][KeySize][ValueSize][Key][Value]
        All integer fields are in network byte order.
        
        Args:
            entry (KVEntry): Entry to serialize
            
        Returns:
            bytes: Serialized entry data
        """
        # Pack header fields in network byte order
        header = struct.pack('!IQI I', entry.crc, entry.timestamp, entry.key_size, entry.value_size)
        
        # Encode key and append value
        key_data = entry.key.encode('utf-8')
        
        return header + key_data + entry.value
    
    def _should_rotate_file(self, entry_size: int) -> bool:
        """
        Check if the active file should be rotated before writing an entry.
        
        Args:
            entry_size (int): Size of the entry to be written
            
        Returns:
            bool: True if file should be rotated
        """
        return self.current_offset + entry_size > self.max_file_size
    
    def _rotate_to_new_file(self) -> None:
        """
        Rotate to a new active data file.
        
        Closes the current active file and creates a new one with
        the next available file ID.
        
        Raises:
            WriterError: If file rotation fails
        """
        try:
            # Close current active file
            if self.active_file_handle:
                self.active_file_handle.close()
            
            # Move to next file ID
            self.active_file_id += 1
            self.current_offset = 0
            
            # Create new active file
            self._open_active_file()
            
        except (OSError, IOError) as e:
            raise WriterError(
                f"Failed to rotate to new file: {e}",
                operation="file_rotation",
                file_path=self._get_file_path(self.active_file_id)
            )
    
    def _ensure_data_directory(self) -> None:
        """
        Ensure the data directory exists.
        
        Raises:
            StorageError: If directory cannot be created
        """
        try:
            os.makedirs(self.data_dir, exist_ok=True)
        except OSError as e:
            raise StorageError(f"Cannot create data directory {self.data_dir}: {e}")
    
    def _initialize_active_file(self) -> None:
        """
        Initialize the active file by finding the next available file ID.
        
        Scans existing data files to determine the next file ID and
        opens it for writing.
        
        Raises:
            WriterError: If active file cannot be initialized
        """
        # Find the highest existing file ID
        existing_files = self._get_existing_file_ids()
        if existing_files:
            # Continue from the last file if it's not full, otherwise create new
            last_file_id = max(existing_files)
            last_file_path = self._get_file_path(last_file_id)
            
            try:
                file_size = os.path.getsize(last_file_path)
                if file_size < self.max_file_size:
                    # Continue writing to the last file
                    self.active_file_id = last_file_id
                    self.current_offset = file_size
                else:
                    # Last file is full, create new one
                    self.active_file_id = last_file_id + 1
                    self.current_offset = 0
            except OSError:
                # If we can't get file size, create new file
                self.active_file_id = last_file_id + 1
                self.current_offset = 0
        else:
            # No existing files, start with file ID 0
            self.active_file_id = 0
            self.current_offset = 0
        
        # Open the active file
        self._open_active_file()
    
    def _open_active_file(self) -> None:
        """
        Open the active data file for writing.
        
        Raises:
            WriterError: If file cannot be opened
        """
        file_path = self._get_file_path(self.active_file_id)
        
        try:
            # Open in append mode to continue writing if file exists
            self.active_file_handle = open(file_path, 'ab')
        except (OSError, IOError) as e:
            raise WriterError(
                f"Cannot open active file: {e}",
                operation="open_active_file",
                file_path=file_path
            )
    
    def _get_existing_file_ids(self) -> list:
        """
        Get list of existing data file IDs.
        
        Returns:
            list: Sorted list of existing file IDs
        """
        file_ids = []
        
        try:
            for filename in os.listdir(self.data_dir):
                if filename.startswith('data_') and filename.endswith('.dat'):
                    # Extract file ID from filename like "data_123.dat"
                    file_id_str = filename[5:-4]  # Remove "data_" and ".dat"
                    try:
                        file_ids.append(int(file_id_str))
                    except ValueError:
                        continue  # Skip malformed filenames
        except OSError:
            pass  # Directory might be empty or inaccessible
        
        return sorted(file_ids)
    
    def _get_file_path(self, file_id: int) -> str:
        """
        Get the file path for a given file_id.
        
        Args:
            file_id (int): The numeric file identifier
            
        Returns:
            str: Full path to the data file
        """
        return os.path.join(self.data_dir, f"data_{file_id}.dat")
    
    def close(self) -> None:
        """
        Close the active file handle.
        
        This should be called when the writer is no longer needed
        to ensure data is properly flushed and file handles are released.
        """
        if self.active_file_handle:
            try:
                self.active_file_handle.close()
            except (OSError, IOError):
                pass  # Ignore errors when closing
            finally:
                self.active_file_handle = None
    
    def __enter__(self):
        """Context manager entry."""
        return self
    
    def __exit__(self, exc_type, exc_val, exc_tb):
        """Context manager exit - ensures file handle is closed."""
        self.close()
    
    def get_stats(self) -> dict:
        """
        Get statistics about the writer state.
        
        Returns:
            dict: Statistics including active file info and write position
        """
        return {
            'active_file_id': self.active_file_id,
            'current_offset': self.current_offset,
            'max_file_size': self.max_file_size,
            'bytes_remaining': self.max_file_size - self.current_offset,
            'active_file_handle_open': self.active_file_handle is not None
        }
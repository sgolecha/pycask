"""
reader.py

KV Reader for PyCask key-value store.

This file provides the KVReader class which handles reading key-value entries
from data files on disk. It manages file handles efficiently and parses the
binary format back into KVEntry objects.

Classes:
    KVReader: Handles reading entries from multiple data files

Author: Santosh Golecha 
Created: 2025-06-06
"""

import os
import struct
import zlib
from typing import Dict, IO
from .models import KVEntry, KVLocation
from .exceptions import CorruptedEntryError, FileNotFoundError


class KVReader:
    """
    Handles reading key-value entries from data files.
    
    The KVReader manages file handles for multiple data files and provides
    methods to read and parse entries based on their location information (i.e. KVLocation).
    It maintains a cache of open file handles for performance.
    
    Attributes:
        data_dir (str): Directory containing data files
        file_handles (Dict[int, IO]): Cache of open file handles by file_id
        
    Example:
        >>> reader = KVReader("./data")
        >>> location = KVLocation(file_id=1, entry_offset=1024, entry_size=56, timestamp=1672531200)
        >>> value = reader.read_value(location)
        >>> entry = reader.read_entry(location)
    """
    
    def __init__(self, data_dir: str):
        """
        Initialize the KVReader.
        
        Args:
            data_dir (str): Directory containing the data files
            
        Raises:
            OSError: If data_dir doesn't exist or isn't accessible
        """
        self.data_dir = data_dir
        self.file_handles: Dict[int, IO] = {}
        
        # Ensure data directory exists
        if not os.path.exists(data_dir):
            raise FileNotFoundError(f"Data directory not found: {data_dir}")
    
    def read_value(self, location: KVLocation) -> bytes:
        """
        Read just the value from the specified location.
        
        This is an optimized method that reads only the value portion
        of an entry without parsing the entire record.
        
        Args:
            location (KVLocation): Location of the entry to read
            
        Returns:
            bytes: The value as raw bytes
            
        Raises:
            CorruptedEntryError: If the entry is corrupted or invalid
            FileNotFoundError: If the data file doesn't exist
        """
        entry = self.read_entry(location)
        return entry.value
    
    def read_entry(self, location: KVLocation) -> KVEntry:
        """
        Read and parse a complete entry from the specified location.
        
        Args:
            location (KVLocation): Location of the entry to read
            
        Returns:
            KVEntry: The parsed key-value entry
            
        Raises:
            CorruptedEntryError: If the entry is corrupted or CRC check fails
            FileNotFoundError: If the data file doesn't exist
        """
        file_handle = self._get_file_handle(location.file_id)
        
        try:
            # Seek to the entry position
            file_handle.seek(location.entry_offset)
            
            # Read the header (CRC + timestamp + key_size + value_size)
            header_data = file_handle.read(KVEntry.HEADER_SIZE)
            if len(header_data) != KVEntry.HEADER_SIZE:
                raise CorruptedEntryError(f"Incomplete header at offset {location.entry_offset}")
            
            # Unpack header fields (network byte order)
            crc, timestamp, key_size, value_size = struct.unpack('!IQI I', header_data)
            
            # Validate sizes
            if key_size < 0 or value_size < 0:
                raise CorruptedEntryError(f"Invalid sizes: key={key_size}, value={value_size}")
            
            # Calculate expected total size and validate against location
            expected_size = KVEntry.HEADER_SIZE + key_size + value_size
            if expected_size != location.entry_size:
                raise CorruptedEntryError(
                    f"Size mismatch: expected {location.entry_size}, calculated {expected_size}"
                )
            
            # Read key and value data
            key_data = file_handle.read(key_size)
            if len(key_data) != key_size:
                raise CorruptedEntryError(f"Incomplete key data: expected {key_size} bytes")
            
            value_data = file_handle.read(value_size)
            if len(value_data) != value_size:
                raise CorruptedEntryError(f"Incomplete value data: expected {value_size} bytes")
            
            # Decode key from UTF-8
            try:
                key = key_data.decode('utf-8')
            except UnicodeDecodeError as e:
                raise CorruptedEntryError(f"Invalid UTF-8 key data: {e}")
            
            # Create entry object
            entry = KVEntry(
                crc=crc,
                timestamp=timestamp,
                key_size=key_size,
                value_size=value_size,
                key=key,
                value=value_data
            )
            
            # Verify CRC
            if not self._verify_crc(entry):
                raise CorruptedEntryError(f"CRC check failed for entry at offset {location.entry_offset}")
            
            return entry
            
        except (OSError, IOError) as e:
            raise CorruptedEntryError(f"IO error reading entry: {e}")
    
    def _get_file_handle(self, file_id: int) -> IO:
        """
        Get or create a file handle for the specified file_id.
        
        Args:
            file_id (int): The numeric file identifier
            
        Returns:
            IO: File handle opened in binary read mode
            
        Raises:
            FileNotFoundError: If the data file doesn't exist
        """
        if file_id not in self.file_handles:
            file_path = self._get_file_path(file_id)
            
            if not os.path.exists(file_path):
                raise FileNotFoundError(f"Data file not found: {file_path}")
            
            try:
                self.file_handles[file_id] = open(file_path, 'rb')
            except (OSError, IOError) as e:
                raise FileNotFoundError(f"Cannot open data file {file_path}: {e}")
        
        return self.file_handles[file_id]
    
    def _get_file_path(self, file_id: int) -> str:
        """
        Get the file path for a given file_id.
        
        Args:
            file_id (int): The numeric file identifier
            
        Returns:
            str: Full path to the data file
        """
        return os.path.join(self.data_dir, f"data_{file_id}.dat")
    
    def _verify_crc(self, entry: KVEntry) -> bool:
        """
        Verify the CRC32 checksum of an entry.
        
        The CRC is calculated over: timestamp + key_size + value_size + key + value
        
        Args:
            entry (KVEntry): The entry to verify
            
        Returns:
            bool: True if CRC is valid, False otherwise
        """
        # Pack the data that was used to calculate the CRC
        data_to_check = struct.pack('!QI I', entry.timestamp, entry.key_size, entry.value_size)
        data_to_check += entry.key.encode('utf-8')
        data_to_check += entry.value
        
        # Calculate CRC32
        calculated_crc = zlib.crc32(data_to_check) & 0xffffffff
        
        return calculated_crc == entry.crc
    
    def close(self) -> None:
        """
        Close all open file handles.
        
        This should be called when the reader is no longer needed
        to free system resources.
        """
        for handle in self.file_handles.values():
            try:
                handle.close()
            except (OSError, IOError):
                pass  # Ignore errors when closing
        
        self.file_handles.clear()
    
    def __enter__(self):
        """Context manager entry."""
        return self
    
    def __exit__(self, exc_type, exc_val, exc_tb):
        """Context manager exit - ensures file handles are closed."""
        self.close()
    
    def get_stats(self) -> Dict[str, int]:
        """
        Get statistics about the reader state.
        
        Returns:
            Dict[str, int]: Statistics including number of open file handles
        """
        return {
            'open_file_handles': len(self.file_handles),
            'data_files_available': len([f for f in os.listdir(self.data_dir) 
                                       if f.startswith('data_') and f.endswith('.dat')])
        }
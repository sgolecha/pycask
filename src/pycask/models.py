"""
models.py

Data models for PyCask key-value store.

This file contains the core data structures used throughout the PyCask
implementation, including the on-disk record format and location tracking.

Classes:
    KVEntry: Represents a key-value record as stored on disk
    KVLocation: Tracks the location of a key-value entry in data files

Author: Santosh Golecha
Created: 2025-06-06
"""

from dataclasses import dataclass
import time
from typing import ClassVar


@dataclass
class KVLocation:
    """
    Tracks the location of a key-value entry in the data files.
    
    This class is used by the keydir (in-memory hash table) to maintain
    pointers to where each key's current value is stored on disk.
    
    Attributes:
        file_id (int): The numeric identifier of the data file containing the entry
        entry_offset (int): Byte offset from the start of the file where the entry begins
        entry_size (int): Total size of the entry in bytes (includes crc)
        timestamp (int): Unix timestamp when this key was last updated
        
    Example:
        >>> location = KVLocation(file_id=1, entry_offset=1024, entry_size=56, timestamp=1672531200)
        >>> print(f"Key stored in file {location.file_id} at offset {location.entry_offset}")
    """
    file_id: int
    entry_offset: int
    entry_size: int
    timestamp: int


@dataclass
class KVEntry:
    """
    Represents a key-value entry as stored on disk.
    
    This class defines the exact format of entries written to data files.
    Each entry contains metadata (CRC, timestamp, sizes) followed by the
    actual key and value data.
    
    The on-disk format is:
        [CRC32][Timestamp][KeySize][ValueSize][Key][Value]
        
    Attributes:
        crc (int): CRC32 checksum of the entry contents (excluding the CRC field itself - network byte order)
        timestamp (int): Unix timestamp when this entry was created (network byte order)
        key_size (int): Size of the key in bytes (network byte order)
        value_size (int): Size of the value in bytes (network byte order)
        key (str): The actual key string (stored as UTF-8 encoded bytes on disk)
        value (bytes): The actual value as raw bytes
        
    Class Attributes:
        HEADER_SIZE (int): Fixed size of the entry header in bytes
        
    Example:
        >>> entry = KVEntry(
        ...     crc=0,  # Will be calculated before writing
        ...     timestamp=int(time.time()),
        ...     key_size=len("hello".encode('utf-8')),
        ...     value_size=len(b"world"),
        ...     key="hello",
        ...     value=b"world"
        ... )
        >>> print(f"Total entry size: {entry.total_size()} bytes")
    """
    
    # Class constants
    HEADER_SIZE: ClassVar[int] = 20  # 4 + 8 + 4 + 4 bytes for CRC, timestamp, key_size, value_size
    
    crc: int
    timestamp: int
    key_size: int
    value_size: int
    key: str
    value: bytes
    
    def total_size(self) -> int:
        """
        Calculate the total size of this entry when serialized.
        
        Returns:
            int: Total size in bytes including header and data
            
        Note:
            This uses UTF-8 encoding to calculate byte sizes for the key string.
            The value is already bytes, so its size is directly len(value).
            The integer fields (sizes, timestamp, CRC) will be serialized
            in network byte order when written to disk.
        """
        return self.HEADER_SIZE + len(self.key.encode('utf-8')) + len(self.value)
    
    @classmethod
    def create(cls, key: str, value: bytes, timestamp: int = None) -> 'KVEntry':
        """
        Create a new KVEntry with automatically calculated sizes.
        
        Args:
            key (str): The key string
            value (bytes): The value as raw bytes
            timestamp (int, optional): Unix timestamp. Defaults to current time.
            
        Returns:
            KVEntry: A new entry with sizes calculated and CRC set to 0
            
        Note:
            The CRC field is set to 0 and should be calculated before writing to disk.
            All integer fields will be serialized in network byte order.
            
        Example:
            >>> entry = KVEntry.create("user:123", b"John Doe")
            >>> assert entry.key_size == len("user:123".encode('utf-8'))
        """
        if timestamp is None:
            timestamp = int(time.time())
            
        return cls(
            crc=0,  # Will be calculated by KVWriter before writing to disk
            timestamp=timestamp,
            key_size=len(key.encode('utf-8')),
            value_size=len(value),
            key=key,
            value=value
        )
    
    def is_valid_sizes(self) -> bool:
        """
        Validate that the stored sizes match the actual key/value sizes.
        
        Returns:
            bool: True if sizes are consistent, False otherwise
            
        This is useful for detecting data corruption or serialization errors.
        """
        actual_key_size = len(self.key.encode('utf-8'))
        actual_value_size = len(self.value)
        return (self.key_size == actual_key_size and 
                self.value_size == actual_value_size)
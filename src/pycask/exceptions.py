"""
exceptions.py

Custom exceptions for PyCask key-value store.

This file defines all custom exceptions used throughout the PyCask
implementation. These exceptions provide specific error handling for
various failure scenarios in the key-value store operations.

Exceptions:
    PyCaskError: Base exception for all PyCask-related errors
    KeyNotFoundError: Raised when a requested key doesn't exist
    CorruptedEntryError: Raised when data corruption is detected
    WriterError: Raised when write operations fail
    ReaderError: Raised when read operations fail

Author: Santosh Golecha
Created: 2025-06-06
"""


class PyCaskError(Exception):
    """
    Base exception class for all PyCask-related errors.
    
    This serves as the root exception that all other PyCask exceptions
    inherit from, allowing users to catch all PyCask-specific errors
    with a single except clause if desired.
    
    Example:
        try:
            store.get("nonexistent_key")
        except PyCaskError as e:
            print(f"PyCask error: {e}")
    """
    pass


class StorageError(PyCaskError):
    """
    Raised for general storage-related issues.
    
    This is a catch-all exception for storage problems that don't
    fit into more specific categories. Examples include:
    - Disk space issues
    - File system errors
    - Directory creation failures
    - Configuration problems
    
    Example:
        >>> try:
        ...     store = KVStore("/readonly/path")
        ... except StorageError as e:
        ...     print(f"Storage setup failed: {e}")
    """
    pass

class KeyNotFoundError(PyCaskError, KeyError):
    """
    Raised when attempting to retrieve a key that doesn't exist.
    
    This exception inherits from both PyCaskError and the built-in KeyError
    to maintain compatibility with standard Python dictionary-like behavior.
    
    Attributes:
        key (str): The key that was not found
        
    Example:
        >>> store = KVStore("./data")
        >>> try:
        ...     value = store.get("missing_key")
        ... except KeyNotFoundError as e:
        ...     print(f"Key not found: {e.key}")
    """
    
    def __init__(self, key: str, message: str = None):
        """
        Initialize KeyNotFoundError.
        
        Args:
            key (str): The key that was not found
            message (str, optional): Custom error message
        """
        self.key = key
        if message is None:
            message = f"Key '{key}' not found in store"
        super().__init__(message)


class CorruptedEntryError(PyCaskError):
    """
    Raised when data corruption is detected in stored entries.
    
    This can occur due to:
    - CRC32 checksum mismatches
    - Invalid entry sizes or offsets  
    - Malformed UTF-8 data in keys
    - Incomplete or truncated entries
    - Invalid header data
    
    Attributes:
        location (str, optional): Description of where corruption was found
        details (str, optional): Additional details about the corruption
        
    Example:
        >>> try:
        ...     entry = reader.read_entry(location)
        ... except CorruptedEntryError as e:
        ...     print(f"Data corruption detected: {e}")
        ...     # Consider running compaction or recovery
    """
    
    def __init__(self, message: str, location: str = None, details: str = None):
        """
        Initialize CorruptedEntryError.
        
        Args:
            message (str): Primary error message
            location (str, optional): Where the corruption was detected
            details (str, optional): Additional corruption details
        """
        self.location = location
        self.details = details
        
        full_message = message
        if location:
            full_message += f" (location: {location})"
        if details:
            full_message += f" - {details}"
            
        super().__init__(full_message)


class WriterError(PyCaskError):
    """
    Raised when write operations fail.
    
    This can occur due to:
    - Disk space exhaustion
    - Permission issues
    - IO errors during writing
    - File rotation failures
    - CRC calculation errors
    
    Attributes:
        operation (str, optional): The write operation that failed
        file_path (str, optional): Path to the file being written
        
    Example:
        >>> try:
        ...     writer.write_entry(entry)
        ... except WriterError as e:
        ...     print(f"Write failed: {e}")
        ...     # Consider retry logic or error recovery
    """
    
    def __init__(self, message: str, operation: str = None, file_path: str = None):
        """
        Initialize WriterError.
        
        Args:
            message (str): Primary error message
            operation (str, optional): The operation that failed
            file_path (str, optional): File path where error occurred
        """
        self.operation = operation
        self.file_path = file_path
        
        full_message = message
        if operation:
            full_message = f"{operation}: {full_message}"
        if file_path:
            full_message += f" (file: {file_path})"
            
        super().__init__(full_message)

class FileNotFoundError(PyCaskError, FileNotFoundError):
    """
    Raised when required data files cannot be found or accessed.
    
    This exception inherits from both PyCaskError and the built-in
    FileNotFoundError to maintain compatibility with standard file operations.
    
    Common scenarios:
    - Data directory doesn't exist
    - Specific data file is missing
    - Permission issues accessing files
    - File handle creation failures
    
    Example:
        >>> try:
        ...     reader = KVReader("/nonexistent/path")
        ... except FileNotFoundError as e:
        ...     print(f"Cannot access data files: {e}")
    """
    pass

class ReaderError(PyCaskError):
    """
    Raised when read operations fail.
    
    This covers IO-related failures during reading that are not
    due to data corruption (which would raise CorruptedEntryError).
    
    Common scenarios:
    - File handle errors
    - Seek operation failures
    - Permission issues during reading
    - Network storage unavailability
    
    Attributes:
        file_id (int, optional): The file ID where the error occurred
        offset (int, optional): The byte offset where reading failed
        
    Example:
        >>> try:
        ...     value = reader.read_value(location)
        ... except ReaderError as e:
        ...     print(f"Read operation failed: {e}")
    """
    
    def __init__(self, message: str, file_id: int = None, offset: int = None):
        """
        Initialize ReaderError.
        
        Args:
            message (str): Primary error message
            file_id (int, optional): File ID where error occurred
            offset (int, optional): Byte offset where reading failed
        """
        self.file_id = file_id
        self.offset = offset
        
        full_message = message
        if file_id is not None:
            full_message += f" (file_id: {file_id}"
            if offset is not None:
                full_message += f", offset: {offset}"
            full_message += ")"
        elif offset is not None:
            full_message += f" (offset: {offset})"
            
        super().__init__(full_message)
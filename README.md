# PyCask

A Python implementation of the Bitcask key-value store, inspired by the original Bitcask paper from Basho.

## Overview

PyCask is a log-structured hash table for fast key-value storage. This implementation provides:

- **Fast writes**: All writes are sequential appends to log files
- **Fast reads**: In-memory hash table (keydir) for O(1) key lookups

## Features

- ✅ Put and Get operations
- ✅ In-memory key directory for fast lookups
- ✅ Multi-file data storage with automatic rotation
- ✅ CRC32 checksums for data integrity
- 🚧 Compaction (planned)
- 🚧 Delete operations (planned)

## Installation

```bash
git clone https://github.com/sgolecha/pycask.git
cd pycask
pip install -e .
```

## Architecture

The implementation consists of several key components:

- **KVStore**: Main interface for put/get operations  (TODO)
- **KVWriter**: Handles writing entries to data files (DONE)
- **KVReader**: Handles reading entries from multiple data files (DONE)
- **KVEntry**: Data structure representing key-value records (DONE)
- **KVLocation**: Tracks where entries are stored on disk (DONE)

## Data Format

Each record in the data files contains:
```
[CRC32][Timestamp][KeySize][ValueSize][Key][Value]
```

## Development

```bash
# Run tests
python -m pytest tests/

# Install in development mode
pip install -e .
```

## License

This project is licensed under the MIT License - see the [LICENSE](LICENSE) file for details.

## References

- [Bitcask: A Log-Structured Hash Table for Fast Key/Value Data](https://riak.com/assets/bitcask-intro.pdf) - Original paper by Basho

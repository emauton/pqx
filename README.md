# PQX - Parquet Explorer

A command-line tool for exploring and demonstrating Apache Parquet files using the [parquet-go](https://github.com/parquet-go/parquet-go) library.

## Installation

```bash
go build -o pqx .
```

## Commands

### `pqx generate`
Generate sample Parquet files with various data patterns:

```bash
# Generate 10,000 users with default settings
pqx generate

# Generate 50,000 users in RowGroups of 10,000
pqx generate -r 50000 -g 10000 -o users.parquet
```

Features demonstrated:
- Different data types (int64, string, float64, time.Time)
- Optional fields (nullable email, zip code)
- Repeated fields (tags array)
- Nested structures (Address)
- Various encodings (dictionary, delta)
- Compression algorithms (snappy)

### `pqx inspect <file>`
Inspect the structure of a Parquet file:

```bash
pqx inspect sample.parquet
```

Shows:
- File metadata and total rows
- RowGroup information
- ColumnChunk details
- Basic statistics (min/max values)
- Bloom filter presence

### `pqx pages <file>`
Explore the page-level structure:

```bash
# Analyze first column, show first 5 pages per RowGroup
pqx pages sample.parquet

# Analyze column 2, show up to 10 pages
pqx pages sample.parquet -c 2 -m 10
```

Demonstrates:
- Page boundaries and sizes
- Page statistics (min/max, null counts)
- Sample values with repetition/definition levels
- Dictionary information

### `pqx schema <file>`
Display schema information:

```bash
# Show schema tree
pqx schema sample.parquet

# Show schema + generated Go type
pqx schema sample.parquet --go-type
```

Shows:
- Schema tree structure
- Column paths and types
- Generated Go struct equivalent
- Logical type information

## Examples

```bash
# 1. Generate a sample file
pqx generate -r 1000 -o demo.parquet

# 2. Inspect the file structure  
pqx inspect demo.parquet

# 3. Look at the schema
pqx schema demo.parquet --go-type

# 4. Explore pages in the name column (column 1)
pqx pages demo.parquet -c 1 -m 3
```

## Architecture Concepts Demonstrated

This tool demonstrates key Parquet concepts:

- **File-level schema** with consistent structure across RowGroups
- **RowGroups** as horizontal partitions for memory management and parallelization  
- **ColumnChunks** as the intersection of RowGroups and Columns
- **Pages** as the basic I/O and compression units
- **Repetition/Definition levels** for nested and optional data
- **Encodings** (dictionary, delta, plain) for different data patterns
- **Schema inference** from Go types vs explicit schema definition
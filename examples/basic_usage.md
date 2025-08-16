# Basic Usage Examples

This document demonstrates the key Parquet concepts using the `pqx` tool.

## 1. Generate Sample Data

```bash
# Generate 1000 users with RowGroups of 500 each
./pqx generate -r 1000 -g 500 -o data/users.parquet
```

This creates a file demonstrating:
- **File-level schema** applied to all RowGroups
- **RowGroups** as horizontal data partitions (2 groups of 500 rows each)
- **Different data types** (int64, string, float64, timestamp, nested structs)
- **Optional fields** (email, zip_code) showing NULL handling
- **Repeated fields** (tags array) showing list encoding
- **Nested structures** (Address struct) showing group encoding
- **Dictionary encoding** (name field) for string compression

## 2. Inspect File Structure

```bash
./pqx inspect data/users.parquet
```

Key observations:
- **File Size**: ~91KB for 1000 records (good compression)
- **RowGroups**: 2 groups (horizontal partitioning)
- **ColumnChunks**: 10 per RowGroup (one per logical column)
- **Statistics**: Min/max values per chunk for query optimization
- **Dictionary**: Name column uses dictionary encoding (500 unique entries)

## 3. Examine Schema

```bash
./pqx schema data/users.parquet --go-type
```

Shows:
- **Schema tree** with nested groups and logical types
- **Column paths** showing flattened structure
- **Generated Go type** demonstrating type inference
- **Required vs Optional** field distinctions

## 4. Explore Page Structure

```bash
# Look at name column (column 1)
./pqx pages data/users.parquet -c 1

# Look at tags column (column 5) - repeated field
./pqx pages data/users.parquet -c 5
```

Demonstrates:
- **Pages** as basic I/O units within ColumnChunks
- **Dictionary encoding** (name column has 500 dictionary entries)
- **Repetition/Definition levels** (R:0 D:0 for required, non-repeated fields)
- **Page statistics** for query optimization
- **Sample values** showing actual data

## Key Architecture Concepts Demonstrated

### 1. **Hierarchical Structure**
```
File
├── RowGroup 0 (rows 1-500)
│   ├── ColumnChunk: id
│   │   └── Page 0: 500 values
│   ├── ColumnChunk: name  
│   │   └── Page 0: 500 values (dictionary encoded)
│   └── ... (8 more ColumnChunks)
└── RowGroup 1 (rows 501-1000)
    └── ... (same structure)
```

### 2. **Schema Consistency**
- Same schema across all RowGroups
- Column positions and types never change
- Enables efficient query planning

### 3. **Columnar Benefits**
- **Compression**: Similar values grouped together
- **Encoding**: Dictionary for repeated strings, delta for integers
- **I/O Efficiency**: Read only needed columns
- **Statistics**: Skip RowGroups/Pages based on min/max values

### 4. **Nested Data Handling**
- **Repetition levels**: Track array boundaries
- **Definition levels**: Handle optional/null fields
- **Groups**: Represent nested structures
- **Lists**: Encode arrays efficiently

## Advanced Examples

### Generate Different Patterns

```bash
# Small file with large RowGroups
./pqx generate -r 10000 -g 10000 -o data/single_rowgroup.parquet

# Large file with small RowGroups  
./pqx generate -r 10000 -g 1000 -o data/many_rowgroups.parquet

# Compare file sizes and structures
./pqx inspect data/single_rowgroup.parquet
./pqx inspect data/many_rowgroups.parquet
```

### Analyze Different Columns

```bash
# Examine optional email field (has nulls)
./pqx pages data/users.parquet -c 2

# Examine repeated tags field (array data)
./pqx pages data/users.parquet -c 5

# Examine nested address.street field
./pqx pages data/users.parquet -c 7
```

This tool provides hands-on experience with all the major Parquet concepts we discussed!
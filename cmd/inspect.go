package cmd

import (
	"fmt"
	"os"

	"github.com/parquet-go/parquet-go"
	"github.com/spf13/cobra"
)

var inspectCmd = &cobra.Command{
	Use:   "inspect <parquet-file>",
	Short: "Inspect the structure of a Parquet file",
	Long: `Inspect displays detailed information about a Parquet file including:
- File metadata and schema
- RowGroup information 
- ColumnChunk details
- Page statistics
- Encoding and compression info`,
	Args: cobra.ExactArgs(1),
	RunE: func(cmd *cobra.Command, args []string) error {
		filename := args[0]
		return inspectParquetFile(filename)
	},
}

func inspectParquetFile(filename string) error {
	// Open the file
	file, err := os.Open(filename)
	if err != nil {
		return fmt.Errorf("failed to open file: %w", err)
	}
	defer file.Close()

	// Get file info for size
	stat, err := file.Stat()
	if err != nil {
		return fmt.Errorf("failed to stat file: %w", err)
	}

	// Open Parquet file
	pqFile, err := parquet.OpenFile(file, stat.Size())
	if err != nil {
		return fmt.Errorf("failed to open parquet file: %w", err)
	}

	// Display file information
	fmt.Printf("=== Parquet File: %s ===\n", filename)
	fmt.Printf("File Size: %d bytes\n", stat.Size())
	fmt.Printf("Total Rows: %d\n", pqFile.NumRows())
	fmt.Printf("Number of RowGroups: %d\n", len(pqFile.RowGroups()))
	fmt.Println()

	// Display schema
	schema := pqFile.Schema()
	fmt.Printf("=== Schema ===\n")
	fmt.Printf("Generated Go Type: %s\n", schema.GoType())
	fmt.Printf("Number of Columns: %d\n", len(schema.Columns()))
	fmt.Println()

	// Display RowGroup information
	for i, rowGroup := range pqFile.RowGroups() {
		fmt.Printf("=== RowGroup %d ===\n", i)
		fmt.Printf("Rows: %d\n", rowGroup.NumRows())
		fmt.Printf("ColumnChunks: %d\n", len(rowGroup.ColumnChunks()))
		
		// Display ColumnChunk information
		for j, chunk := range rowGroup.ColumnChunks() {
			fmt.Printf("  Column %d:\n", j)
			fmt.Printf("    Type: %s\n", chunk.Type())
			fmt.Printf("    Values: %d\n", chunk.NumValues())
			
			// Try to get column index for statistics
			if columnIndex, err := chunk.ColumnIndex(); err == nil {
				fmt.Printf("    Pages: %d\n", columnIndex.NumPages())
				if columnIndex.NumPages() > 0 {
					minVal := columnIndex.MinValue(0)
					maxVal := columnIndex.MaxValue(0)
					fmt.Printf("    Min Value: %v\n", minVal)
					fmt.Printf("    Max Value: %v\n", maxVal)
				}
			}
			
			// Show Bloom filter info
			if bloomFilter := chunk.BloomFilter(); bloomFilter != nil {
				fmt.Printf("    Bloom Filter: Present\n")
			}
		}
		fmt.Println()
	}

	return nil
}

func init() {
	rootCmd.AddCommand(inspectCmd)
}
package cmd

import (
	"fmt"
	"io"
	"os"

	"github.com/parquet-go/parquet-go"
	"github.com/spf13/cobra"
)

var (
	columnIndex int
	maxPages    int
)

var pagesCmd = &cobra.Command{
	Use:   "pages <parquet-file>",
	Short: "Explore pages within ColumnChunks",
	Long: `Pages command demonstrates the page-level structure of Parquet files:
- Shows page boundaries and sizes
- Displays page statistics (min/max values)
- Demonstrates how data is chunked for I/O
- Shows encoding information per page`,
	Args: cobra.ExactArgs(1),
	RunE: func(cmd *cobra.Command, args []string) error {
		filename := args[0]
		return explorePages(filename)
	},
}

func explorePages(filename string) error {
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

	fmt.Printf("=== Page Analysis: %s ===\n", filename)
	fmt.Printf("Analyzing column %d\n\n", columnIndex)

	// Iterate through RowGroups
	for rgIdx, rowGroup := range pqFile.RowGroups() {
		fmt.Printf("=== RowGroup %d ===\n", rgIdx)
		
		chunks := rowGroup.ColumnChunks()
		if columnIndex >= len(chunks) {
			return fmt.Errorf("column index %d out of range (max: %d)", columnIndex, len(chunks)-1)
		}
		
		chunk := chunks[columnIndex]
		fmt.Printf("Column Type: %s\n", chunk.Type())
		fmt.Printf("Total Values: %d\n", chunk.NumValues())
		
		// Get pages from the column chunk
		pages := chunk.Pages()
		defer pages.Close()
		
		pageNum := 0
		for pageNum < maxPages {
			page, err := pages.ReadPage()
			if err == io.EOF {
				break
			}
			if err != nil {
				return fmt.Errorf("failed to read page: %w", err)
			}
			
			fmt.Printf("\n  Page %d:\n", pageNum)
			fmt.Printf("    Values: %d\n", page.NumValues())
			fmt.Printf("    Rows: %d\n", page.NumRows())
			fmt.Printf("    Nulls: %d\n", page.NumNulls())
			fmt.Printf("    Size: %d bytes\n", page.Size())
			fmt.Printf("    Column: %d\n", page.Column())
			
			// Show page bounds (min/max values)
			if min, max, ok := page.Bounds(); ok {
				fmt.Printf("    Min Value: %v\n", min)
				fmt.Printf("    Max Value: %v\n", max)
			} else {
				fmt.Printf("    Bounds: Not available\n")
			}
			
			// Show dictionary info if present
			if dict := page.Dictionary(); dict != nil {
				fmt.Printf("    Dictionary: %d entries\n", dict.Len())
			} else {
				fmt.Printf("    Dictionary: None\n")
			}
			
			// Demonstrate reading values from the page
			valueReader := page.Values()
			values := make([]parquet.Value, min(10, int(page.NumValues()))) // Read first 10 values
			n, err := valueReader.ReadValues(values)
			if err != nil && err != io.EOF {
				return fmt.Errorf("failed to read values: %w", err)
			}
			
			if n > 0 {
				fmt.Printf("    Sample Values (%d shown):\n", n)
				for i := 0; i < n; i++ {
					fmt.Printf("      [%d] %v (R:%d D:%d)\n", 
						i, values[i], 
						values[i].RepetitionLevel(), 
						values[i].DefinitionLevel())
				}
			}
			
			pageNum++
		}
		
		if pageNum == maxPages {
			fmt.Printf("\n  ... (showing only first %d pages)\n", maxPages)
		}
		fmt.Println()
	}

	return nil
}

func init() {
	pagesCmd.Flags().IntVarP(&columnIndex, "column", "c", 0, "Column index to analyze")
	pagesCmd.Flags().IntVarP(&maxPages, "max-pages", "m", 5, "Maximum number of pages to show per RowGroup")
	
	rootCmd.AddCommand(pagesCmd)
}
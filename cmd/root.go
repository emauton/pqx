package cmd

import (
	"fmt"
	"os"

	"github.com/spf13/cobra"
)

var rootCmd = &cobra.Command{
	Use:   "pqx",
	Short: "A CLI tool for exploring Parquet files and demonstrating parquet-go features",
	Long: `pqx (Parquet eXplorer) is a command-line tool built with Cobra for exploring and 
experimenting with Apache Parquet files using the parquet-go library.

It provides various commands to:
- Inspect Parquet file structure and schema
- Generate sample data and write Parquet files  
- Demonstrate different encodings and optimizations
- Show RowGroups, ColumnChunks, and Pages
- Convert between formats`,
}

func Execute() {
	if err := rootCmd.Execute(); err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
}

func init() {
	// Global flags can be added here
}
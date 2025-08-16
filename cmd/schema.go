package cmd

import (
	"fmt"
	"os"
	"reflect"

	"github.com/parquet-go/parquet-go"
	"github.com/spf13/cobra"
)

var showGoType bool

var schemaCmd = &cobra.Command{
	Use:   "schema <parquet-file>",
	Short: "Display schema information and generate Go types",
	Long: `Schema command shows detailed information about the Parquet schema:
- Schema tree structure
- Column paths and types
- Generated Go type equivalent
- Logical type information`,
	Args: cobra.ExactArgs(1),
	RunE: func(cmd *cobra.Command, args []string) error {
		filename := args[0]
		return analyzeSchema(filename)
	},
}

func analyzeSchema(filename string) error {
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

	schema := pqFile.Schema()
	
	fmt.Printf("=== Parquet Schema: %s ===\n\n", filename)
	
	// Show basic schema info
	fmt.Printf("Schema Root: %s\n", schema.Name())
	fmt.Printf("Total Columns: %d\n", len(schema.Columns()))
	fmt.Println()
	
	// Show column information
	fmt.Printf("=== Columns ===\n")
	columns := schema.Columns()
	for i, columnPath := range columns {
		fmt.Printf("Column %d: %s\n", i, formatColumnPath(columnPath))
		
		// Get the leaf node for this column
		// This is a simplified way to show column info
		fmt.Printf("  Path: %v\n", columnPath)
	}
	fmt.Println()
	
	// Generate and show Go type
	if showGoType {
		goType := schema.GoType()
		fmt.Printf("=== Generated Go Type ===\n")
		fmt.Printf("Type: %s\n", goType)
		fmt.Printf("Kind: %s\n", goType.Kind())
		
		if goType.Kind() == reflect.Struct {
			fmt.Printf("Fields: %d\n", goType.NumField())
			fmt.Println("\nStruct Definition:")
			fmt.Printf("type GeneratedStruct struct {\n")
			
			for i := 0; i < goType.NumField(); i++ {
				field := goType.Field(i)
				fmt.Printf("    %-12s %s\n", field.Name, field.Type)
			}
			fmt.Printf("}\n")
		}
		fmt.Println()
	}
	
	// Show schema tree structure
	fmt.Printf("=== Schema Tree ===\n")
	printSchemaNode(schema, "", 0)
	
	return nil
}

func formatColumnPath(path []string) string {
	result := ""
	for i, part := range path {
		if i > 0 {
			result += "."
		}
		result += part
	}
	return result
}

func printSchemaNode(node parquet.Node, indent string, depth int) {
	if depth > 10 { // Prevent infinite recursion
		fmt.Printf("%s... (max depth reached)\n", indent)
		return
	}
	
	// Print current node
	nodeType := "GROUP"
	if node.Leaf() {
		nodeType = node.Type().String()
	}
	
	repetition := "REQUIRED"
	if node.Optional() {
		repetition = "OPTIONAL"
	} else if node.Repeated() {
		repetition = "REPEATED"
	}
	
	// Get node name - root schema doesn't have a name, fields do
	nodeName := "(root)"
	if field, ok := node.(parquet.Field); ok {
		nodeName = field.Name()
	}
	
	fmt.Printf("%s%s %s %s\n", indent, repetition, nodeType, nodeName)
	
	// Print child nodes if this is a group
	if !node.Leaf() {
		for _, field := range node.Fields() {
			printSchemaNode(field, indent+"  ", depth+1)
		}
	}
}

func init() {
	schemaCmd.Flags().BoolVarP(&showGoType, "go-type", "g", false, "Show generated Go type")
	
	rootCmd.AddCommand(schemaCmd)
}
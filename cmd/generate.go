package cmd

import (
	"fmt"
	"math/rand"
	"os"
	"time"

	"github.com/parquet-go/parquet-go"
	"github.com/spf13/cobra"
)

var (
	numRows     int
	rowGroupSize int
	outputFile  string
)

var generateCmd = &cobra.Command{
	Use:   "generate",
	Short: "Generate sample Parquet files with different data patterns",
	Long: `Generate creates sample Parquet files to demonstrate various features:
- Different data types and encodings
- Nested structures  
- Optional and repeated fields
- Various compression algorithms`,
	RunE: func(cmd *cobra.Command, args []string) error {
		return generateSampleData()
	},
}

// Sample data structures to demonstrate Parquet features
type User struct {
	ID        int64     `parquet:"id"`
	Name      string    `parquet:"name,dict"`           // Dictionary encoding
	Email     *string   `parquet:"email,optional"`      // Optional field
	Age       int32     `parquet:"age"`
	Score     float64   `parquet:"score"`               // Regular encoding for floats
	Tags      []string  `parquet:"tags,list"`           // Repeated field
	CreatedAt time.Time `parquet:"created_at,timestamp"`
	Address   Address   `parquet:"address"`             // Nested structure
}

type Address struct {
	Street  string  `parquet:"street"`
	City    string  `parquet:"city,snappy"`  // Snappy compression
	ZipCode *string `parquet:"zip_code,optional"`
}

func generateSampleData() error {
	// Create output file
	file, err := os.Create(outputFile)
	if err != nil {
		return fmt.Errorf("failed to create output file: %w", err)
	}
	defer file.Close()

	// Create writer with custom row group size
	writer := parquet.NewGenericWriter[User](file, 
		parquet.MaxRowsPerRowGroup(int64(rowGroupSize)),
	)
	defer writer.Close()

	fmt.Printf("Generating %d users in RowGroups of %d...\n", numRows, rowGroupSize)

	// Generate data in batches
	batchSize := 1000
	cities := []string{"New York", "Los Angeles", "Chicago", "Houston", "Phoenix", "Philadelphia"}
	tags := []string{"golang", "parquet", "data", "analytics", "performance", "columnar"}
	
	for i := 0; i < numRows; i += batchSize {
		remaining := numRows - i
		if remaining > batchSize {
			remaining = batchSize
		}
		
		batch := make([]User, remaining)
		for j := 0; j < remaining; j++ {
			userID := int64(i + j + 1)
			
			// Some users have no email (demonstrate optional fields)
			var email *string
			if rand.Float32() < 0.8 { // 80% have email
				emailStr := fmt.Sprintf("user%d@example.com", userID)
				email = &emailStr
			}
			
			// Some addresses have no zip code
			var zipCode *string
			if rand.Float32() < 0.7 { // 70% have zip code
				zip := fmt.Sprintf("%05d", rand.Intn(99999))
				zipCode = &zip
			}
			
			// Random number of tags (0-3)
			numTags := rand.Intn(4)
			userTags := make([]string, numTags)
			for k := 0; k < numTags; k++ {
				userTags[k] = tags[rand.Intn(len(tags))]
			}
			
			batch[j] = User{
				ID:    userID,
				Name:  fmt.Sprintf("User_%d", userID),
				Email: email,
				Age:   int32(18 + rand.Intn(65)), // Age 18-82
				Score: rand.Float64() * 100,     // Score 0-100
				Tags:  userTags,
				CreatedAt: time.Now().Add(-time.Duration(rand.Intn(365*24)) * time.Hour),
				Address: Address{
					Street:  fmt.Sprintf("%d Main St", rand.Intn(9999)+1),
					City:    cities[rand.Intn(len(cities))],
					ZipCode: zipCode,
				},
			}
		}
		
		// Write batch
		if _, err := writer.Write(batch); err != nil {
			return fmt.Errorf("failed to write batch: %w", err)
		}
		
		if (i+batchSize)%10000 == 0 || i+batchSize >= numRows {
			fmt.Printf("Generated %d/%d users\n", min(i+batchSize, numRows), numRows)
		}
	}

	fmt.Printf("Successfully generated %s with %d users\n", outputFile, numRows)
	return nil
}

func min(a, b int) int {
	if a < b {
		return a
	}
	return b
}

func init() {
	generateCmd.Flags().IntVarP(&numRows, "rows", "r", 10000, "Number of rows to generate")
	generateCmd.Flags().IntVarP(&rowGroupSize, "rowgroup-size", "g", 5000, "Rows per RowGroup")
	generateCmd.Flags().StringVarP(&outputFile, "output", "o", "sample.parquet", "Output filename")
	
	rootCmd.AddCommand(generateCmd)
}
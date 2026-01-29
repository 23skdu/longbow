package main

import (
	"fmt"
	"os"
	"path/filepath"
	"strings"
)

type ImportChange struct {
	FilePath  string
	OldImport string
	NewImport string
	Modified  bool
}

var importMappings = map[string]string{
	"github.com/apache/arrow-go/v23/arrow":         "github.com/apache/arrow-go/v23/arrow",
	"github.com/apache/arrow-go/v23/arrow/array":   "github.com/apache/arrow-go/v23/arrow/array",
	"github.com/apache/arrow-go/v23/arrow/memory":  "github.com/apache/arrow-go/v23/arrow/memory",
	"github.com/apache/arrow-go/v23/arrow/compute": "github.com/apache/arrow-go/v23/arrow/compute",
	"github.com/apache/arrow-go/v23/arrow/ipc":     "github.com/apache/arrow-go/v23/arrow/ipc",
	"github.com/apache/arrow-go/v23/arrow/scalar":  "github.com/apache/arrow-go/v23/arrow/scalar",
	"github.com/apache/arrow-go/v23/arrow/flight":  "github.com/apache/arrow-go/v23/arrow/flight",
	"github.com/apache/arrow-go/v23/arrow/float16": "github.com/apache/arrow-go/v23/arrow/float16",
	"github.com/apache/arrow-go/v23/arrow/parquet": "github.com/apache/arrow-go/v23/arrow/parquet",
}

func findGoFiles(root string) ([]string, error) {
	var goFiles []string
	err := filepath.Walk(root, func(path string, info os.FileInfo, walkErr error) error {
		if walkErr != nil {
			return nil
		}
		if info.IsDir() || !strings.HasSuffix(path, ".go") {
			return nil
		}
		goFiles = append(goFiles, path)
		return nil
	})
	return goFiles, err
}

func updateImports(filePath string, changes []ImportChange) (bool, error) {
	content, err := os.ReadFile(filePath)
	if err != nil {
		return false, err
	}

	updatedContent := content
	modified := false

	for _, change := range changes {
		if strings.Contains(string(content), change.OldImport) {
			updatedContent = []byte(strings.ReplaceAll(string(updatedContent), change.OldImport, change.NewImport))
			modified = true
		}
	}

	if modified {
		err = os.WriteFile(filePath, updatedContent, 0644)
		if err != nil {
			return false, err
		}
	}

	return modified, nil
}

func generateMigrationPlan() []ImportChange {
	changes := []ImportChange{}
	for oldImp, newImp := range importMappings {
		changes = append(changes, ImportChange{
			OldImport: oldImp,
			NewImport: newImp,
			Modified:  false,
		})
	}
	return changes
}

func main() {
	root := "/Users/rsd/REPOS/longbow"
	fmt.Println("Arrow v18 → v23 Migration Tool")
	fmt.Println("====================================")

	changes := generateMigrationPlan()
	fmt.Printf("Found %d import changes needed:\n", len(changes))

	goFiles, err := findGoFiles(root)
	if err != nil {
		fmt.Printf("Error: %v\n", err)
		os.Exit(1)
	}

	modifiedCount := 0
	for _, filePath := range goFiles {
		changed, err := updateImports(filePath, changes)
		if err != nil {
			fmt.Printf("Error updating %s: %v\n", filePath, err)
			continue
		}
		if changed {
			modifiedCount++
		}
	}

	fmt.Printf("Processed %d files, %d modified\n", len(goFiles), modifiedCount)
	fmt.Println("Migration complete. Ready for manual review.")
	fmt.Println("Commands to run:")
	fmt.Println("go mod tidy")
	fmt.Println("go build ./...")
}

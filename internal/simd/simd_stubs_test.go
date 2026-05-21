package simd

import (
	"bufio"
	"go/ast"
	"go/parser"
	"go/token"
	"os"
	"strings"
	"testing"
)

// TestNoRedundantGeneratedStubs checks that functions declared in
// all_kernels_stubs_amd64.go do not duplicate declarations in simd_amd64.go
// (or other hand-written assembly stub files). This prevents the compilation
// error that occurred with euclideanInt16AVX2Kernel et al. being declared in
// both files — see docs/nextsteps.md "Build System Fix Required".
//
// Files with architecture-specific build constraints (arm64, avx512, etc.)
// or platform fallback files (stubs_avx_fallbacks.go) are excluded since
// they are never compiled alongside the generated amd64 stubs.
func TestNoRedundantGeneratedStubs(t *testing.T) {
	// Read build constraint lines from each .go file.
	// We skip files whose build constraint excludes GOARCH=amd64.
	fileBuildTags := readBuildTags(t)

	fset := token.NewFileSet()
	funcDecls := make(map[string]map[string]bool)

	entries, err := os.ReadDir(".")
	if err != nil {
		t.Fatalf("failed to read dir: %v", err)
	}

	for _, entry := range entries {
		fname := entry.Name()
		if entry.IsDir() || !strings.HasSuffix(fname, ".go") {
			continue
		}
		if strings.HasSuffix(fname, "_test.go") || fname == "generate.go" || fname == "tools.go" {
			continue
		}

		// Skip files with a build constraint that excludes amd64
		tags := fileBuildTags[fname]
		if len(tags) > 0 {
			isAmd64 := false
			for _, tag := range tags {
				if tag == "amd64" || tag == "!arm64" {
					isAmd64 = true
					break
				}
			}
			if !isAmd64 {
				// Only compiled for non-amd64 archs
				continue
			}
		}

		file, err := parser.ParseFile(fset, fname, nil, parser.SkipObjectResolution)
		if err != nil {
			t.Logf("Skipping %s (parse error): %v", fname, err)
			continue
		}
		if file.Name.Name != "simd" {
			continue
		}

		for _, decl := range file.Decls {
			funcDecl, ok := decl.(*ast.FuncDecl)
			if !ok || funcDecl.Name == nil || funcDecl.Recv != nil {
				continue
			}
			name := funcDecl.Name.Name
			if name == "init" {
				continue
			}
			if funcDecls[name] == nil {
				funcDecls[name] = make(map[string]bool)
			}
			funcDecls[name][fname] = true
		}
	}

	// Focus specifically on: all_kernels_stubs_amd64.go + any other amd64 file
	var duplicates []string
	for name, files := range funcDecls {
		if len(files) < 2 {
			continue
		}
		// Check if all_kernels_stubs_amd64.go is one of the files
		if !files["all_kernels_stubs_amd64.go"] {
			continue
		}
		var others []string
		for f := range files {
			if f != "all_kernels_stubs_amd64.go" {
				others = append(others, f)
			}
		}
		if len(others) > 0 {
			duplicates = append(duplicates,
				name+" declared in all_kernels_stubs_amd64.go and: "+strings.Join(others, ", "))
		}
	}

	if len(duplicates) > 0 {
		t.Errorf("Function(s) from generated stubs also declared elsewhere (duplicate symbol compilation error):\n%s",
			strings.Join(duplicates, "\n"))
	}
}

// readBuildTags reads //go:build lines from Go source files in the current directory.
func readBuildTags(t *testing.T) map[string][]string {
	t.Helper()
	result := make(map[string][]string)
	entries, err := os.ReadDir(".")
	if err != nil {
		t.Fatalf("failed to read dir: %v", err)
	}
	for _, entry := range entries {
		fname := entry.Name()
		if entry.IsDir() || !strings.HasSuffix(fname, ".go") || strings.HasSuffix(fname, "_test.go") {
			continue
		}
		f, err := os.Open(fname)
		if err != nil {
			continue
		}
		scanner := bufio.NewScanner(f)
		for scanner.Scan() {
			line := strings.TrimSpace(scanner.Text())
			if strings.HasPrefix(line, "//go:build") {
				tags := strings.Fields(strings.TrimPrefix(line, "//go:build"))
				result[fname] = tags
				break
			}
			if strings.HasPrefix(line, "// +build") {
				tags := strings.Fields(strings.TrimPrefix(line, "// +build"))
				result[fname] = tags
				break
			}
			if !strings.HasPrefix(line, "//") && line != "" {
				// First non-comment line, no build constraint found
				break
			}
		}
		f.Close()
	}
	return result
}

// Command metrics-inventory extracts the set of Prometheus metric families this
// repository registers, together with their label names.
//
// It is the source of truth for the observability conformance gate in
// internal/observability and for the metric table in docs/metrics.md.
//
// Usage:
//
//	go run ./tools/metrics-inventory > internal/observability/testdata/registered_metrics.tsv
//
// Why AST parsing rather than a regex: metric declarations are spread across
// ~40 files and appear in several shapes (promauto.NewCounter, a shared
// constructor taking a Name string, options built in a helper). A regex over
// `Name: "..."` cannot tell a registration from an unrelated struct field with
// a field called Name, and cannot recover label names. Walking composite
// literals can.
package main

import (
	"fmt"
	"go/ast"
	"go/parser"
	"go/token"
	"os"
	"path/filepath"
	"sort"
	"strconv"
	"strings"
)

const namespacePrefix = "longbow_"

// metricVar identifies a package-level variable holding a metric, so that
// `X := mustRegister(Name("longbow_foo", ...))` can be tied back to X.
type registration struct {
	name   string
	labels []string
	file   string
	line   int
}

// constructorNames are the helpers this repository uses to build a metric.
var constructorNames = map[string]bool{
	"promauto.NewCounter":        true,
	"promauto.NewGauge":          true,
	"promauto.NewHistogram":      true,
	"promauto.NewSummary":        true,
	"promauto.NewCounterVec":     true,
	"promauto.NewGaugeVec":       true,
	"promauto.NewHistogramVec":   true,
	"promauto.NewSummaryVec":     true,
	"prometheus.NewCounter":      true,
	"prometheus.NewGauge":        true,
	"prometheus.NewHistogram":    true,
	"prometheus.NewSummary":      true,
	"prometheus.NewCounterVec":   true,
	"prometheus.NewGaugeVec":     true,
	"prometheus.NewHistogramVec": true,
	"prometheus.NewSummaryVec":   true,
}

// helperNames are in-repo constructors that accept a metric name as their first
// string argument and a []string of labels as their second.
var helperNames = map[string]bool{
	"mustRegister": true,
	"newMetric":    true,
}

func main() {
	roots := []string{"internal", "cmd"}
	if len(os.Args) > 1 {
		roots = os.Args[1:]
	}

	fset := token.NewFileSet()
	var regs []registration

	for _, root := range roots {
		err := filepath.Walk(root, func(path string, info os.FileInfo, err error) error {
			if err != nil {
				return err
			}
			if info.IsDir() {
				if info.Name() == "vendor" || strings.HasPrefix(info.Name(), ".") {
					return filepath.SkipDir
				}
				return nil
			}
			if !strings.HasSuffix(path, ".go") || strings.HasSuffix(path, "_test.go") {
				return nil
			}
			f, err := parser.ParseFile(fset, path, nil, 0)
			if err != nil {
				fmt.Fprintf(os.Stderr, "parse %s: %v\n", path, err)
				return nil
			}
			regs = append(regs, extract(fset, path, f)...)
			return nil
		})
		if err != nil {
			fmt.Fprintln(os.Stderr, err)
			os.Exit(1)
		}
	}

	// Merge: the same family may be declared more than once across files.
	byName := map[string]registration{}
	labelsFor := map[string]map[string]bool{}
	for _, r := range regs {
		if !strings.HasPrefix(r.name, namespacePrefix) {
			continue
		}
		if _, ok := byName[r.name]; !ok {
			byName[r.name] = r
		}
		if labelsFor[r.name] == nil {
			labelsFor[r.name] = map[string]bool{}
		}
		for _, l := range r.labels {
			labelsFor[r.name][l] = true
		}
	}

	names := make([]string, 0, len(byName))
	for n := range byName {
		names = append(names, n)
	}
	sort.Strings(names)

	fmt.Fprintf(os.Stderr, "extracted %d metric families from %d roots\n", len(names), len(roots))
	for _, n := range names {
		ls := make([]string, 0, len(labelsFor[n]))
		for l := range labelsFor[n] {
			ls = append(ls, l)
		}
		sort.Strings(ls)
		fmt.Printf("%s\t%s\n", n, strings.Join(ls, ","))
	}
}

func extract(fset *token.FileSet, path string, f *ast.File) []registration {
	var out []registration
	ast.Inspect(f, func(n ast.Node) bool {
		call, ok := n.(*ast.CallExpr)
		if !ok {
			return true
		}
		sel, ok := call.Fun.(*ast.SelectorExpr)
		if !ok {
			return true
		}
		pkg, ok := sel.X.(*ast.Ident)
		if !ok {
			return true
		}
		name := pkg.Name + "." + sel.Sel.Name
		pos := fset.Position(call.Pos())

		switch {
		case constructorNames[name]:
			name, labels := fromOpts(call)
			if name != "" {
				out = append(out, registration{name: name, labels: labels, file: path, line: pos.Line})
			}
		case helperNames[sel.Sel.Name]:
			if len(call.Args) >= 2 {
				if n, ok := stringLit(call.Args[0]); ok {
					if ls, ok := stringSlice(call.Args[1]); ok {
						out = append(out, registration{name: n, labels: ls, file: path, line: pos.Line})
					}
				}
			}
		}
		return true
	})
	return out
}

// fromOpts pulls Name and the []string label list out of a constructor call.
//
// A family may be split across `Namespace` and `Name`; Prometheus joins them
// with an underscore, so the two are composed here rather than dropping the
// family (which is how `longbow_filter_execution_duration_seconds` would
// otherwise go missing).
func fromOpts(call *ast.CallExpr) (string, []string) {
	var name, namespace string
	var labels []string
	for _, arg := range call.Args {
		cl, ok := arg.(*ast.CompositeLit)
		if !ok {
			continue
		}
		for _, elt := range cl.Elts {
			kv, ok := elt.(*ast.KeyValueExpr)
			if !ok {
				continue
			}
			key, ok := kv.Key.(*ast.Ident)
			if !ok {
				continue
			}
			switch key.Name {
			case "Name":
				if n, ok := stringLit(kv.Value); ok {
					name = n
				}
			case "Namespace":
				if n, ok := stringLit(kv.Value); ok {
					namespace = n
				}
			}
		}
		// Label list: []string{...} appearing as a constructor argument.
		if ls, ok := stringSlice(cl); ok && labels == nil {
			labels = ls
		}
	}
	if namespace != "" && name != "" && !strings.Contains(name, namespace+"_") {
		name = namespace + "_" + name
	}
	// The label list is normally a sibling argument of the opts literal, not
	// part of it.
	for _, arg := range call.Args {
		if ls, ok := stringSlice(arg); ok {
			labels = ls
		}
	}
	return name, labels
}

func stringLit(e ast.Expr) (string, bool) {
	lit, ok := e.(*ast.BasicLit)
	if !ok || lit.Kind != token.STRING {
		return "", false
	}
	s, err := strconv.Unquote(lit.Value)
	if err != nil {
		return "", false
	}
	return s, true
}

// stringSlice returns the elements of a []string{...} composite literal.
func stringSlice(e ast.Expr) ([]string, bool) {
	cl, ok := e.(*ast.CompositeLit)
	if !ok {
		return nil, false
	}
	arr, ok := cl.Type.(*ast.ArrayType)
	if !ok {
		return nil, false
	}
	if id, ok := arr.Elt.(*ast.Ident); !ok || id.Name != "string" {
		return nil, false
	}
	out := make([]string, 0, len(cl.Elts))
	for _, elt := range cl.Elts {
		if s, ok := stringLit(elt); ok {
			out = append(out, s)
		}
	}
	return out, true
}

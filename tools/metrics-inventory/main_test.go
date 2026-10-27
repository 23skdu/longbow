package main

import (
	"go/parser"
	"go/token"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestExtract_PromautoCounter(t *testing.T) {
	src := `package test
import "github.com/prometheus/client_golang/prometheus/promauto"
import "github.com/prometheus/client_golang/prometheus"

var testCounter = promauto.NewCounter(prometheus.CounterOpts{
	Name: "longbow_test_total",
	Help: "A test counter",
})
`
	fset := token.NewFileSet()
	f, err := parser.ParseFile(fset, "test.go", src, 0)
	require.NoError(t, err)

	regs := extract(fset, "test.go", f)
	require.Len(t, regs, 1)
	assert.Equal(t, "longbow_test_total", regs[0].name)
}

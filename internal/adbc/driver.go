package adbc

import (
	"github.com/apache/arrow-adbc/go/adbc"
)

// Driver represents a driver value.
type Driver struct{}

// NewDriver constructs a new Driver.
func NewDriver() adbc.Driver {
	return &Driver{}
}

// NewDatabase constructs a new Database.
func (d *Driver) NewDatabase(opts map[string]string) (adbc.Database, error) {
	return NewDatabase(opts)
}

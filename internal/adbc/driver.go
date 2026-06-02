package adbc

import (
	"github.com/apache/arrow-adbc/go/adbc"
)

type Driver struct{}

func NewDriver() adbc.Driver {
	return &Driver{}
}

func (d *Driver) NewDatabase(opts map[string]string) (adbc.Database, error) {
	return NewDatabase(opts)
}

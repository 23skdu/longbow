package adbc

import (
	"context"

	"github.com/apache/arrow-adbc/go/adbc"
)

type Database struct {
	opts map[string]string
}

func NewDatabase(opts map[string]string) (adbc.Database, error) {
	if opts == nil {
		opts = make(map[string]string)
	}
	return &Database{opts: opts}, nil
}

func (d *Database) SetOptions(opts map[string]string) error {
	for k, v := range opts {
		d.opts[k] = v
	}
	return nil
}

func (d *Database) SetOption(key, value string) error {
	d.opts[key] = value
	return nil
}

func (d *Database) Open(ctx context.Context) (adbc.Connection, error) {
	return NewConnection(d), nil
}

func (d *Database) Close() error {
	return nil
}

func (d *Database) GetOption(key string) (string, error) {
	if val, ok := d.opts[key]; ok {
		return val, nil
	}
	return "", adbc.Error{Code: adbc.StatusNotFound}
}

func (d *Database) GetOptionBytes(key string) ([]byte, error) {
	return nil, adbc.Error{Code: adbc.StatusNotImplemented}
}

func (d *Database) GetOptionInt(key string) (int64, error) {
	return 0, adbc.Error{Code: adbc.StatusNotImplemented}
}

func (d *Database) GetOptionDouble(key string) (float64, error) {
	return 0, adbc.Error{Code: adbc.StatusNotImplemented}
}

func (d *Database) SetOptionBytes(key string, value []byte) error {
	return adbc.Error{Code: adbc.StatusNotImplemented}
}

func (d *Database) SetOptionInt(key string, value int64) error {
	return adbc.Error{Code: adbc.StatusNotImplemented}
}

func (d *Database) SetOptionDouble(key string, value float64) error {
	return adbc.Error{Code: adbc.StatusNotImplemented}
}

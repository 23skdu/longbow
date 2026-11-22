package adbc

import (
	"context"
	"fmt"

	"github.com/23skdu/longbow/internal/store"
	"github.com/apache/arrow-adbc/go/adbc"
	"github.com/apache/arrow-go/v18/arrow/memory"
	"github.com/rs/zerolog"
)

const (
	// OptionKeyStore is a option key store constant.
	OptionKeyStore = "longbow.store"
)

// Database represents a database value.
type Database struct {
	opts   map[string]string
	store  *store.VectorStore
	logger zerolog.Logger
}

// NewDatabase constructs a new Database.
func NewDatabase(opts map[string]string) (adbc.Database, error) {
	if opts == nil {
		opts = make(map[string]string)
	}
	return &Database{opts: opts, logger: zerolog.Nop()}, nil
}

// SetOptions sets the options.
func (d *Database) SetOptions(opts map[string]string) error {
	for k, v := range opts {
		d.opts[k] = v
	}
	return nil
}

// SetOption sets the option.
func (d *Database) SetOption(key, value string) error {
	d.opts[key] = value
	return nil
}

// Open runs the open operation.
func (d *Database) Open(ctx context.Context) (adbc.Connection, error) {
	return newConnection(d), nil
}

// Close runs the close operation.
func (d *Database) Close() error {
	if d.store != nil {
		err := d.store.Close()
		d.store = nil
		return err
	}
	return nil
}

// GetOption returns the option.
func (d *Database) GetOption(key string) (string, error) {
	if val, ok := d.opts[key]; ok {
		return val, nil
	}
	return "", adbc.Error{Code: adbc.StatusNotFound}
}

// GetOptionBytes returns the option bytes.
func (d *Database) GetOptionBytes(key string) ([]byte, error) {
	return nil, adbc.Error{Code: adbc.StatusNotImplemented}
}

// GetOptionInt returns the option int.
func (d *Database) GetOptionInt(key string) (int64, error) {
	return 0, adbc.Error{Code: adbc.StatusNotImplemented}
}

// GetOptionDouble returns the option double.
func (d *Database) GetOptionDouble(key string) (float64, error) {
	return 0, adbc.Error{Code: adbc.StatusNotImplemented}
}

// SetOptionBytes sets the option bytes.
func (d *Database) SetOptionBytes(key string, value []byte) error {
	return adbc.Error{Code: adbc.StatusNotImplemented}
}

// SetOptionInt sets the option int.
func (d *Database) SetOptionInt(key string, value int64) error {
	return adbc.Error{Code: adbc.StatusNotImplemented}
}

// SetOptionDouble sets the option double.
func (d *Database) SetOptionDouble(key string, value float64) error {
	return adbc.Error{Code: adbc.StatusNotImplemented}
}

// SetVectorStore sets the vector store.
func (d *Database) SetVectorStore(vs *store.VectorStore) {
	d.store = vs
}

// SetLogger sets the logger.
func (d *Database) SetLogger(logger zerolog.Logger) {
	d.logger = logger
}

// VectorStore runs the vector store operation.
func (d *Database) VectorStore() *store.VectorStore {
	return d.store
}

// NewDatabaseWithStore constructs a new DatabaseWithStore.
func NewDatabaseWithStore(vs *store.VectorStore, logger zerolog.Logger) adbc.Database {
	return &Database{
		opts:   make(map[string]string),
		store:  vs,
		logger: logger,
	}
}

func (d *Database) initStore() *store.VectorStore {
	if d.store != nil {
		return d.store
	}
	d.logger.Info().Msg("ADBC: no VectorStore attached, creating in-memory store")
	d.store = store.NewVectorStore(
		memory.NewGoAllocator(),
		d.logger,
		256*1024*1024,
		0, 0,
	)
	return d.store
}

// String runs the string operation.
func (d *Database) String() string {
	if d.store != nil {
		return "adbc.Database{store=attached}"
	}
	return "adbc.Database{store=nil}"
}

var _ fmt.Stringer = (*Database)(nil)

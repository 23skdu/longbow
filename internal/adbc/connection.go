package adbc

import (
	"context"

	"github.com/apache/arrow-adbc/go/adbc"
	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
)

type Connection struct {
	db *Database
}

func NewConnection(db *Database) adbc.Connection {
	return &Connection{db: db}
}

func (c *Connection) Close() error {
	return nil
}

func (c *Connection) SetOption(key, value string) error {
	return adbc.Error{Code: adbc.StatusNotImplemented}
}

func (c *Connection) NewStatement() (adbc.Statement, error) {
	return NewStatement(c), nil
}

func (c *Connection) GetInfo(ctx context.Context, infoCodes []adbc.InfoCode) (array.RecordReader, error) {
	return nil, adbc.Error{Code: adbc.StatusNotImplemented}
}

func (c *Connection) GetObjects(ctx context.Context, depth adbc.ObjectDepth, catalog, dbSchema, tableName, columnName *string, tableType []string) (array.RecordReader, error) {
	return nil, adbc.Error{Code: adbc.StatusNotImplemented}
}

func (c *Connection) GetTableSchema(ctx context.Context, catalog, dbSchema *string, tableName string) (*arrow.Schema, error) {
	return nil, adbc.Error{Code: adbc.StatusNotImplemented}
}

func (c *Connection) GetTableTypes(ctx context.Context) (array.RecordReader, error) {
	return nil, adbc.Error{Code: adbc.StatusNotImplemented}
}

func (c *Connection) ReadPartition(ctx context.Context, serializedPartition []byte) (array.RecordReader, error) {
	return nil, adbc.Error{Code: adbc.StatusNotImplemented}
}

func (c *Connection) Commit(ctx context.Context) error {
	return adbc.Error{Code: adbc.StatusNotImplemented}
}

func (c *Connection) Rollback(ctx context.Context) error {
	return adbc.Error{Code: adbc.StatusNotImplemented}
}

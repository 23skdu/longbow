package adbc_test

import (
	"testing"

	longbowadbc "github.com/23skdu/longbow/internal/adbc"
)

// TestDriver_NewDriver runs the test driver_new driver operation.
func TestDriver_NewDriver(t *testing.T) {
	d := longbowadbc.NewDriver()
	if d == nil {
		t.Fatal("NewDriver: got nil")
	}
}

// TestDriver_NewDatabase runs the test driver_new database operation.
func TestDriver_NewDatabase(t *testing.T) {
	d := longbowadbc.NewDriver()
	if d == nil {
		t.Fatal("NewDriver: got nil")
	}
	if _, ok := d.(*longbowadbc.Driver); !ok {
		t.Errorf("NewDriver: expected *longbowadbc.Driver, got %T", d)
	}

	db, err := d.NewDatabase(nil)
	if err != nil {
		t.Fatalf("NewDatabase: %v", err)
	}
	if db == nil {
		t.Fatal("NewDatabase: got nil")
	}
	if err := db.Close(); err != nil {
		t.Errorf("db.Close: %v", err)
	}
}

// TestDriver_NewDatabase_WithOpts runs the test driver_new database_with opts operation.
func TestDriver_NewDatabase_WithOpts(t *testing.T) {
	d := longbowadbc.NewDriver()
	db, err := d.NewDatabase(map[string]string{"path": "/tmp/longbow"})
	if err != nil {
		t.Fatalf("NewDatabase: %v", err)
	}
	if db == nil {
		t.Fatal("NewDatabase: got nil")
	}
	if err := db.Close(); err != nil {
		t.Errorf("db.Close: %v", err)
	}
}

package adbc_test

import (
	"context"
	"testing"

	"github.com/apache/arrow-adbc/go/adbc"

	longbowadbc "github.com/23skdu/longbow/internal/adbc"
)

func assertNotFound(t *testing.T, err error, ctx string) {
	t.Helper()
	if err == nil {
		t.Fatalf("%s: expected error, got nil", ctx)
	}
	adbcErr, ok := err.(adbc.Error)
	if !ok {
		t.Fatalf("%s: expected adbc.Error, got %T (%v)", ctx, err, err)
	}
	if adbcErr.Code != adbc.StatusNotFound {
		t.Fatalf("%s: expected StatusNotFound (%d), got %d (%v)",
			ctx, adbc.StatusNotFound, adbcErr.Code, adbcErr)
	}
}

func newTestDatabase(t *testing.T, opts map[string]string) adbc.Database {
	t.Helper()
	driver := longbowadbc.NewDriver()
	db, err := driver.NewDatabase(opts)
	if err != nil {
		t.Fatalf("NewDatabase: %v", err)
	}
	t.Cleanup(func() { _ = db.Close() })
	return db
}

// concrete returns the *longbowadbc.Database. Some methods (GetOption,
// SetOption, GetOptionBytes, etc.) are defined on the concrete type but
// are not part of the adbc.Database interface in Apache Arrow ADBC Go 18.
// We use the concrete type to exercise those methods and lock in the
// StatusNotImplemented / StatusNotFound contract.
func concrete(t *testing.T, db adbc.Database) *longbowadbc.Database {
	t.Helper()
	c, ok := db.(*longbowadbc.Database)
	if !ok {
		t.Fatalf("expected *longbowadbc.Database, got %T", db)
	}
	return c
}

func TestDatabase_NewDatabase_NilOpts(t *testing.T) {
	db, err := longbowadbc.NewDriver().NewDatabase(nil)
	if err != nil {
		t.Fatalf("NewDatabase(nil): %v", err)
	}
	if db == nil {
		t.Fatal("NewDatabase(nil): got nil database")
	}
	if err := db.Close(); err != nil {
		t.Errorf("Close: %v", err)
	}
}

func TestDatabase_NewDatabase_WithOpts(t *testing.T) {
	db, err := longbowadbc.NewDriver().NewDatabase(map[string]string{"key": "value"})
	if err != nil {
		t.Fatalf("NewDatabase: %v", err)
	}
	if db == nil {
		t.Fatal("NewDatabase: got nil database")
	}
	if err := db.Close(); err != nil {
		t.Errorf("Close: %v", err)
	}
}

func TestDatabase_SetOptions(t *testing.T) {
	db := newTestDatabase(t, map[string]string{"existing": "old"})
	if err := db.SetOptions(map[string]string{"existing": "new", "added": "yes"}); err != nil {
		t.Fatalf("SetOptions: %v", err)
	}
	got, err := concrete(t, db).GetOption("existing")
	if err != nil {
		t.Fatalf("GetOption(existing): %v", err)
	}
	if got != "new" {
		t.Errorf("GetOption(existing) = %q, want %q", got, "new")
	}
	got, err = concrete(t, db).GetOption("added")
	if err != nil {
		t.Fatalf("GetOption(added): %v", err)
	}
	if got != "yes" {
		t.Errorf("GetOption(added) = %q, want %q", got, "yes")
	}
}

func TestDatabase_SetOption(t *testing.T) {
	db := newTestDatabase(t, nil)
	cdb := concrete(t, db)
	if err := cdb.SetOption("k", "v"); err != nil {
		t.Fatalf("SetOption: %v", err)
	}
	got, err := cdb.GetOption("k")
	if err != nil {
		t.Fatalf("GetOption(k): %v", err)
	}
	if got != "v" {
		t.Errorf("GetOption(k) = %q, want %q", got, "v")
	}
}

func TestDatabase_Open(t *testing.T) {
	db := newTestDatabase(t, nil)
	conn, err := db.Open(context.Background())
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	if conn == nil {
		t.Fatal("Open: got nil connection")
	}
	if err := conn.Close(); err != nil {
		t.Errorf("conn.Close: %v", err)
	}
}

func TestDatabase_Close(t *testing.T) {
	db := newTestDatabase(t, nil)
	if err := db.Close(); err != nil {
		t.Errorf("Close: %v", err)
	}
}

func TestDatabase_GetOption_Found(t *testing.T) {
	db := newTestDatabase(t, map[string]string{"present": "yes"})
	got, err := concrete(t, db).GetOption("present")
	if err != nil {
		t.Fatalf("GetOption: %v", err)
	}
	if got != "yes" {
		t.Errorf("GetOption(present) = %q, want %q", got, "yes")
	}
}

func TestDatabase_GetOption_NotFound(t *testing.T) {
	db := newTestDatabase(t, map[string]string{"present": "yes"})
	_, err := concrete(t, db).GetOption("missing")
	assertNotFound(t, err, "GetOption(missing)")
}

func TestDatabase_GetOptionBytes_NotImplemented(t *testing.T) {
	db := newTestDatabase(t, nil)
	val, err := concrete(t, db).GetOptionBytes("k")
	assertNotImplemented(t, err, "GetOptionBytes")
	if val != nil {
		t.Errorf("GetOptionBytes: expected nil, got %v", val)
	}
}

func TestDatabase_GetOptionInt_NotImplemented(t *testing.T) {
	db := newTestDatabase(t, nil)
	val, err := concrete(t, db).GetOptionInt("k")
	assertNotImplemented(t, err, "GetOptionInt")
	if val != 0 {
		t.Errorf("GetOptionInt: expected 0, got %d", val)
	}
}

func TestDatabase_GetOptionDouble_NotImplemented(t *testing.T) {
	db := newTestDatabase(t, nil)
	val, err := concrete(t, db).GetOptionDouble("k")
	assertNotImplemented(t, err, "GetOptionDouble")
	if val != 0 {
		t.Errorf("GetOptionDouble: expected 0, got %f", val)
	}
}

func TestDatabase_SetOptionBytes_NotImplemented(t *testing.T) {
	db := newTestDatabase(t, nil)
	assertNotImplemented(t, concrete(t, db).SetOptionBytes("k", []byte("v")), "SetOptionBytes")
}

func TestDatabase_SetOptionInt_NotImplemented(t *testing.T) {
	db := newTestDatabase(t, nil)
	assertNotImplemented(t, concrete(t, db).SetOptionInt("k", 42), "SetOptionInt")
}

func TestDatabase_SetOptionDouble_NotImplemented(t *testing.T) {
	db := newTestDatabase(t, nil)
	assertNotImplemented(t, concrete(t, db).SetOptionDouble("k", 3.14), "SetOptionDouble")
}

package storage

import (
	"context"
	"os"
	"testing"
)

func TestGCSBackendKeyGeneration(t *testing.T) {
	tests := []struct {
		prefix   string
		name     string
		ext      string
		expected string
	}{
		{"", "col1", ".parquet", "snapshots/col1.parquet"},
		{"prod", "col1", ".parquet", "prod/snapshots/col1.parquet"},
		{"prod/data", "my-data", ".arrow", "prod/data/snapshots/my-data.arrow"},
		{"trailing/", "test", ".pq", "trailing/snapshots/test.pq"},
	}

	for _, tt := range tests {
		t.Run(tt.prefix+"/"+tt.name, func(t *testing.T) {
			key := buildGCSKeyWithExt(tt.prefix, tt.name, tt.ext)
			if key != tt.expected {
				t.Errorf("buildGCSKeyWithExt(%q, %q, %q) = %q, want %q", tt.prefix, tt.name, tt.ext, key, tt.expected)
			}
		})
	}
}

func FuzzGCSKeyGeneration(f *testing.F) {
	f.Add("prefix", "name", ".ext")
	f.Fuzz(func(t *testing.T, prefix, name, ext string) {
		key := buildGCSKeyWithExt(prefix, name, ext)
		if key == "" {
			t.Errorf("key should not be empty")
		}
	})
}

func TestGCSBackendIntegration(t *testing.T) {
	if os.Getenv("GCS_TEST_BUCKET") == "" {
		t.Skip("Skipping GCS integration test: GCS_TEST_BUCKET not set")
	}

	ctx := context.Background()
	cfg := &GCSBackendConfig{
		Bucket: os.Getenv("GCS_TEST_BUCKET"),
		Prefix: os.Getenv("GCS_TEST_PREFIX"),
	}

	backend, err := NewGCSBackend(ctx, cfg)
	if err != nil {
		t.Fatalf("Failed to create GCSBackend: %v", err)
	}

	testName := "integration_test"
	testData := []byte("gcs snapshot test data")

	// Write
	err = backend.WriteSnapshot(ctx, testName, testData)
	if err != nil {
		t.Fatalf("WriteSnapshot failed: %v", err)
	}

	// List
	listed, err := backend.ListSnapshots(ctx)
	if err != nil {
		t.Fatalf("ListSnapshots failed: %v", err)
	}
	found := false
	for _, n := range listed {
		if n == testName {
			found = true
			break
		}
	}
	if !found {
		t.Errorf("Snapshot %s not found in list %v", testName, listed)
	}

	// Read
	rc, err := backend.ReadSnapshot(ctx, testName)
	if err != nil {
		t.Fatalf("ReadSnapshot failed: %v", err)
	}
	defer rc.Close()

	// Delete
	err = backend.DeleteSnapshot(ctx, testName)
	if err != nil {
		t.Fatalf("DeleteSnapshot failed: %v", err)
	}
}

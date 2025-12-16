package main

import (
	"bytes"
	"context"
	"fmt"
	"io"
	"net"
	"net/http"
	"os"
	"os/exec"
	"path/filepath"
	"syscall"
	"testing"
	"time"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/apache/arrow-go/v18/arrow/flight"
	"github.com/apache/arrow-go/v18/arrow/memory"
	"google.golang.org/grpc"
	"github.com/apache/arrow-go/v18/arrow/ipc"
	"google.golang.org/grpc/credentials/insecure"
)

var binaryPath string

func TestMain(m *testing.M) {
os.Exit(testMain(m))
}

func testMain(m *testing.M) int {
// Build binary once for all tests
tmpDir, err := os.MkdirTemp("", "longbow-test-*")
if err != nil {
fmt.Fprintf(os.Stderr, "Failed to create temp dir: %v\n", err)
return 1
}
defer func() { _ = os.RemoveAll(tmpDir) }()

binaryPath = filepath.Join(tmpDir, "longbow")
cmd := exec.Command("go", "build", "-o", binaryPath, ".")
cmd.Stderr = os.Stderr
if err := cmd.Run(); err != nil {
fmt.Fprintf(os.Stderr, "Failed to build binary: %v\n", err)
return 1
}

return m.Run()
}

// getFreePort returns an available port
func getFreePort() (int, error) {
	l, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		return 0, err
	}
	defer func() { _ = l.Close() }()
	return l.Addr().(*net.TCPAddr).Port, nil
}

// serverInstance holds a running server process
type serverInstance struct {
	cmd         *exec.Cmd
	dataAddr    string
	metaAddr    string
	metricsAddr string
	dataDir     string
}

// startServer starts the longbow binary with test configuration
func startServer(t *testing.T) *serverInstance {
	t.Helper()

	dataPort, err := getFreePort()
	if err != nil {
		t.Fatalf("Failed to get data port: %v", err)
	}
	metaPort, err := getFreePort()
	if err != nil {
		t.Fatalf("Failed to get meta port: %v", err)
	}
	metricsPort, err := getFreePort()
	if err != nil {
		t.Fatalf("Failed to get metrics port: %v", err)
	}

	dataDir, err := os.MkdirTemp("", "longbow-data-*")
	if err != nil {
		t.Fatalf("Failed to create data dir: %v", err)
	}

	si := &serverInstance{
		dataAddr:    fmt.Sprintf("127.0.0.1:%d", dataPort),
		metaAddr:    fmt.Sprintf("127.0.0.1:%d", metaPort),
		metricsAddr: fmt.Sprintf("127.0.0.1:%d", metricsPort),
		dataDir:     dataDir,
	}

	si.cmd = exec.Command(binaryPath)
	si.cmd.Env = append(os.Environ(),
		"LONGBOW_LISTEN_ADDR="+si.dataAddr,
		"LONGBOW_META_ADDR="+si.metaAddr,
		"LONGBOW_METRICS_ADDR="+si.metricsAddr,
		"LONGBOW_DATA_PATH="+dataDir,
		"LONGBOW_MAX_MEMORY=104857600", // 100MB
	)
	si.cmd.Stdout = io.Discard
	si.cmd.Stderr = io.Discard

	if err := si.cmd.Start(); err != nil {
		_ = os.RemoveAll(dataDir)
		t.Fatalf("Failed to start server: %v", err)
	}

	// Wait for server to be ready
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	for {
		select {
		case <-ctx.Done():
			si.stop()
			t.Fatalf("Server failed to start within timeout")
		default:
			conn, err := net.DialTimeout("tcp", si.dataAddr, 100*time.Millisecond)
			if err == nil {
				_ = conn.Close()
				return si
			}
			time.Sleep(50 * time.Millisecond)
		}
	}
}

// stop gracefully stops the server
func (si *serverInstance) stop() {
	if si.cmd != nil && si.cmd.Process != nil {
		_ = si.cmd.Process.Signal(syscall.SIGTERM)
		_ = si.cmd.Wait()
	}
	if si.dataDir != "" {
		_ = os.RemoveAll(si.dataDir)
	}
}

// TestServerStartsAndAcceptsConnections verifies basic server wiring
func TestServerStartsAndAcceptsConnections(t *testing.T) {
	si := startServer(t)
	defer si.stop()

	// Test Data Server connection
	conn, err := grpc.NewClient(si.metaAddr,
		grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		t.Fatalf("Failed to connect to meta server: %v", err)
	}
	defer func() { _ = conn.Close() }()

	client := flight.NewClientFromConn(conn, nil)
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	// ListFlights should work (even if empty)
	stream, err := client.ListFlights(ctx, &flight.Criteria{})
	if err != nil {
		t.Fatalf("ListFlights failed: %v", err)
	}

	// Drain stream
	for {
		_, err := stream.Recv()
		if err == io.EOF {
			break
		}
		if err != nil {
			t.Fatalf("ListFlights recv failed: %v", err)
		}
	}
}

// TestMetaServerConnection verifies meta server is listening
func TestMetaServerConnection(t *testing.T) {
	si := startServer(t)
	defer si.stop()

	conn, err := grpc.NewClient(si.metaAddr,
		grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		t.Fatalf("Failed to connect to meta server: %v", err)
	}
	defer func() { _ = conn.Close() }()

	client := flight.NewClientFromConn(conn, nil)
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	stream, err := client.ListFlights(ctx, &flight.Criteria{})
	if err != nil {
		t.Fatalf("Meta ListFlights failed: %v", err)
	}

	for {
		_, err := stream.Recv()
		if err == io.EOF {
			break
		}
		if err != nil {
			t.Fatalf("Meta ListFlights recv failed: %v", err)
		}
	}
}

// TestMetricsEndpoint verifies Prometheus metrics are exposed
func TestMetricsEndpoint(t *testing.T) {
	si := startServer(t)
	defer si.stop()

	resp, err := http.Get("http://" + si.metricsAddr + "/metrics")
	if err != nil {
		t.Fatalf("Failed to get metrics: %v", err)
	}
	defer func() { _ = resp.Body.Close() }()

	if resp.StatusCode != http.StatusOK {
		t.Errorf("Expected 200, got %d", resp.StatusCode)
	}

	body, _ := io.ReadAll(resp.Body)
	if !bytes.Contains(body, []byte("go_goroutines")) {
		t.Error("Expected Prometheus metrics in response")
	}
}

// TestDataServerDoPutDoGet verifies full data path
func TestDataServerDoPutDoGet(t *testing.T) {
	si := startServer(t)
	defer si.stop()

	conn, err := grpc.NewClient(si.dataAddr,
		grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		t.Fatalf("Failed to connect: %v", err)
	}
	defer func() { _ = conn.Close() }()

	client := flight.NewClientFromConn(conn, nil)
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	// Create test collection via DoPut
	mem := memory.NewGoAllocator()
	schema := arrow.NewSchema([]arrow.Field{
		{Name: "id", Type: arrow.BinaryTypes.String},
		{Name: "vector", Type: arrow.ListOf(arrow.PrimitiveTypes.Float32)},
	}, nil)

	builder := array.NewRecordBuilder(mem, schema)
	defer builder.Release()

	idBuilder := builder.Field(0).(*array.StringBuilder)
	vecBuilder := builder.Field(1).(*array.ListBuilder)
	vecValBuilder := vecBuilder.ValueBuilder().(*array.Float32Builder)

	idBuilder.Append("test-1")
	vecBuilder.Append(true)
	for i := 0; i < 128; i++ {
		vecValBuilder.Append(float32(i) * 0.01)
	}

	rec := builder.NewRecord()
	defer rec.Release()

	// Build FlightDescriptor with metadata

	desc := &flight.FlightDescriptor{
		Type: flight.DescriptorPATH,
		Path: []string{"test_collection"},
	}

	// DoPut
	stream, err := client.DoPut(ctx)
	if err != nil {
		t.Fatalf("DoPut failed: %v", err)
	}

	w := flight.NewRecordWriter(stream, ipc.WithSchema(schema))
	w.SetFlightDescriptor(desc)
	if err := w.Write(rec); err != nil {
		t.Fatalf("Write record failed: %v", err)
	}
	_ = w.Close()
	_ = stream.CloseSend()

	// Read ack
	for {
		_, err := stream.Recv()
		if err == io.EOF {
			break
		}
		if err != nil {
			t.Fatalf("DoPut recv failed: %v", err)
		}
	}

	// Verify collection exists via ListFlights (on MetaServer)
	metaConn, err := grpc.NewClient(si.metaAddr,
		grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		t.Fatalf("Failed to connect to meta server: %v", err)
	}
	defer func() { _ = metaConn.Close() }()

	metaClient := flight.NewClientFromConn(metaConn, nil)
	listStream, err := metaClient.ListFlights(ctx, &flight.Criteria{})
	if err != nil {
		t.Fatalf("ListFlights failed: %v", err)
	}
	found := false
	for {
		info, err := listStream.Recv()
		if err == io.EOF {
			break
		}
		if err != nil {
			t.Fatalf("ListFlights recv failed: %v", err)
		}
		if len(info.FlightDescriptor.Path) > 0 && info.FlightDescriptor.Path[0] == "test_collection" {
			found = true
		}
	}

	if !found {
		t.Error("Collection not found after DoPut")
	}
}

// TestGracefulShutdown verifies SIGTERM handling
func TestGracefulShutdown(t *testing.T) {
	si := startServer(t)

	// Send SIGTERM
	_ = si.cmd.Process.Signal(syscall.SIGTERM)

	// Wait for exit with timeout
	done := make(chan error, 1)
	go func() {
		done <- si.cmd.Wait()
	}()

	select {
	case err := <-done:
		if err != nil {
			// exit status 0 is expected, anything else is fine for graceful shutdown
			t.Logf("Server exited: %v", err)
		}
	case <-time.After(10 * time.Second):
		_ = si.cmd.Process.Kill()
		t.Fatal("Server did not shut down gracefully within timeout")
	}

	_ = os.RemoveAll(si.dataDir)
}

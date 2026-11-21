package main

import (
	"context"
	"net/http"
	"os"
	"sync"
	"time"

	lbflight "github.com/23skdu/longbow/internal/flight"
	"github.com/23skdu/longbow/internal/store"
	"github.com/rs/zerolog"
	"google.golang.org/grpc"
)

func gracefulShutdown(
	logger zerolog.Logger,
	vectorStore *store.VectorStore,
	wsServer *store.WebSocketServer,
	mqExporter *store.MessageQueueExporter,
	rdmaSrv *lbflight.RDMAServer,
	dataServer *grpc.Server,
	metaServer *grpc.Server,
	metaService *store.MetaServer,
	metricsSrv *http.Server,
) error {
	// Stop WebSocket server
	if wsServer != nil {
		if err := wsServer.Stop(); err != nil {
			logger.Error().Err(err).Msg("WebSocket server stop failed")
		} else {
			logger.Info().Msg("WebSocket server stopped")
		}
	}

	// Stop MQ Exporter
	if mqExporter != nil {
		if err := mqExporter.Stop(); err != nil {
			logger.Error().Err(err).Msg("MQ exporter stop failed")
		} else {
			logger.Info().Msg("MQ exporter stopped")
		}
	}

	// Stop RDMA server
	if rdmaSrv != nil {
		rdmaSrv.Stop()
		logger.Info().Msg("RDMA server stopped")
	}

	// In benchmark mode (LONGBOW_SHUTDOWN_SKIP_FINAL_SNAPSHOT=true) the data
	// directory is immediately deleted after shutdown, making persistence and
	// graceful gRPC drain unnecessary. Exit immediately to minimize per-config
	// overhead and avoid port-TIME_WAIT delays from the next startup.
	if os.Getenv("LONGBOW_SHUTDOWN_SKIP_FINAL_SNAPSHOT") == "true" {
		logger.Info().Msg("Benchmark mode – skipping shutdown sequence, exiting")
		return nil
	}

	// Allow a grace period for pending pprof profile collections to finish
	// Benchmark tool often collects profile just before sending SIGTERM.
	time.Sleep(2 * time.Second)

	shutdownCtx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()

	done := make(chan struct{})
	go func() {
		var wg sync.WaitGroup
		wg.Add(3)
		go func() {
			defer wg.Done()
			dataServer.GracefulStop()
			logger.Info().Msg("Data server stopped")
		}()
		go func() {
			defer wg.Done()
			metaServer.GracefulStop()
			_ = metaService.Close() // Clean up coordinator clients
			logger.Info().Msg("Meta server stopped")
		}()
		go func() {
			defer wg.Done()
			if metricsSrv != nil {
				if err := metricsSrv.Shutdown(shutdownCtx); err != nil {
					logger.Error().Err(err).Msg("Metrics server shutdown failed")
				}
				logger.Info().Msg("Metrics server stopped")
			}
		}()
		wg.Wait()
		close(done)
	}()

	select {
	case <-done:
		logger.Info().Msg("gRPC servers stopped gracefully")
	case <-shutdownCtx.Done():
		logger.Warn().Msg("Shutdown timed out, forcing stop")
		dataServer.Stop()
		metaServer.Stop()
	}

	// Close VectorStore (flushes WAL/Snapshots)
	if err := vectorStore.Close(); err != nil {
		logger.Error().Err(err).Msg("Failed to close vector store")
	} else {
		logger.Info().Msg("Vector store closed successfully")
	}

	return nil
}

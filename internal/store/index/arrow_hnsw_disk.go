package index

import (
	"fmt"
	"log/slog"
	"os"
	"path/filepath"
)

// GetDiskGraph returns the disk graph if enabled
func (h *ArrowHNSW) GetDiskGraph() *DiskGraph {
	return h.diskGraph.Load()
}

// SetDiskGraph sets the disk graph
func (h *ArrowHNSW) SetDiskGraph(disk *DiskGraph) {
	h.diskGraph.Store(disk)
}

// maybeFlushToDisk flushes the in-memory graph to disk if UseDisk is enabled
// and the node count has grown significantly since the last flush.
func (h *ArrowHNSW) maybeFlushToDisk() {
	if !h.config.UseDisk {
		return
	}
	nc := h.nodeCount.Load()
	if nc < 5000 {
		return
	}
	doFlush := !h.diskFlushed.Load()
	if !doFlush {
		lastFlushNC := h.lastFlushNC.Load()
		doFlush = lastFlushNC == 0 || (nc-lastFlushNC)*5 > lastFlushNC // 20% growth
	}
	if doFlush {
		if err := h.FlushToDisk(); err != nil {
			slog.Error("FlushToDisk failed", "index", h.name, "error", err)
		} else {
			h.lastFlushNC.Store(nc)
		}
	}
}

// FlushToDisk writes the current in-memory graph to disk and opens it as a
// read-only backing store. Subsequent neighbor lookups can fall through to
// the disk file when in-memory data is evicted. Only effective when
// config.UseDisk is true and config.DiskPath is set.
func (h *ArrowHNSW) FlushToDisk() error {
	if !h.config.UseDisk || h.config.DiskPath == "" {
		return nil
	}

	gd := h.data.Load()
	if gd == nil {
		return nil
	}

	meta := h.metadataRegistry.Load()
	if meta == nil {
		return nil
	}

	path := filepath.Join(h.config.DiskPath, h.name+"_graph.bin")
	if err := os.MkdirAll(filepath.Dir(path), 0750); err != nil {
		return fmt.Errorf("create disk graph directory: %w", err)
	}

	nc := h.nodeCount.Load()
	if nc <= 0 {
		return nil // No data to write
	}

	// The WriteDiskGraph function expects maxNodeID as the highest node ID + 1
	if err := WriteDiskGraph(gd, path, int(nc), 0, 0, meta.EntryPoint, int(meta.MaxLevel)); err != nil {
		return fmt.Errorf("write disk graph: %w", err)
	}

	// Open the written file as a read-only DiskGraph
	dg, err := NewDiskGraph(path)
	if err != nil {
		return fmt.Errorf("open disk graph: %w", err)
	}

	h.SetDiskGraph(dg)
	gd.BackingGraph = dg
	h.diskFlushed.Store(true)
	return nil
}

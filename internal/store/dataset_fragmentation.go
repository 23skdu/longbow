package store

import (
	"fmt"
)

// DiskLayoutInfo represents the fragmentation status of a dataset's batches.
type DiskLayoutInfo struct {
	TotalBatches   int
	TotalRows      int
	TotalDeleted   int
	BatchDetails   []BatchLayoutDetail
	ReadAmpAverage float64
}

type BatchLayoutDetail struct {
	BatchIdx int
	Rows     int
	Deleted  int
	Density  float64
	ReadAmp  float64
	Hits     int64
}

// GetDiskLayoutInfo returns diagnostic information about batch fragmentation.
func (d *Dataset) GetDiskLayoutInfo() DiskLayoutInfo {
	d.dataMu.RLock()
	defer d.dataMu.RUnlock()

	info := DiskLayoutInfo{
		TotalBatches: len(d.Records),
		BatchDetails: make([]BatchLayoutDetail, 0, len(d.Records)),
	}

	totalReadAmp := 0.0

	for i, rec := range d.Records {
		rows := int(rec.NumRows())
		deleted := 0
		if d.Tombstones[i] != nil {
			deleted = int(d.Tombstones[i].Count()) // #nosec G115
		}

		density := 0.0
		if rows > 0 {
			density = float64(deleted) / float64(rows)
		}

		readAmp := 1.0
		if density < 1.0 {
			readAmp = 1.0 / (1.0 - density)
		}

		hits := int64(0)
		if d.fragmentationTracker != nil {
			hits = d.fragmentationTracker.getOrCreateBatch(i).hits.Load()
		}

		detail := BatchLayoutDetail{
			BatchIdx: i,
			Rows:     rows,
			Deleted:  deleted,
			Density:  density,
			ReadAmp:  readAmp,
			Hits:     hits,
		}

		info.BatchDetails = append(info.BatchDetails, detail)
		info.TotalRows += rows
		info.TotalDeleted += deleted
		totalReadAmp += readAmp
	}

	if len(d.Records) > 0 {
		info.ReadAmpAverage = totalReadAmp / float64(len(d.Records))
	}

	return info
}

// VisualizeLayout returns a string representation of the fragmentation.
func (d *Dataset) VisualizeLayout() string {
	info := d.GetDiskLayoutInfo()

	result := fmt.Sprintf("Dataset: %s\n", d.Name)
	result += fmt.Sprintf("Total Rows: %d (Deleted: %d)\n", info.TotalRows, info.TotalDeleted)
	result += fmt.Sprintf("Avg Read Amp: %.2fx\n\n", info.ReadAmpAverage)
	result += "Batch | Rows | Del | Density | ReadAmp | Hits | Visualize\n"
	result += "------|------|-----|---------|---------|------|----------\n"

	for _, det := range info.BatchDetails {
		// Create a small progress bar for density
		barLen := 10
		filled := int(det.Density * float64(barLen))
		bar := ""
		for i := 0; i < barLen; i++ {
			if i < filled {
				bar += "█"
			} else {
				bar += "░"
			}
		}

		result += fmt.Sprintf("%5d | %4d | %3d | %6.1f%% | %6.2fx | %4d | %s\n",
			det.BatchIdx, det.Rows, det.Deleted, det.Density*100, det.ReadAmp, det.Hits, bar)
	}

	return result
}

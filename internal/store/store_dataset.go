package store

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/23skdu/longbow/internal/storage"
)

type NamespaceMigrationConfig struct {
	SourceNamespace string
	TargetNamespace string
	TargetNode      string
	Datasets        []string
	CopyMode        bool
}

type NamespaceMigrationResult struct {
	Success          bool
	MigratedDatasets int
	FailedDatasets   []string
	Duration         time.Duration
}

func (vs *VectorStore) MigrateNamespace(config NamespaceMigrationConfig) (*NamespaceMigrationResult, error) {
	startTime := time.Now()
	result := &NamespaceMigrationResult{
		FailedDatasets: make([]string, 0),
	}

	sourceNS := vs.GetNamespace(config.SourceNamespace)
	if sourceNS == nil {
		return result, errors.New("source namespace not found")
	}

	targetNS := vs.GetNamespace(config.TargetNamespace)
	if targetNS == nil {
		if err := vs.CreateNamespace(config.TargetNamespace); err != nil {
			return result, fmt.Errorf("failed to create target namespace: %w", err)
		}
		targetNS = vs.GetNamespace(config.TargetNamespace)
	}

	datasets := sourceNS.ListDatasets()
	if len(config.Datasets) > 0 {
		var filtered []string
		for _, ds := range datasets {
			for _, wanted := range config.Datasets {
				if ds == wanted {
					filtered = append(filtered, ds)
				}
			}
		}
		datasets = filtered
	}

	for _, dataset := range datasets {
		newName := config.TargetNamespace + "/" + dataset[len(config.SourceNamespace)+1:]

		if config.CopyMode {
			if vs.engine != nil && vs.engine.GetSnapshotBackend() != nil {
				if err := vs.CloneDataset(context.Background(), dataset, newName, vs.engine.GetSnapshotBackend()); err != nil {
					result.FailedDatasets = append(result.FailedDatasets, dataset)
					continue
				}
			} else {
				result.FailedDatasets = append(result.FailedDatasets, dataset)
				continue
			}
		} else {
			if vs.engine != nil && vs.engine.GetSnapshotBackend() != nil {
				_, err := vs.ExportDataset(dataset, vs.engine.GetSnapshotBackend())
				if err != nil {
					result.FailedDatasets = append(result.FailedDatasets, dataset)
					continue
				}

				_, err = vs.ImportDataset(context.Background(), newName, vs.engine.GetSnapshotBackend(), nil)
				if err != nil {
					result.FailedDatasets = append(result.FailedDatasets, dataset)
					continue
				}
			} else {
				result.FailedDatasets = append(result.FailedDatasets, dataset)
				continue
			}
		}

		result.MigratedDatasets++
	}

	result.Success = len(result.FailedDatasets) == 0
	result.Duration = time.Since(startTime)

	return result, nil
}

// ExportDataset exports a dataset to a storage backend in Parquet format.
func (vs *VectorStore) ExportDataset(name string, backend storage.SnapshotBackend) (int64, error) {
	datasetIO := NewDatasetIO(vs)
	return datasetIO.ExportToParquet(vs.ctx, name, backend)
}

// ImportDataset imports a dataset from a storage backend in Parquet format.
func (vs *VectorStore) ImportDataset(ctx context.Context, name string, backend storage.SnapshotBackend, schema *arrow.Schema) (int64, error) {
	return vs.ImportDatasetFrom(ctx, name, name, backend, schema)
}

// ImportDatasetFrom imports a snapshot as a new dataset name.
func (vs *VectorStore) ImportDatasetFrom(ctx context.Context, snapshotName, datasetName string, backend storage.SnapshotBackend, schema *arrow.Schema) (int64, error) {
	datasetIO := NewDatasetIO(vs)
	return datasetIO.ImportFromParquet(ctx, snapshotName, datasetName, backend, schema)
}

// CloneDataset clones a dataset by exporting and then importing it under a new name.
func (vs *VectorStore) CloneDataset(ctx context.Context, source, target string, backend storage.SnapshotBackend) error {
	datasetIO := NewDatasetIO(vs)
	_, err := datasetIO.ExportToParquet(ctx, source, backend)
	if err != nil {
		return err
	}
	_, err = datasetIO.ImportFromParquet(ctx, source, target, backend, nil)
	return err
}

package store

import (
	"context"
	"fmt"

	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/apache/arrow-go/v18/arrow/compute"
)

// ErrNeighborSelectionLengthMismatch is the error type for mismatched input lengths.
type ErrNeighborSelectionLengthMismatch struct {
	DistancesLen int
	IDsLen       int
}

func (e *ErrNeighborSelectionLengthMismatch) Error() string {
	return fmt.Sprintf("neighbor selection length mismatch: dists=%d ids=%d", e.DistancesLen, e.IDsLen)
}

// SelectTopKNeighbors selects the k nearest neighbors based on distances using Arrow compute kernels.
// It uses the custom "select_k_neighbors" kernel.
func (b *BatchDistanceComputer) SelectTopKNeighbors(
	distances []float32,
	ids []uint32,
	k int,
) (indices []uint32, dists []float32, err error) {
	if k <= 0 {
		return nil, nil, nil
	}
	if len(distances) != len(ids) {
		return nil, nil, &ErrNeighborSelectionLengthMismatch{DistancesLen: len(distances), IDsLen: len(ids)}
	}
	if len(ids) <= k {
		outID := make([]uint32, len(ids))
		outDist := make([]float32, len(distances))
		copy(outID, ids)
		copy(outDist, distances)
		return outID, outDist, nil
	}

	// 1. Build Arrow Arrays
	distBuilder := array.NewFloat32Builder(b.mem)
	defer distBuilder.Release()
	distBuilder.AppendValues(distances, nil)
	distArr := distBuilder.NewFloat32Array()
	defer distArr.Release()

	idBuilder := array.NewUint32Builder(b.mem)
	defer idBuilder.Release()
	idBuilder.AppendValues(ids, nil)
	idArr := idBuilder.NewUint32Array()
	defer idArr.Release()

	ctx := context.Background()

	// 2. Call Custom Kernel "select_k_neighbors"
	// Inputs: [Dists, IDs] -> Output: Indices (Uint32)
	// Note: We return indices into the input arrays.
	// Our kernel implementation currently ignores K logic and sorts fully,
	// but places smallest elements first.
	// So we take the first K indices.

	// We did: selectKInit handles SelectKOptions.
	opts := &SelectKOptions{K: k}

	datumDists := compute.NewDatum(distArr)
	datumIDs := compute.NewDatum(idArr) // Passed but currently unused by kernel (it sorts indices by dists)

	indicesDatum, err := compute.CallFunction(ctx, "select_k_neighbors", opts, datumDists, datumIDs)
	if err != nil {
		return nil, nil, NewNeighborSelectionFailedError("select_k_neighbors", err)
	}
	defer indicesDatum.Release()

	indicesArr := indicesDatum.(*compute.ArrayDatum).MakeArray().(*array.Uint32)
	defer indicesArr.Release()

	// 3. Take Top K
	// The kernel returns sorted indices [0..N]. We only need [0..K].
	// Slice the indices array.

	limit := k
	if indicesArr.Len() < limit {
		limit = indicesArr.Len()
	}

	// Use NewSlice provided by Arrow Array (returns Interface)
	topKIndices := array.NewSlice(indicesArr, 0, int64(limit))
	defer topKIndices.Release()

	// 4. Take (Gather) IDs and Distances using the top K indices
	takeOpts := compute.DefaultTakeOptions()
	takeOpts.BoundsCheck = false // Indices guaranteed valid by kernel

	takenIDsDatum, err := compute.Take(ctx, *takeOpts, datumIDs, compute.NewDatum(topKIndices))
	if err != nil {
		return nil, nil, NewNeighborSelectionFailedError("take_ids", err)
	}
	defer takenIDsDatum.Release()
	takenIDs := takenIDsDatum.(*compute.ArrayDatum).MakeArray().(*array.Uint32)
	defer takenIDs.Release()

	takenDistsDatum, err := compute.Take(ctx, *takeOpts, datumDists, compute.NewDatum(topKIndices))
	if err != nil {
		return nil, nil, NewNeighborSelectionFailedError("take_dists", err)
	}
	defer takenDistsDatum.Release()
	takenDists := takenDistsDatum.(*compute.ArrayDatum).MakeArray().(*array.Float32)
	defer takenDists.Release()

	// 5. Convert back to Go slices
	resIDs := make([]uint32, limit)
	copy(resIDs, takenIDs.Uint32Values())

	resDists := make([]float32, limit)
	copy(resDists, takenDists.Float32Values())

	return resIDs, resDists, nil
}

func NewNeighborSelectionFailedError(op string, err error) error {
	return fmt.Errorf("neighbor selection failed during %s: %w", op, err)
}

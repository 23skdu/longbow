package store

import (
	"context"
	"math/rand"
	"testing"
	"time"

	"github.com/23skdu/longbow/internal/store/types"
	"github.com/stretchr/testify/require"
)

func FuzzFindPath(f *testing.F) {
	// Seed data: startID, targetID, maxHops, earlyTerminate, distThreshold
	f.Add(uint32(0), uint32(10), 5, true, float32(0.5))
	f.Add(uint32(1), uint32(20), 10, false, float32(1.0))
	f.Add(uint32(5), uint32(5), 2, true, float32(0.1))

	f.Fuzz(func(t *testing.T, startID, targetID uint32, maxHops int, earlyTerminate bool, distThreshold float32) {
		if startID >= 100 || targetID >= 100 || maxHops < 0 || maxHops > 20 {
			return
		}

		g := types.NewGraphData(100, 2, false, false, 0, false, false, false, types.VectorTypeFloat32)
		err := g.EnsureChunk(0, 0, 2)
		require.NoError(t, err)

		// Create a random graph structure
		rng := rand.New(rand.NewSource(time.Now().UnixNano()))
		for i := uint32(0); i < 100; i++ {
			_ = g.SetVector(i, []float32{rng.Float32() * 10, rng.Float32() * 10})
		}

		// Random edges
		for i := uint32(0); i < 100; i++ {
			neighbors := make([]uint32, 0)
			for j := 0; j < 5; j++ {
				n := uint32(rng.Intn(100))
				if n != i {
					neighbors = append(neighbors, n)
				}
			}
			_ = g.SetNeighbors(i, neighbors)
		}

		config := NavigatorConfig{
			MaxHops:           maxHops,
			EarlyTerminate:    earlyTerminate,
			DistanceThreshold: distThreshold,
		}
		nav := NewGraphNavigator(g, config, nil)
		err = nav.Initialize()
		require.NoError(t, err)

		ctx, cancel := context.WithTimeout(context.Background(), 100*time.Millisecond)
		defer cancel()

		query := NavigatorQuery{
			StartID:  startID,
			TargetID: targetID,
			MaxHops:  maxHops,
		}

		_, err = nav.FindPath(ctx, query)
		// We don't necessarily care if it finds a path, just that it doesn't crash or hang
		if err != nil && err != context.DeadlineExceeded && err != context.Canceled {
			t.Errorf("FindPath failed with error: %v", err)
		}
	})
}

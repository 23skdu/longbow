package store

import (
	"context"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// The hedged group wait loop is the one piece of GlobalSearch whose correctness
// depends on a select race: the winning replica writes its result, then cancels
// the group context, then releases the replica wait group. All three cases can
// therefore be ready in a single select evaluation, and Go chooses among ready
// cases pseudo-randomly. Returning on the cancellation or group-completed case
// without draining discards the winner at random — an intermittent missing peer
// contribution in the merged top-k.
//
// These tests make that interleaving deterministic instead of hoping a real
// scheduler reproduces it.

// winnerThenCancel reproduces the winner's exact ordering: buffer the result,
// then cancel the group context.
func winnerThenCancel(t *testing.T) (chan []SearchResult, context.Context, context.CancelFunc) {
	t.Helper()
	resultHedge := make(chan []SearchResult, 1)
	ctx, cancel := context.WithCancel(context.Background())
	resultHedge <- []SearchResult{{ID: 42, Score: 0.9}}
	cancel()
	return resultHedge, ctx, cancel
}

// TestAwaitHedgeGroup_DrainOnCancelledContext is the core regression: the
// winner's result is buffered and the group context is already cancelled, so
// both cases are ready simultaneously. The helper must return the winner.
func TestAwaitHedgeGroup_DrainOnCancelledContext(t *testing.T) {
	resultHedge, ctx, cancel := winnerThenCancel(t)
	defer cancel()

	failSignal := make(chan struct{}, 1)
	finishedAll := make(chan struct{})

	// Repeat: select randomises among ready cases, so a single run can pass by
	// luck even with the buggy implementation.
	for i := 0; i < 200; i++ {
		// Re-arm the buffered winner and cancelled context each iteration.
		select {
		case <-resultHedge:
		default:
		}
		resultHedge <- []SearchResult{{ID: 42, Score: 0.9}}

		res, outcome := awaitHedgeGroup(resultHedge, failSignal, finishedAll, ctx, 1)
		require.Equal(t, hedgeWinner, outcome, "iteration %d: winner must not be dropped", i)
		require.Len(t, res, 1)
		require.Equal(t, VectorID(42), res[0].ID)
	}
}

// TestAwaitHedgeGroup_DrainOnGroupCompleted covers the other simultaneous-case
// combination: the winner buffered its result and the replica wait group has
// already completed.
func TestAwaitHedgeGroup_DrainOnGroupCompleted(t *testing.T) {
	resultHedge := make(chan []SearchResult, 1)
	resultHedge <- []SearchResult{{ID: 7, Score: 0.5}}

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	failSignal := make(chan struct{}, 1)
	finishedAll := make(chan struct{})
	close(finishedAll)

	for i := 0; i < 200; i++ {
		select {
		case <-resultHedge:
		default:
		}
		resultHedge <- []SearchResult{{ID: 7, Score: 0.5}}

		res, outcome := awaitHedgeGroup(resultHedge, failSignal, finishedAll, ctx, 1)
		require.Equal(t, hedgeWinner, outcome, "iteration %d", i)
		require.Equal(t, VectorID(7), res[0].ID)
	}
}

// TestAwaitHedgeGroup_AllThreeReady is the worst case: buffered winner,
// cancelled context and completed replica group are all ready at once.
func TestAwaitHedgeGroup_AllThreeReady(t *testing.T) {
	resultHedge := make(chan []SearchResult, 1)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	failSignal := make(chan struct{}, 2)
	finishedAll := make(chan struct{})
	close(finishedAll)

	for i := 0; i < 200; i++ {
		select {
		case <-resultHedge:
		default:
		}
		resultHedge <- []SearchResult{{ID: VectorID(uint32(i)), Score: 1.0}} // #nosec G115

		res, outcome := awaitHedgeGroup(resultHedge, failSignal, finishedAll, ctx, 2)
		require.Equal(t, hedgeWinner, outcome, "iteration %d: buffered winner must survive", i)
		require.Equal(t, VectorID(uint32(i)), res[0].ID) // #nosec G115
	}
}

// TestAwaitHedgeGroup_OneFailureKeepsGroupOpen asserts a single failure does
// NOT end the group — the remaining replica may still win — and that the
// group does end once every replica has failed.
func TestAwaitHedgeGroup_OneFailureKeepsGroupOpen(t *testing.T) {
	resultHedge := make(chan []SearchResult, 1)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	failSignal := make(chan struct{}, 2)
	finishedAll := make(chan struct{})

	// One of two replicas failed: the helper must still be waiting.
	failSignal <- struct{}{}
	type waitResult struct {
		res     []SearchResult
		outcome hedgeOutcome
	}
	got := make(chan waitResult, 1)
	go func() {
		res, o := awaitHedgeGroup(resultHedge, failSignal, finishedAll, ctx, 2)
		got <- waitResult{res, o}
	}()

	select {
	case o := <-got:
		t.Fatalf("helper returned after only 1 of 2 replica failures: %+v", o)
	case <-time.After(150 * time.Millisecond):
		// Still waiting, as required.
	}

	// The surviving replica wins: the group must surface its result.
	resultHedge <- []SearchResult{{ID: 11, Score: 0.8}}
	select {
	case o := <-got:
		require.Equal(t, hedgeWinner, o.outcome)
		require.Equal(t, VectorID(11), o.res[0].ID)
	case <-time.After(2 * time.Second):
		t.Fatal("helper did not return the winner after a partial failure")
	}

	// Every replica failed: the group ends as a partial failure.
	failSignal <- struct{}{}
	failSignal <- struct{}{}
	res, outcome := awaitHedgeGroup(resultHedge, failSignal, finishedAll, ctx, 2)
	require.Equal(t, hedgeAllFailed, outcome)
	require.Nil(t, res)
	require.True(t, outcome.partialFailure())
}

// TestAwaitHedgeGroup_CancelledWithoutWinner asserts cancellation with nothing
// buffered is reported as a partial failure (peer contribution lost) rather
// than as a winner.
func TestAwaitHedgeGroup_CancelledWithoutWinner(t *testing.T) {
	resultHedge := make(chan []SearchResult, 1)
	ctx, cancel := context.WithCancel(context.Background())
	cancel()

	failSignal := make(chan struct{}, 1)
	finishedAll := make(chan struct{})

	res, outcome := awaitHedgeGroup(resultHedge, failSignal, finishedAll, ctx, 1)
	require.Equal(t, hedgeCancelled, outcome)
	require.Nil(t, res)
	require.True(t, outcome.partialFailure())
}

// TestAwaitHedgeGroup_GroupCompletedWithoutWinner asserts an empty completed
// group is distinguished from a failure: the replicas finished, they just had
// nothing to contribute, so it is not counted as a partial failure.
func TestAwaitHedgeGroup_GroupCompletedWithoutWinner(t *testing.T) {
	resultHedge := make(chan []SearchResult, 1)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	failSignal := make(chan struct{}, 1)
	finishedAll := make(chan struct{})
	close(finishedAll)

	res, outcome := awaitHedgeGroup(resultHedge, failSignal, finishedAll, ctx, 1)
	require.Equal(t, hedgeGroupDrained, outcome)
	require.Nil(t, res)
	require.False(t, outcome.partialFailure())
}

// TestAwaitHedgeGroup_WinnerFirst covers the plain happy path where the wait
// loop observes the result before anything else becomes ready.
func TestAwaitHedgeGroup_WinnerFirst(t *testing.T) {
	resultHedge := make(chan []SearchResult, 1)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	failSignal := make(chan struct{}, 1)
	finishedAll := make(chan struct{})

	go func() {
		time.Sleep(10 * time.Millisecond)
		resultHedge <- []SearchResult{{ID: 5, Score: 0.7}}
	}()

	res, outcome := awaitHedgeGroup(resultHedge, failSignal, finishedAll, ctx, 1)
	require.Equal(t, hedgeWinner, outcome)
	require.Equal(t, VectorID(5), res[0].ID)
}

// TestDrainHedgeResult pins the helper's non-blocking contract: it never
// blocks and reports emptiness accurately.
func TestDrainHedgeResult(t *testing.T) {
	ch := make(chan []SearchResult, 1)

	_, ok := drainHedgeResult(ch)
	require.False(t, ok, "empty channel must report no result")

	ch <- []SearchResult{{ID: 1, Score: 1}}
	res, ok := drainHedgeResult(ch)
	require.True(t, ok)
	require.Equal(t, VectorID(1), res[0].ID)

	_, ok = drainHedgeResult(ch)
	require.False(t, ok, "second drain must be empty")

	// Draining a closed channel returns immediately with no value.
	closed := make(chan []SearchResult)
	close(closed)
	res, ok = drainHedgeResult(closed)
	require.False(t, ok)
	require.Nil(t, res)

	// An unbuffered channel with no sender must return immediately rather
	// than block — this is the whole point of the helper.
	blocked := make(chan []SearchResult)
	done := make(chan struct{})
	go func() {
		_, got := drainHedgeResult(blocked)
		assert.False(t, got, "empty channel must report no result")
		close(done)
	}()
	select {
	case <-done:
	case <-time.After(2 * time.Second):
		t.Fatal("drainHedgeResult blocked on an empty unbuffered channel")
	}
}

// TestHedgeOutcome_PartialFailureClassification pins which outcomes count
// toward GlobalSearchPartialFailures.
func TestHedgeOutcome_PartialFailureClassification(t *testing.T) {
	assert.True(t, hedgeAllFailed.partialFailure())
	assert.True(t, hedgeCancelled.partialFailure())
	assert.False(t, hedgeGroupDrained.partialFailure())
	assert.False(t, hedgeWinner.partialFailure())
}

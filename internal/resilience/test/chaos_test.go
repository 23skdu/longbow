package test

import (
	"context"
	"fmt"
	"math/rand"
	"sync"
	"sync/atomic"
	"time"

	"github.com/23skdu/longbow/internal/resilience"
)

type ChaosType int

const (
	ChaosLatency ChaosType = iota
	ChaosError
	ChaosTimeout
	ChaosCircuitBreak
	ChaosMemoryPressure
	ChaosNetworkPartition
)

type ChaosConfig struct {
	Type            ChaosType
	Probability     float64
	Delay           time.Duration
	ErrorRate       float64
	TimeoutRate     float64
	Duration        time.Duration
	TargetComponent string
}

type ChaosInjector struct {
	configs       []ChaosConfig
	running       int32
	mu            sync.RWMutex
	injectedCount int64
}

func NewChaosInjector() *ChaosInjector {
	return &ChaosInjector{
		configs: make([]ChaosConfig, 0),
		running: 0,
	}
}

func (ci *ChaosInjector) AddConfig(config ChaosConfig) {
	ci.mu.Lock()
	defer ci.mu.Unlock()
	ci.configs = append(ci.configs, config)
}

func (ci *ChaosInjector) ShouldInject(target string) (bool, ChaosConfig) {
	ci.mu.RLock()
	defer ci.mu.RUnlock()

	if atomic.LoadInt32(&ci.running) == 0 {
		return false, ChaosConfig{}
	}

	for _, config := range ci.configs {
		if config.TargetComponent == target || config.TargetComponent == "all" {
			if rand.Float64() < config.Probability {
				atomic.AddInt64(&ci.injectedCount, 1)
				return true, config
			}
		}
	}

	return false, ChaosConfig{}
}

func (ci *ChaosInjector) Start() {
	atomic.StoreInt32(&ci.running, 1)
}

func (ci *ChaosInjector) Stop() {
	atomic.StoreInt32(&ci.running, 0)
}

func (ci *ChaosInjector) GetInjectedCount() int64 {
	return atomic.LoadInt64(&ci.injectedCount)
}

func (ci *ChaosInjector) Reset() {
	atomic.StoreInt64(&ci.injectedCount, 0)
}

type ChaosProxy struct {
	injector *ChaosInjector
	name     string
}

func NewChaosProxy(name string, injector *ChaosInjector) *ChaosProxy {
	return &ChaosProxy{
		injector: injector,
		name:     name,
	}
}

func (cp *ChaosProxy) ExecuteWithChaos(fn func() (interface{}, error)) (interface{}, error) {
	shouldInject, config := cp.injector.ShouldInject(cp.name)
	if !shouldInject {
		return fn()
	}

	switch config.Type {
	case ChaosLatency:
		time.Sleep(config.Delay)
		return fn()
	case ChaosError:
		if rand.Float64() < config.ErrorRate {
			return nil, fmt.Errorf("chaos injected error for %s", cp.name)
		}
		return fn()
	case ChaosTimeout:
		if rand.Float64() < config.TimeoutRate {
			time.Sleep(config.Duration)
			return nil, fmt.Errorf("chaos injected timeout for %s", cp.name)
		}
		return fn()
	default:
		return fn()
	}
}

type ResilienceTestSuite struct {
	injector *ChaosInjector
	manager  *resilience.ResilienceManager
	proxies  map[string]*ChaosProxy
	metrics  *TestMetrics
	mu       sync.RWMutex
}

type TestMetrics struct {
	TotalRequests       int64
	SuccessfulRequests  int64
	FailedRequests      int64
	InjectedFaults      int64
	CircuitBreakerTrips int64
	RetryAttempts       int64
	AverageLatency      time.Duration
	mu                  sync.RWMutex
}

func NewResilienceTestSuite() *ResilienceTestSuite {
	config := resilience.DefaultResilienceConfig()
	manager := resilience.NewResilienceManager(config)
	injector := NewChaosInjector()

	return &ResilienceTestSuite{
		injector: injector,
		manager:  manager,
		proxies:  make(map[string]*ChaosProxy),
		metrics:  &TestMetrics{},
	}
}

func (rts *ResilienceTestSuite) GetProxy(name string) *ChaosProxy {
	rts.mu.RLock()
	proxy, exists := rts.proxies[name]
	rts.mu.RUnlock()

	if exists {
		return proxy
	}

	rts.mu.Lock()
	defer rts.mu.Unlock()

	if proxy, exists := rts.proxies[name]; exists {
		return proxy
	}

	proxy = NewChaosProxy(name, rts.injector)
	rts.proxies[name] = proxy
	return proxy
}

func (rts *ResilienceTestSuite) RunLoadTest(ctx context.Context, duration time.Duration, concurrency int, operation func() (interface{}, error)) *TestResults {
	start := time.Now()
	end := start.Add(duration)

	var wg sync.WaitGroup
	results := make(chan *TestResult, concurrency*100)

	for i := 0; i < concurrency; i++ {
		wg.Add(1)
		go func(workerID int) {
			defer wg.Done()
			rts.worker(ctx, end, workerID, operation, results)
		}(i)
	}

	go func() {
		wg.Wait()
		close(results)
	}()

	return rts.collectResults(results)
}

func (rts *ResilienceTestSuite) worker(ctx context.Context, endTime time.Time, workerID int, operation func() (interface{}, error), results chan<- *TestResult) {
	requestCount := 0

	for time.Now().Before(endTime) {
		select {
		case <-ctx.Done():
			return
		default:
		}

		start := time.Now()
		_, err := operation()
		latency := time.Since(start)

		testResult := &TestResult{
			WorkerID:  workerID,
			RequestID: requestCount,
			Latency:   latency,
			Success:   err == nil,
			Error:     err,
			Timestamp: start,
		}

		results <- testResult
		requestCount++

		rts.recordRequest(testResult)
	}
}

func (rts *ResilienceTestSuite) recordRequest(result *TestResult) {
	rts.metrics.mu.Lock()
	defer rts.metrics.mu.Unlock()

	rts.metrics.TotalRequests++

	if result.Success {
		rts.metrics.SuccessfulRequests++
	} else {
		rts.metrics.FailedRequests++
	}

	rts.metrics.AverageLatency = time.Duration(
		(int64(rts.metrics.AverageLatency) + int64(result.Latency)) / 2,
	)
}

func (rts *ResilienceTestSuite) collectResults(results <-chan *TestResult) *TestResults {
	testResults := &TestResults{
		StartTime: time.Now(),
	}

	var totalLatency time.Duration
	var minLatency, maxLatency time.Duration = time.Hour, 0

	for result := range results {
		testResults.TotalRequests++
		testResults.Results = append(testResults.Results, result)

		if result.Success {
			testResults.SuccessfulRequests++
		} else {
			testResults.FailedRequests++
		}

		totalLatency += result.Latency
		if result.Latency < minLatency {
			minLatency = result.Latency
		}
		if result.Latency > maxLatency {
			maxLatency = result.Latency
		}

		if testResults.StartTime.IsZero() || result.Timestamp.Before(testResults.StartTime) {
			testResults.StartTime = result.Timestamp
		}
		if result.Timestamp.After(testResults.EndTime) {
			testResults.EndTime = result.Timestamp
		}
	}

	if testResults.TotalRequests > 0 {
		testResults.AverageLatency = totalLatency / time.Duration(testResults.TotalRequests)
		testResults.MinLatency = minLatency
		testResults.MaxLatency = maxLatency
		testResults.SuccessRate = float64(testResults.SuccessfulRequests) / float64(testResults.TotalRequests)
		testResults.ErrorRate = float64(testResults.FailedRequests) / float64(testResults.TotalRequests)
	}

	return testResults
}

type TestResult struct {
	WorkerID  int
	RequestID int
	Latency   time.Duration
	Success   bool
	Error     error
	Timestamp time.Time
}

type TestResults struct {
	TotalRequests      int
	SuccessfulRequests int
	FailedRequests     int
	AverageLatency     time.Duration
	MinLatency         time.Duration
	MaxLatency         time.Duration
	SuccessRate        float64
	ErrorRate          float64
	StartTime          time.Time
	EndTime            time.Time
	Results            []*TestResult
}

func (rts *ResilienceTestSuite) TestCircuitBreaker(ctx context.Context, testName string, faultRate float64) *TestResults {
	rts.injector.AddConfig(ChaosConfig{
		Type:            ChaosError,
		Probability:     faultRate,
		ErrorRate:       1.0,
		TargetComponent: testName,
	})

	proxy := rts.GetProxy(testName)

	operation := func() (interface{}, error) {
		return proxy.ExecuteWithChaos(func() (interface{}, error) {
			return "success", nil
		})
	}

	rts.injector.Start()
	defer rts.injector.Stop()

	return rts.RunLoadTest(ctx, 30*time.Second, 10, operation)
}

func (rts *ResilienceTestSuite) TestRetryLogic(ctx context.Context, testName string, faultRate float64) *TestResults {
	rts.injector.AddConfig(ChaosConfig{
		Type:            ChaosLatency,
		Probability:     faultRate,
		Delay:           2 * time.Second,
		TargetComponent: testName,
	})

	proxy := rts.GetProxy(testName)

	operation := func() (interface{}, error) {
		_, err := rts.manager.ExecuteWithRetry(ctx, "network", testName, func() (interface{}, error) {
			return proxy.ExecuteWithChaos(func() (interface{}, error) {
				return "success", nil
			})
		})
		return nil, err
	}

	rts.injector.Start()
	defer rts.injector.Stop()

	return rts.RunLoadTest(ctx, 30*time.Second, 5, operation)
}

func (rts *ResilienceTestSuite) TestBulkhead(ctx context.Context, testName string, concurrency int) *TestResults {
	config := resilience.DefaultResilienceConfig()
	config.Bulkheads[testName] = concurrency / 2
	rts.manager = resilience.NewResilienceManager(config)

	operation := func() (interface{}, error) {
		_, err := rts.manager.ExecuteWithResilience(ctx, testName, testName, func() (interface{}, error) {
			time.Sleep(100 * time.Millisecond)
			return "success", nil
		})
		return nil, err
	}

	return rts.RunLoadTest(ctx, 30*time.Second, concurrency, operation)
}

func (rts *ResilienceTestSuite) TestGracefulDegradation(ctx context.Context, testName string) *TestResults {
	rts.manager.AddFallbackStrategy(testName+"_fallback", resilience.DegradationModerate, func(ctx context.Context) (interface{}, error) {
		return "fallback_result", nil
	}, true)

	rts.injector.AddConfig(ChaosConfig{
		Type:            ChaosError,
		Probability:     0.8,
		ErrorRate:       1.0,
		TargetComponent: testName,
	})

	proxy := rts.GetProxy(testName)

	operation := func() (interface{}, error) {
		return rts.manager.ExecuteWithDegradation(ctx, testName, testName, testName+"_fallback", func() (interface{}, error) {
			return proxy.ExecuteWithChaos(func() (interface{}, error) {
				return "primary_result", nil
			})
		})
	}

	rts.injector.Start()
	defer rts.injector.Stop()

	return rts.RunLoadTest(ctx, 30*time.Second, 10, operation)
}

func (rts *ResilienceTestSuite) GetMetrics() *TestMetrics {
	rts.metrics.mu.RLock()
	defer rts.metrics.mu.RUnlock()

	return &TestMetrics{
		TotalRequests:       rts.metrics.TotalRequests,
		SuccessfulRequests:  rts.metrics.SuccessfulRequests,
		FailedRequests:      rts.metrics.FailedRequests,
		InjectedFaults:      rts.metrics.InjectedFaults,
		CircuitBreakerTrips: rts.metrics.CircuitBreakerTrips,
		RetryAttempts:       rts.metrics.RetryAttempts,
		AverageLatency:      rts.metrics.AverageLatency,
	}
}

func (rts *ResilienceTestSuite) Reset() {
	rts.injector.Reset()
	rts.metrics = &TestMetrics{}
}

func (tr *TestResults) PrintSummary() {
	fmt.Printf("Test Results Summary:\n")
	fmt.Printf("Total Requests: %d\n", tr.TotalRequests)
	fmt.Printf("Successful: %d (%.2f%%)\n", tr.SuccessfulRequests, tr.SuccessRate*100)
	fmt.Printf("Failed: %d (%.2f%%)\n", tr.FailedRequests, tr.ErrorRate*100)
	fmt.Printf("Average Latency: %v\n", tr.AverageLatency)
	fmt.Printf("Min Latency: %v\n", tr.MinLatency)
	fmt.Printf("Max Latency: %v\n", tr.MaxLatency)
	fmt.Printf("Duration: %v\n", tr.EndTime.Sub(tr.StartTime))
}

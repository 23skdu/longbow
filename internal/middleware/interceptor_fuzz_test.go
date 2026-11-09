package middleware

import (
	"context"
	"errors"
	"testing"

	"github.com/23skdu/longbow/internal/breaker"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

const (
	fuzzProtectedDoGet    = "/arrow.flight.protocol.FlightService/DoGet"
	fuzzProtectedDoAction = "/arrow.flight.protocol.FlightService/DoAction"
)

// FuzzCircuitBreakerInterceptor drives the breaker with a deterministic
// failure sequence: the first ten protected failures must surface the
// handler's own error, the eleventh call must be rejected with the
// Unavailable breaker error without invoking the handler, and unprotected
// methods must never trip the breaker at all.
func FuzzCircuitBreakerInterceptor(f *testing.F) {
	f.Add(fuzzProtectedDoGet, "boom", "payload")
	f.Add(fuzzProtectedDoAction, "", "")
	f.Add("/some.other.Service/Method", "err with \n newline", "x")
	f.Add("", "e", "")
	f.Add("/arrow.flight.protocol.FlightService/DoGetx", "near-miss method", "y")
	f.Add("/Arrow.Flight.Protocol.FlightService/DoGet", "case differs", "z")

	f.Fuzz(func(t *testing.T, fullMethod, errMsg, payload string) {
		if len(fullMethod) > 1024 || len(errMsg) > 512 || len(payload) > 512 {
			return
		}

		// Fresh interceptor (and therefore breaker) for every input.
		interceptor := CircuitBreakerInterceptor()
		info := &grpc.UnaryServerInfo{FullMethod: fullMethod}
		ctx := context.Background()
		handlerErr := errors.New(errMsg)

		handlerCalls := 0
		failing := func(context.Context, any) (any, error) {
			handlerCalls++
			return nil, handlerErr
		}
		succeeding := func(context.Context, any) (any, error) {
			handlerCalls++
			return payload, nil
		}

		protected := fullMethod == fuzzProtectedDoGet || fullMethod == fuzzProtectedDoAction

		// Ten failures must be passed through untouched: the breaker only
		// opens once ConsecutiveFailures reaches its threshold.
		for i := 0; i < 10; i++ {
			res, err := interceptor(ctx, nil, info, failing)
			if err == nil {
				t.Fatalf("failing call %d returned no error", i+1)
			}
			if errors.Is(err, breaker.ErrOpenState) {
				t.Fatalf("breaker opened after only %d failures", i+1)
			}
			if !errors.Is(err, handlerErr) {
				t.Fatalf("failing call %d returned %v, want the handler error", i+1, err)
			}
			if status.Code(err) == codes.Unavailable {
				t.Fatalf("failing call %d surfaced a breaker status code", i+1)
			}
			if res != nil {
				t.Errorf("failing call %d returned result %v, want nil", i+1, res)
			}
		}
		if handlerCalls != 10 {
			t.Fatalf("handler ran %d times, want 10", handlerCalls)
		}

		// The eleventh call: protected methods are rejected by the breaker
		// without running the handler; unprotected methods still run it.
		res, err := interceptor(ctx, nil, info, failing)
		if protected {
			if handlerCalls != 10 {
				t.Errorf("blocked call ran the handler (calls=%d)", handlerCalls)
			}
			if status.Code(err) != codes.Unavailable || status.Convert(err).Message() != "service circuit breaker is open" {
				t.Errorf("blocked call returned %v, want Unavailable 'service circuit breaker is open'", err)
			}
			if errors.Is(err, handlerErr) {
				t.Error("blocked call wrapped the handler error")
			}
			if res != nil {
				t.Errorf("blocked call returned result %v, want nil", res)
			}

			// Still open: a healthy call is rejected the same way.
			res2, err2 := interceptor(ctx, nil, info, succeeding)
			if handlerCalls != 10 {
				t.Errorf("healthy call ran the handler while open (calls=%d)", handlerCalls)
			}
			if status.Code(err2) != codes.Unavailable {
				t.Errorf("healthy call returned %v, want Unavailable", err2)
			}
			if res2 != nil {
				t.Errorf("healthy call returned result %v, want nil", res2)
			}
			return
		}

		if handlerCalls != 11 {
			t.Errorf("unprotected handler ran %d times, want 11", handlerCalls)
		}
		if !errors.Is(err, handlerErr) {
			t.Errorf("unprotected call %d returned %v, want the handler error", 11, err)
		}
		if res != nil {
			t.Errorf("unprotected failing call returned result %v, want nil", res)
		}

		// An unprotected healthy call must be a transparent pass-through.
		res2, err2 := interceptor(ctx, nil, info, succeeding)
		if handlerCalls != 12 {
			t.Errorf("unprotected handler ran %d times, want 12", handlerCalls)
		}
		if err2 != nil {
			t.Errorf("unhealthy call returned error %v", err2)
		}
		if res2 != payload {
			t.Errorf("unprotected healthy call returned %v, want %q", res2, payload)
		}
	})
}

// FuzzAdmissionInterceptorPassThrough checks that a nil admission
// controller makes the unary interceptor completely transparent: the
// handler runs exactly once with the original request and its result (or
// error) is returned verbatim, regardless of how the method name would be
// classified for admission control.
func FuzzAdmissionInterceptorPassThrough(f *testing.F) {
	f.Add("/svc.Search/Search", "req-1", "res-1", "", false)
	f.Add("/svc.Store/Put", "", "", "failed", true)
	f.Add("/svc.Admin/DoAction", "req", "res", "", false)
	f.Add("", "", "", "", true)
	f.Add("/svc.Search/DoPut", "a", "b", "boom", true)

	f.Fuzz(func(t *testing.T, fullMethod, req, res, errMsg string, fail bool) {
		if len(fullMethod) > 1024 || len(req) > 512 || len(res) > 512 || len(errMsg) > 512 {
			return
		}

		interceptor := AdmissionInterceptor(nil)
		info := &grpc.UnaryServerInfo{FullMethod: fullMethod}
		handlerErr := errors.New(errMsg)

		handlerCalls := 0
		handler := func(_ context.Context, got any) (any, error) {
			handlerCalls++
			if got != req {
				t.Errorf("handler received %v, want %q", got, req)
			}
			if fail {
				return nil, handlerErr
			}
			return res, nil
		}

		out, err := interceptor(context.Background(), req, info, handler)

		if handlerCalls != 1 {
			t.Fatalf("handler ran %d times, want exactly 1", handlerCalls)
		}
		if fail {
			if !errors.Is(err, handlerErr) {
				t.Errorf("returned error %v, want the handler error", err)
			}
			if out != nil {
				t.Errorf("returned result %v, want nil on error", out)
			}
			return
		}
		if err != nil {
			t.Errorf("returned error %v, want nil", err)
		}
		if out != res {
			t.Errorf("returned result %v, want %q", out, res)
		}
	})
}

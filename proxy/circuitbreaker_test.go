// SPDX-License-Identifier: Apache-2.0

package proxy

import (
	"context"
	"errors"
	"sync"
	"testing"
	"time"

	"github.com/luraproject/lura/v2/config"
	"github.com/luraproject/lura/v2/logging"
	"github.com/luraproject/lura/v2/sd"
)

func cbConfig(maxErrors int, timeout time.Duration, halfOpenMax int) *config.Backend {
	return &config.Backend{
		URLPattern: "/test",
		CircuitBreaker: config.CircuitBreaker{
			Name:                  "test-breaker",
			MaxErrors:             maxErrors,
			Timeout:               timeout,
			MaxConcurrentRequests: halfOpenMax,
		},
	}
}

func failingProxy(err error) Proxy {
	return func(_ context.Context, _ *Request) (*Response, error) {
		return nil, err
	}
}

func TestCircuitBreakerMiddleware_disabled(t *testing.T) {
	remote := cbConfig(0, time.Second, 1)
	mw, subscriber := NewCircuitBreakerMiddlewareWithSubscriber(logging.NoOp, remote, sd.FixedSubscriber{"h1"})
	if _, ok := subscriber.(sd.FixedSubscriber); !ok {
		t.Error("disabled circuit breaker should return the received subscriber untouched")
	}
	expected := errors.New("backend error")
	p := mw(failingProxy(expected))
	for i := 0; i < 10; i++ {
		if _, err := p(context.Background(), &Request{}); err != expected {
			t.Errorf("disabled circuit breaker should never reject requests, got: %v", err)
		}
	}
}

func TestCircuitBreakerMiddleware_opensAfterThreshold(t *testing.T) {
	remote := cbConfig(3, time.Hour, 1)
	mw, _ := NewCircuitBreakerMiddlewareWithSubscriber(logging.NoOp, remote, sd.FixedSubscriber{"h1"})
	backendErr := errors.New("backend error")
	p := mw(failingProxy(backendErr))

	for i := 0; i < 3; i++ {
		if _, err := p(context.Background(), &Request{}); err != backendErr {
			t.Fatalf("call %d: expected the backend error, got: %v", i, err)
		}
	}
	// the circuit is now open: requests must be rejected before hitting the backend
	if _, err := p(context.Background(), &Request{}); err != ErrCircuitBreakerOpen {
		t.Errorf("expected ErrCircuitBreakerOpen, got: %v", err)
	}
}

func TestCircuitBreakerMiddleware_successResetsErrorCount(t *testing.T) {
	remote := cbConfig(2, time.Hour, 1)
	mw, _ := NewCircuitBreakerMiddlewareWithSubscriber(logging.NoOp, remote, sd.FixedSubscriber{"h1"})
	backendErr := errors.New("backend error")
	fail := true
	p := mw(func(_ context.Context, _ *Request) (*Response, error) {
		if fail {
			return nil, backendErr
		}
		return &Response{}, nil
	})

	if _, err := p(context.Background(), &Request{}); err != backendErr {
		t.Fatalf("expected the backend error, got: %v", err)
	}
	fail = false
	if _, err := p(context.Background(), &Request{}); err != nil {
		t.Fatalf("expected a success, got: %v", err)
	}
	fail = true
	// the error counter was reset, so a single error must not open the circuit
	if _, err := p(context.Background(), &Request{}); err != backendErr {
		t.Fatalf("expected the backend error, got: %v", err)
	}
	if _, err := p(context.Background(), &Request{}); err != backendErr {
		t.Errorf("circuit opened too early, expected the backend error, got: %v", err)
	}
}

func TestCircuitBreakerMiddleware_halfOpenAfterTimeout(t *testing.T) {
	remote := cbConfig(1, 50*time.Millisecond, 1)
	mw, _ := NewCircuitBreakerMiddlewareWithSubscriber(logging.NoOp, remote, sd.FixedSubscriber{"h1"})
	backendErr := errors.New("backend error")
	fail := true
	p := mw(func(_ context.Context, _ *Request) (*Response, error) {
		if fail {
			return nil, backendErr
		}
		return &Response{}, nil
	})

	if _, err := p(context.Background(), &Request{}); err != backendErr {
		t.Fatalf("expected the backend error, got: %v", err)
	}
	if _, err := p(context.Background(), &Request{}); err != ErrCircuitBreakerOpen {
		t.Fatalf("expected ErrCircuitBreakerOpen, got: %v", err)
	}

	time.Sleep(60 * time.Millisecond)

	// half-open: a probe request is let through and succeeds, closing the circuit
	fail = false
	if _, err := p(context.Background(), &Request{}); err != nil {
		t.Fatalf("expected the half-open probe to succeed, got: %v", err)
	}
	// circuit closed again: traffic flows
	if _, err := p(context.Background(), &Request{}); err != nil {
		t.Errorf("expected the circuit to be closed, got: %v", err)
	}
}

func TestCircuitBreakerMiddleware_halfOpenFailureReopens(t *testing.T) {
	remote := cbConfig(1, 50*time.Millisecond, 1)
	mw, _ := NewCircuitBreakerMiddlewareWithSubscriber(logging.NoOp, remote, sd.FixedSubscriber{"h1"})
	backendErr := errors.New("backend error")
	p := mw(failingProxy(backendErr))

	if _, err := p(context.Background(), &Request{}); err != backendErr {
		t.Fatalf("expected the backend error, got: %v", err)
	}
	time.Sleep(60 * time.Millisecond)

	// half-open probe fails: the circuit opens again
	if _, err := p(context.Background(), &Request{}); err != backendErr {
		t.Fatalf("expected the backend error, got: %v", err)
	}
	if _, err := p(context.Background(), &Request{}); err != ErrCircuitBreakerOpen {
		t.Errorf("expected the circuit to be open again, got: %v", err)
	}
}

func TestCircuitBreakerMiddleware_halfOpenLimitsConcurrentRequests(t *testing.T) {
	remote := cbConfig(1, 50*time.Millisecond, 1)
	mw, _ := NewCircuitBreakerMiddlewareWithSubscriber(logging.NoOp, remote, sd.FixedSubscriber{"h1"})
	backendErr := errors.New("backend error")
	release := make(chan struct{})
	probeStarted := make(chan struct{})
	fail := true
	p := mw(func(_ context.Context, _ *Request) (*Response, error) {
		if fail {
			return nil, backendErr
		}
		close(probeStarted)
		<-release
		return &Response{}, nil
	})

	if _, err := p(context.Background(), &Request{}); err != backendErr {
		t.Fatalf("expected the backend error, got: %v", err)
	}
	time.Sleep(60 * time.Millisecond)

	// half-open: the single allowed in-flight request blocks
	fail = false
	done := make(chan error, 1)
	go func() {
		_, err := p(context.Background(), &Request{})
		done <- err
	}()
	<-probeStarted

	// a second request must be rejected while the probe is in-flight
	if _, err := p(context.Background(), &Request{}); err != ErrCircuitBreakerOpen {
		t.Errorf("expected the extra half-open request to be rejected, got: %v", err)
	}
	close(release)
	if err := <-done; err != nil {
		t.Errorf("expected the probe request to succeed, got: %v", err)
	}
}

func TestCircuitBreakerMiddleware_subscriberTripsAndRecovers(t *testing.T) {
	remote := cbConfig(100, time.Hour, 1)
	hosts := []string{"h1", "h2"}
	subscriber := sd.SubscriberFunc(func() ([]string, error) {
		return hosts, nil
	})
	mw, healthAware := NewCircuitBreakerMiddlewareWithSubscriber(logging.NoOp, remote, subscriber)
	p := mw(func(_ context.Context, _ *Request) (*Response, error) {
		return &Response{}, nil
	})

	if _, err := healthAware.Hosts(); err != nil {
		t.Fatal(err)
	}
	if _, err := p(context.Background(), &Request{}); err != nil {
		t.Fatalf("expected the request to pass, got: %v", err)
	}

	// a host goes down: the circuit opens
	hosts = []string{"h1"}
	if _, err := healthAware.Hosts(); err != nil {
		t.Fatal(err)
	}
	if _, err := p(context.Background(), &Request{}); err != ErrCircuitBreakerOpen {
		t.Fatalf("expected the circuit to be open after a host went down, got: %v", err)
	}

	// the host recovers: the circuit goes half-open and lets the probe through
	hosts = []string{"h1", "h2"}
	if _, err := healthAware.Hosts(); err != nil {
		t.Fatal(err)
	}
	if _, err := p(context.Background(), &Request{}); err != nil {
		t.Fatalf("expected the half-open probe to pass, got: %v", err)
	}
	// the probe succeeded: the circuit is closed again
	if _, err := p(context.Background(), &Request{}); err != nil {
		t.Errorf("expected the circuit to be closed after recovery, got: %v", err)
	}
}

func TestCircuitBreakerMiddleware_subscriberEmptyHostsTrip(t *testing.T) {
	remote := cbConfig(100, time.Hour, 1)
	hosts := []string{"h1"}
	subscriber := sd.SubscriberFunc(func() ([]string, error) {
		return hosts, nil
	})
	mw, healthAware := NewCircuitBreakerMiddlewareWithSubscriber(logging.NoOp, remote, subscriber)
	p := mw(func(_ context.Context, _ *Request) (*Response, error) {
		return &Response{}, nil
	})

	if _, err := healthAware.Hosts(); err != nil {
		t.Fatal(err)
	}
	hosts = []string{}
	if _, err := healthAware.Hosts(); err != nil {
		t.Fatal(err)
	}
	if _, err := p(context.Background(), &Request{}); err != ErrCircuitBreakerOpen {
		t.Errorf("expected the circuit to be open after all hosts went down, got: %v", err)
	}
}

func TestCircuitBreakerMiddleware_concurrentAccess(t *testing.T) {
	remote := cbConfig(5, 10*time.Millisecond, 3)
	mw, healthAware := NewCircuitBreakerMiddlewareWithSubscriber(logging.NoOp, remote, sd.FixedSubscriber{"h1", "h2"})
	backendErr := errors.New("backend error")
	var failMu sync.Mutex
	fail := false
	p := mw(func(_ context.Context, _ *Request) (*Response, error) {
		failMu.Lock()
		f := fail
		failMu.Unlock()
		if f {
			return nil, backendErr
		}
		return &Response{}, nil
	})

	var wg sync.WaitGroup
	for i := 0; i < 64; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			for j := 0; j < 100; j++ {
				failMu.Lock()
				fail = (i+j)%3 == 0
				failMu.Unlock()
				_, _ = p(context.Background(), &Request{})
				_, _ = healthAware.Hosts()
			}
		}(i)
	}
	wg.Wait()
}

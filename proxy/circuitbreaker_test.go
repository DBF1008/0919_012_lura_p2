// SPDX-License-Identifier: Apache-2.0

package proxy

import (
	"context"
	"errors"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/luraproject/lura/v2/config"
	"github.com/luraproject/lura/v2/logging"
	"github.com/luraproject/lura/v2/sd"
)

func TestNewCircuitBreakerMiddleware_closedState(t *testing.T) {
	calls := 0
	mw := NewCircuitBreakerMiddleware(logging.NoOp, &config.Backend{
		CircuitBreaker: config.CircuitBreakerConfig{MaxErrors: 2},
	}, sd.FixedSubscriber{"http://127.0.0.1:8080"})

	p := mw(func(ctx context.Context, r *Request) (*Response, error) {
		calls++
		return &Response{}, nil
	})

	for i := 0; i < 3; i++ {
		if _, err := p(context.Background(), &Request{}); err != nil {
			t.Fatalf("unexpected error on a closed circuit: %s", err)
		}
	}
	if calls != 3 {
		t.Errorf("expected 3 calls to the next proxy, got %d", calls)
	}
}

func TestNewCircuitBreakerMiddleware_opensAfterMaxErrors(t *testing.T) {
	expected := errors.New("backend error")
	calls := 0
	mw := NewCircuitBreakerMiddleware(logging.NoOp, &config.Backend{
		CircuitBreaker: config.CircuitBreakerConfig{
			MaxErrors: 3,
			Timeout:   time.Minute,
		},
	}, sd.FixedSubscriber{"http://127.0.0.1:8080"})

	p := mw(func(ctx context.Context, r *Request) (*Response, error) {
		calls++
		return nil, expected
	})

	for i := 0; i < 3; i++ {
		if _, err := p(context.Background(), &Request{}); err != expected {
			t.Fatalf("expected the backend error to be propagated, got: %v", err)
		}
	}
	if calls != 3 {
		t.Fatalf("expected 3 calls to the next proxy, got %d", calls)
	}

	// the circuit is now open: requests must be rejected without reaching the backend
	for i := 0; i < 5; i++ {
		if _, err := p(context.Background(), &Request{}); err != ErrCircuitOpen {
			t.Errorf("expected ErrCircuitOpen, got: %v", err)
		}
	}
	if calls != 3 {
		t.Errorf("the next proxy was called while the circuit was open (%d calls)", calls)
	}
}

func TestNewCircuitBreakerMiddleware_successResetsErrorCount(t *testing.T) {
	fail := true
	mw := NewCircuitBreakerMiddleware(logging.NoOp, &config.Backend{
		CircuitBreaker: config.CircuitBreakerConfig{
			MaxErrors: 2,
			Timeout:   time.Minute,
		},
	}, sd.FixedSubscriber{"http://127.0.0.1:8080"})

	p := mw(func(ctx context.Context, r *Request) (*Response, error) {
		if fail {
			return nil, errors.New("backend error")
		}
		return &Response{}, nil
	})

	for i := 0; i < 10; i++ {
		if _, err := p(context.Background(), &Request{}); err == nil {
			t.Fatal("expected a backend error")
		}
		fail = false
		if _, err := p(context.Background(), &Request{}); err != nil {
			t.Fatalf("unexpected error: %s", err)
		}
		fail = true
	}
}

func TestNewCircuitBreakerMiddleware_halfOpenAfterTimeout(t *testing.T) {
	fail := true
	mw := NewCircuitBreakerMiddleware(logging.NoOp, &config.Backend{
		CircuitBreaker: config.CircuitBreakerConfig{
			MaxErrors: 1,
			Timeout:   50 * time.Millisecond,
		},
	}, sd.FixedSubscriber{"http://127.0.0.1:8080"})

	p := mw(func(ctx context.Context, r *Request) (*Response, error) {
		if fail {
			return nil, errors.New("backend error")
		}
		return &Response{}, nil
	})

	if _, err := p(context.Background(), &Request{}); err == nil {
		t.Fatal("expected a backend error")
	}
	if _, err := p(context.Background(), &Request{}); err != ErrCircuitOpen {
		t.Fatalf("expected ErrCircuitOpen, got: %v", err)
	}

	// wait for the recovery timeout so the circuit moves to half-open
	time.Sleep(60 * time.Millisecond)

	// the backend has recovered: the trial request goes through and closes the circuit
	fail = false
	if _, err := p(context.Background(), &Request{}); err != nil {
		t.Fatalf("expected the half-open trial request to reach the backend, got: %v", err)
	}
	for i := 0; i < 3; i++ {
		if _, err := p(context.Background(), &Request{}); err != nil {
			t.Fatalf("expected the circuit to be closed again, got: %v", err)
		}
	}
}

func TestNewCircuitBreakerMiddleware_halfOpenFailureReopens(t *testing.T) {
	mw := NewCircuitBreakerMiddleware(logging.NoOp, &config.Backend{
		CircuitBreaker: config.CircuitBreakerConfig{
			MaxErrors: 1,
			Timeout:   50 * time.Millisecond,
		},
	}, sd.FixedSubscriber{"http://127.0.0.1:8080"})

	p := mw(func(ctx context.Context, r *Request) (*Response, error) {
		return nil, errors.New("backend error")
	})

	if _, err := p(context.Background(), &Request{}); err == nil {
		t.Fatal("expected a backend error")
	}
	time.Sleep(60 * time.Millisecond)

	// the half-open trial request fails: the circuit must open again
	if _, err := p(context.Background(), &Request{}); err == nil {
		t.Fatal("expected a backend error from the half-open trial request")
	}
	if _, err := p(context.Background(), &Request{}); err != ErrCircuitOpen {
		t.Fatalf("expected the circuit to be open again, got: %v", err)
	}
}

func TestNewCircuitBreakerMiddleware_disabled(t *testing.T) {
	calls := 0
	mw := NewCircuitBreakerMiddleware(logging.NoOp, &config.Backend{
		CircuitBreaker: config.CircuitBreakerConfig{
			Disabled:  true,
			MaxErrors: 1,
			Timeout:   time.Minute,
		},
	}, sd.FixedSubscriber{"http://127.0.0.1:8080"})

	p := mw(func(ctx context.Context, r *Request) (*Response, error) {
		calls++
		return nil, errors.New("backend error")
	})

	for i := 0; i < 10; i++ {
		if _, err := p(context.Background(), &Request{}); err == nil {
			t.Fatal("expected the backend error to be propagated")
		}
	}
	if calls != 10 {
		t.Errorf("a disabled circuit breaker must never reject requests, rejected %d", 10-calls)
	}
}

// flakySubscriber simulates a service discovery subscriber where hosts go
// down and come back
type flakySubscriber struct {
	mu    sync.Mutex
	hosts []string
}

func (f *flakySubscriber) Hosts() ([]string, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	res := make([]string, len(f.hosts))
	copy(res, f.hosts)
	return res, nil
}

func (f *flakySubscriber) set(hosts ...string) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.hosts = hosts
}

func TestNewCircuitBreakerMiddleware_subscriberIntegration(t *testing.T) {
	subscriber := &flakySubscriber{hosts: []string{"http://h1:8080", "http://h2:8080"}}
	mw := NewCircuitBreakerMiddleware(logging.NoOp, &config.Backend{
		CircuitBreaker: config.CircuitBreakerConfig{
			MaxErrors: 100,
			Timeout:   time.Minute,
			Interval:  10 * time.Millisecond,
		},
	}, subscriber)

	calls := 0
	p := mw(func(ctx context.Context, r *Request) (*Response, error) {
		calls++
		return &Response{}, nil
	})

	if _, err := p(context.Background(), &Request{}); err != nil {
		t.Fatalf("unexpected error: %s", err)
	}

	// a host goes down: the watcher must open the circuit
	subscriber.set("http://h2:8080")
	waitFor(t, time.Second, func() bool {
		_, err := p(context.Background(), &Request{})
		return err == ErrCircuitOpen
	})

	// the host recovers: the watcher must move the circuit to half-open so a
	// successful trial request closes it again
	subscriber.set("http://h1:8080", "http://h2:8080")
	waitFor(t, time.Second, func() bool {
		_, err := p(context.Background(), &Request{})
		return err == nil
	})
}

func TestNewCircuitBreakerMiddleware_subscriberEmpties(t *testing.T) {
	subscriber := &flakySubscriber{hosts: []string{"http://h1:8080"}}
	mw := NewCircuitBreakerMiddleware(logging.NoOp, &config.Backend{
		CircuitBreaker: config.CircuitBreakerConfig{
			MaxErrors: 100,
			Timeout:   time.Minute,
			Interval:  10 * time.Millisecond,
		},
	}, subscriber)

	p := mw(func(ctx context.Context, r *Request) (*Response, error) {
		return &Response{}, nil
	})

	if _, err := p(context.Background(), &Request{}); err != nil {
		t.Fatalf("unexpected error: %s", err)
	}

	// all hosts gone: the circuit must open
	subscriber.set()
	waitFor(t, time.Second, func() bool {
		_, err := p(context.Background(), &Request{})
		return err == ErrCircuitOpen
	})
}

func TestNewCircuitBreakerMiddleware_concurrent(t *testing.T) {
	mw := NewCircuitBreakerMiddleware(logging.NoOp, &config.Backend{
		CircuitBreaker: config.CircuitBreakerConfig{
			MaxErrors:             5,
			Timeout:               20 * time.Millisecond,
			MaxConcurrentRequests: 2,
		},
	}, sd.FixedSubscriber{"http://127.0.0.1:8080"})

	var fail atomic.Bool
	p := mw(func(ctx context.Context, r *Request) (*Response, error) {
		time.Sleep(time.Millisecond)
		if fail.Load() {
			return nil, errors.New("backend error")
		}
		return &Response{}, nil
	})

	var wg sync.WaitGroup
	for i := 0; i < 50; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			if i%3 == 0 {
				fail.Store(true)
			} else {
				fail.Store(false)
			}
			// requests either reach the backend or are rejected by the breaker;
			// both outcomes are fine, this test only checks for data races and
			// deadlocks under concurrent state transitions
			_, _ = p(context.Background(), &Request{})
		}(i)
	}
	wg.Wait()
}

func TestCircuitBreaker_halfOpenConcurrencyLimit(t *testing.T) {
	cb := newCircuitBreaker(logging.NoOp, "test", config.CircuitBreakerConfig{
		MaxErrors:             1,
		Timeout:               20 * time.Millisecond,
		MaxConcurrentRequests: 2,
	})

	cb.onFailure()
	if cb.currentState() != stateOpen {
		t.Fatalf("expected the circuit to be open, got: %s", cb.currentState())
	}
	if cb.allow() {
		t.Fatal("an open circuit must reject requests")
	}

	time.Sleep(30 * time.Millisecond)

	// only MaxConcurrentRequests trial requests are allowed in half-open
	if !cb.allow() || !cb.allow() {
		t.Fatal("the first half-open trial requests must be allowed")
	}
	if cb.allow() {
		t.Fatal("half-open trial requests over the limit must be rejected")
	}

	// a failure in half-open reopens the circuit
	cb.onFailure()
	if cb.currentState() != stateOpen {
		t.Fatalf("expected the circuit to reopen, got: %s", cb.currentState())
	}

	time.Sleep(30 * time.Millisecond)
	if !cb.allow() {
		t.Fatal("expected a half-open trial request after the timeout")
	}
	cb.onSuccess()
	if cb.currentState() != stateClosed {
		t.Fatalf("expected the circuit to be closed, got: %s", cb.currentState())
	}
}

func waitFor(t *testing.T, timeout time.Duration, cond func() bool) {
	t.Helper()
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		if cond() {
			return
		}
		time.Sleep(5 * time.Millisecond)
	}
	t.Fatal("condition not met before the timeout")
}

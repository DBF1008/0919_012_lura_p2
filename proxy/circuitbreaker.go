// SPDX-License-Identifier: Apache-2.0

package proxy

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"time"

	"github.com/luraproject/lura/v2/config"
	"github.com/luraproject/lura/v2/logging"
	"github.com/luraproject/lura/v2/sd"
)

// ErrCircuitBreakerOpen is the error returned when the circuit breaker is open
// and the request is rejected before being sent to the backend
var ErrCircuitBreakerOpen = errors.New("circuit breaker is open")

// breakerState represents the state of a circuit breaker
type breakerState int32

const (
	// breakerClosed is the state of a circuit breaker letting all the requests through
	breakerClosed breakerState = iota
	// breakerOpen is the state of a circuit breaker rejecting all the requests
	breakerOpen
	// breakerHalfOpen is the state of a circuit breaker letting a limited amount
	// of requests through in order to check if the backend has recovered
	breakerHalfOpen
)

func (s breakerState) String() string {
	switch s {
	case breakerClosed:
		return "CLOSED"
	case breakerOpen:
		return "OPEN"
	case breakerHalfOpen:
		return "HALF-OPEN"
	}
	return "UNKNOWN"
}

// circuitBreaker is a thread-safe three-state (closed, open, half-open) circuit breaker
type circuitBreaker struct {
	name                  string
	logger                logging.Logger
	maxErrors             int
	timeout               time.Duration
	halfOpenMaxConcurrent int

	mu                sync.Mutex
	state             breakerState
	consecutiveErrors int
	openedAt          time.Time
	halfOpenInFlight  int
	halfOpenSuccesses int
}

func newCircuitBreaker(logger logging.Logger, remote *config.Backend) *circuitBreaker {
	return &circuitBreaker{
		name:                  remote.CircuitBreaker.Name,
		logger:                logger,
		maxErrors:             remote.CircuitBreaker.MaxErrors,
		timeout:               remote.CircuitBreaker.Timeout,
		halfOpenMaxConcurrent: remote.CircuitBreaker.MaxConcurrentRequests,
		state:                 breakerClosed,
	}
}

// allow reports whether a request is allowed to proceed, tracking the in-flight
// requests while half-open
func (cb *circuitBreaker) allow() bool {
	cb.mu.Lock()
	defer cb.mu.Unlock()

	switch cb.state {
	case breakerOpen:
		if time.Since(cb.openedAt) < cb.timeout {
			return false
		}
		cb.setState(breakerHalfOpen)
		fallthrough
	case breakerHalfOpen:
		if cb.halfOpenInFlight >= cb.halfOpenMaxConcurrent {
			return false
		}
		cb.halfOpenInFlight++
		return true
	default:
		return true
	}
}

// onSuccess records a successful request
func (cb *circuitBreaker) onSuccess() {
	cb.mu.Lock()
	defer cb.mu.Unlock()

	switch cb.state {
	case breakerHalfOpen:
		cb.halfOpenInFlight--
		cb.halfOpenSuccesses++
		if cb.halfOpenSuccesses >= cb.halfOpenMaxConcurrent {
			cb.setState(breakerClosed)
		}
	case breakerClosed:
		cb.consecutiveErrors = 0
	}
}

// onFailure records a failed request
func (cb *circuitBreaker) onFailure() {
	cb.mu.Lock()
	defer cb.mu.Unlock()

	switch cb.state {
	case breakerHalfOpen:
		cb.halfOpenInFlight--
		cb.setState(breakerOpen)
	case breakerClosed:
		cb.consecutiveErrors++
		if cb.consecutiveErrors >= cb.maxErrors {
			cb.setState(breakerOpen)
		}
	}
}

// trip forces the circuit open. It is called when the service discovery layer
// reports backend hosts going down
func (cb *circuitBreaker) trip() {
	cb.mu.Lock()
	defer cb.mu.Unlock()
	if cb.state != breakerOpen {
		cb.setState(breakerOpen)
	}
}

// recover moves the circuit into the half-open state, so traffic is ramped up
// gradually. It is called when the service discovery layer reports the backend
// hosts are back
func (cb *circuitBreaker) recover() {
	cb.mu.Lock()
	defer cb.mu.Unlock()
	if cb.state == breakerOpen {
		cb.setState(breakerHalfOpen)
	}
}

// setState moves the breaker into the received state, resetting the counters
// tied to the previous one. The caller must hold the mutex
func (cb *circuitBreaker) setState(state breakerState) {
	if cb.state == state {
		return
	}
	cb.logger.Info(fmt.Sprintf("[BACKEND: %s] circuit breaker state change: %s -> %s", cb.name, cb.state, state))
	cb.state = state
	switch state {
	case breakerOpen:
		cb.openedAt = time.Now()
		cb.consecutiveErrors = 0
		cb.halfOpenInFlight = 0
		cb.halfOpenSuccesses = 0
	case breakerHalfOpen:
		cb.halfOpenInFlight = 0
		cb.halfOpenSuccesses = 0
	case breakerClosed:
		cb.consecutiveErrors = 0
		cb.halfOpenInFlight = 0
		cb.halfOpenSuccesses = 0
	}
}

// middleware returns a proxy middleware rejecting the requests while the
// circuit is open and recording the result of the let-through ones
func (cb *circuitBreaker) middleware() Middleware {
	return func(next ...Proxy) Proxy {
		if len(next) > 1 {
			cb.logger.Fatal(fmt.Sprintf("too many proxies for this proxy middleware: circuitBreaker only accepts 1 proxy, got %d", len(next)))
			return nil
		}
		return func(ctx context.Context, request *Request) (*Response, error) {
			if !cb.allow() {
				return nil, ErrCircuitBreakerOpen
			}
			response, err := next[0](ctx, request)
			if err != nil {
				cb.onFailure()
			} else {
				cb.onSuccess()
			}
			return response, err
		}
	}
}

// NewCircuitBreakerMiddlewareWithSubscriber creates a circuit breaker middleware
// over the received backend configuration, along with a subscriber wrapping the
// received one so the breaker is tripped when the service discovery layer reports
// hosts going down and moved into the half-open state when they recover.
//
// If the circuit breaker is disabled (max_errors <= 0), it returns an empty
// middleware and the received subscriber untouched
func NewCircuitBreakerMiddlewareWithSubscriber(logger logging.Logger, remote *config.Backend, subscriber sd.Subscriber) (Middleware, sd.Subscriber) {
	if remote.CircuitBreaker.MaxErrors <= 0 {
		return emptyMiddlewareFallback(logger), subscriber
	}
	cb := newCircuitBreaker(logger, remote)
	return cb.middleware(), newHealthSubscriber(subscriber, cb)
}

// healthSubscriber is a subscriber wrapping another one, reporting the changes
// observed in the backend host set to a circuit breaker
type healthSubscriber struct {
	inner sd.Subscriber
	cb    *circuitBreaker

	mu          sync.Mutex
	initialized bool
	last        map[string]struct{}
}

func newHealthSubscriber(inner sd.Subscriber, cb *circuitBreaker) *healthSubscriber {
	return &healthSubscriber{
		inner: inner,
		cb:    cb,
		last:  map[string]struct{}{},
	}
}

// Hosts implements the sd.Subscriber interface, tripping the circuit breaker
// when hosts go down and recovering it (half-open) when they come back
func (h *healthSubscriber) Hosts() ([]string, error) {
	hosts, err := h.inner.Hosts()

	h.mu.Lock()
	defer h.mu.Unlock()

	if err != nil || len(hosts) == 0 {
		if h.initialized && len(h.last) > 0 {
			h.cb.trip()
		}
		h.initialized = true
		h.last = map[string]struct{}{}
		return hosts, err
	}

	current := make(map[string]struct{}, len(hosts))
	for _, host := range hosts {
		current[host] = struct{}{}
	}

	if h.initialized {
		switch {
		case len(current) < len(h.last):
			h.cb.trip()
		case len(current) > len(h.last):
			h.cb.recover()
		}
	}
	h.initialized = true
	h.last = current
	return hosts, nil
}

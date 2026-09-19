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

// ErrCircuitOpen is the error returned by the circuit breaker middleware when
// the circuit is open and no request is allowed to reach the backend
var ErrCircuitOpen = errors.New("circuit breaker is open")

const (
	// defaultCircuitBreakerMaxErrors is the default number of consecutive
	// errors tolerated before the circuit is opened
	defaultCircuitBreakerMaxErrors = 5
	// defaultCircuitBreakerTimeout is the default time the circuit stays
	// fully open before moving to the half-open state
	defaultCircuitBreakerTimeout = 30 * time.Second
	// defaultCircuitBreakerInterval is the default polling interval used to
	// watch the subscriber host set
	defaultCircuitBreakerInterval = time.Second
	// defaultCircuitBreakerMaxConcurrentRequests is the default number of
	// in-flight requests allowed while the circuit is half-open
	defaultCircuitBreakerMaxConcurrentRequests = 1
)

// breakerState represents the state of a circuit breaker: closed, open or half-open
type breakerState int

const (
	// stateClosed means the circuit is closed and requests flow normally
	stateClosed breakerState = iota
	// stateOpen means the circuit is open and requests are rejected
	stateOpen
	// stateHalfOpen means the circuit is half-open and a limited number of
	// trial requests are allowed through
	stateHalfOpen
)

func (s breakerState) String() string {
	switch s {
	case stateClosed:
		return "closed"
	case stateOpen:
		return "open"
	case stateHalfOpen:
		return "half-open"
	}
	return "unknown"
}

// circuitBreaker is a concurrency-safe, three-state (closed/open/half-open)
// circuit breaker. All its public methods are safe for concurrent use
type circuitBreaker struct {
	logger                logging.Logger
	name                  string
	maxErrors             int
	timeout               time.Duration
	maxConcurrentRequests int

	mu                sync.Mutex
	state             breakerState
	consecutiveErrors int
	openedAt          time.Time
	halfOpenRequests  int
}

func newCircuitBreaker(logger logging.Logger, name string, cfg config.CircuitBreakerConfig) *circuitBreaker {
	maxErrors := cfg.MaxErrors
	if maxErrors <= 0 {
		maxErrors = defaultCircuitBreakerMaxErrors
	}
	timeout := cfg.Timeout
	if timeout <= 0 {
		timeout = defaultCircuitBreakerTimeout
	}
	maxConcurrentRequests := cfg.MaxConcurrentRequests
	if maxConcurrentRequests <= 0 {
		maxConcurrentRequests = defaultCircuitBreakerMaxConcurrentRequests
	}
	return &circuitBreaker{
		logger:                logger,
		name:                  name,
		maxErrors:             maxErrors,
		timeout:               timeout,
		maxConcurrentRequests: maxConcurrentRequests,
		state:                 stateClosed,
	}
}

// allow reports whether a request is allowed to reach the backend. It must be
// called before sending the request and, if it returns true, the caller must
// report the result of the request with either onSuccess or onFailure
func (cb *circuitBreaker) allow() bool {
	cb.mu.Lock()
	defer cb.mu.Unlock()

	switch cb.state {
	case stateOpen:
		if time.Since(cb.openedAt) < cb.timeout {
			return false
		}
		cb.setStateLocked(stateHalfOpen)
		cb.halfOpenRequests = 0
	case stateHalfOpen:
		if cb.halfOpenRequests >= cb.maxConcurrentRequests {
			return false
		}
	}

	if cb.state == stateHalfOpen {
		cb.halfOpenRequests++
	}
	return true
}

// onSuccess reports a successful request to the backend
func (cb *circuitBreaker) onSuccess() {
	cb.mu.Lock()
	defer cb.mu.Unlock()

	cb.consecutiveErrors = 0
	if cb.state == stateHalfOpen {
		cb.halfOpenRequests--
		cb.setStateLocked(stateClosed)
	}
}

// onFailure reports a failed request to the backend
func (cb *circuitBreaker) onFailure() {
	cb.mu.Lock()
	defer cb.mu.Unlock()

	cb.consecutiveErrors++
	switch cb.state {
	case stateHalfOpen:
		cb.halfOpenRequests--
		cb.openLocked()
	case stateClosed:
		if cb.consecutiveErrors >= cb.maxErrors {
			cb.openLocked()
		}
	}
}

// trip forces the circuit into the open state. It is used when the service
// discovery layer reports that one or more backend hosts went down
func (cb *circuitBreaker) trip() {
	cb.mu.Lock()
	defer cb.mu.Unlock()

	if cb.state != stateOpen {
		cb.logger.Warning(fmt.Sprintf("circuit breaker [%s]: backend host(s) down, opening the circuit", cb.name))
	}
	cb.openLocked()
}

// cooldown moves the circuit from the open state into the half-open state so
// traffic is gradually restored. It is used when the service discovery layer
// reports that previously lost backend hosts are back
func (cb *circuitBreaker) cooldown() {
	cb.mu.Lock()
	defer cb.mu.Unlock()

	if cb.state == stateOpen {
		cb.setStateLocked(stateHalfOpen)
		cb.halfOpenRequests = 0
	}
}

// state returns the current state of the circuit breaker
func (cb *circuitBreaker) currentState() breakerState {
	cb.mu.Lock()
	defer cb.mu.Unlock()
	return cb.state
}

func (cb *circuitBreaker) openLocked() {
	cb.setStateLocked(stateOpen)
	cb.openedAt = time.Now()
	cb.consecutiveErrors = 0
}

func (cb *circuitBreaker) setStateLocked(state breakerState) {
	if cb.state != state {
		cb.logger.Info(fmt.Sprintf("circuit breaker [%s]: state changed %s -> %s", cb.name, cb.state, state))
	}
	cb.state = state
}

// watch polls the subscriber and trips the breaker when hosts go down or moves
// it to the half-open state when previously lost hosts come back
func (cb *circuitBreaker) watch(subscriber sd.Subscriber, interval time.Duration) {
	if interval <= 0 {
		interval = defaultCircuitBreakerInterval
	}
	last, err := subscriber.Hosts()
	if err != nil || len(last) == 0 {
		cb.trip()
	}
	go func() {
		ticker := time.NewTicker(interval)
		defer ticker.Stop()
		for range ticker.C {
			hosts, err := subscriber.Hosts()
			if err != nil || len(hosts) == 0 {
				cb.trip()
				last = hosts
				continue
			}
			switch {
			case len(hosts) < len(last):
				cb.trip()
			case len(hosts) > len(last):
				cb.cooldown()
			}
			last = hosts
		}
	}()
}

// NewCircuitBreakerMiddleware creates a proxy middleware protecting the backend
// with a three-state (closed/open/half-open) circuit breaker. The breaker is
// checked before the request is sent to the backend, so requests are rejected
// with ErrCircuitOpen while the backend is considered down. The middleware also
// watches the subscriber so hosts reported as down by the service discovery
// layer open the circuit and recovered hosts move it to the half-open state
func NewCircuitBreakerMiddleware(logger logging.Logger, remote *config.Backend, subscriber sd.Subscriber) Middleware {
	if remote.CircuitBreaker.Disabled {
		return func(next ...Proxy) Proxy {
			if len(next) > 1 {
				logger.Fatal(fmt.Sprintf("too many proxies for this %s %s -> %s proxy middleware: NewCircuitBreakerMiddleware only accepts 1 proxy, got %d",
					remote.ParentEndpointMethod, remote.ParentEndpoint, remote.URLPattern, len(next)))
				return nil
			}
			return next[0]
		}
	}

	name := fmt.Sprintf("%s %s -> %s", remote.ParentEndpointMethod, remote.ParentEndpoint, remote.URLPattern)
	cb := newCircuitBreaker(logger, name, remote.CircuitBreaker)
	cb.watch(subscriber, remote.CircuitBreaker.Interval)

	return func(next ...Proxy) Proxy {
		if len(next) > 1 {
			logger.Fatal(fmt.Sprintf("too many proxies for this %s %s -> %s proxy middleware: NewCircuitBreakerMiddleware only accepts 1 proxy, got %d",
				remote.ParentEndpointMethod, remote.ParentEndpoint, remote.URLPattern, len(next)))
			return nil
		}
		return func(ctx context.Context, request *Request) (*Response, error) {
			if !cb.allow() {
				return nil, ErrCircuitOpen
			}
			resp, err := next[0](ctx, request)
			if err != nil {
				cb.onFailure()
				return resp, err
			}
			cb.onSuccess()
			return resp, nil
		}
	}
}

package algorithms

import (
	"container/list"
	"sync"
	"time"
)

// SlidingWindow is a log-based sliding-window rate limiter. Concurrent calls
// to Allow are safe.
//
// Smoother than TokenBucket — strictly enforces "no more than `Limit` events
// in any rolling `Window`-duration interval." Higher memory cost: up to
// `Limit` timestamps stored per key.
//
// Behavior contrast:
//   - TokenBucket allows bursts up to capacity, then refills smoothly.
//   - SlidingWindow allows up to Limit anywhere inside any rolling window;
//     no burst-allowance separate from the rate.
type SlidingWindow struct {
	limit  int
	window time.Duration
	clock  Clock

	mu  sync.Mutex
	log *list.List // doubly-linked list of time.Time (oldest at Front)
}

// NewSlidingWindow constructs an empty window.
//
// limit and window must be > 0. clock may be nil (defaults to time.Now).
func NewSlidingWindow(limit int, window time.Duration, clock Clock) (*SlidingWindow, error) {
	if limit <= 0 || window <= 0 {
		return nil, ErrInvalidConfig
	}
	if clock == nil {
		clock = time.Now
	}
	return &SlidingWindow{
		limit:  limit,
		window: window,
		clock:  clock,
		log:    list.New(),
	}, nil
}

// Allow records n events if the rolling window has room. Returns the verdict.
//
// Returns ErrInvalidTokens if n <= 0.
func (s *SlidingWindow) Allow(n int) (Verdict, error) {
	if n <= 0 {
		return Verdict{}, ErrInvalidTokens
	}

	s.mu.Lock()
	defer s.mu.Unlock()

	now := s.clock()
	s.trimLocked(now)

	if s.log.Len()+n > s.limit {
		// Deny. Retry-after is when the oldest in-window event ages out.
		var wait time.Duration
		if oldest := s.log.Front(); oldest != nil {
			wait = oldest.Value.(time.Time).Add(s.window).Sub(now)
			if wait < 0 {
				wait = 0
			}
		}
		return Verdict{
			Allowed:      false,
			Remaining:    max(0, s.limit-s.log.Len()),
			RetryAfterMs: int(wait / time.Millisecond),
			WaitFor:      wait,
		}, nil
	}

	for i := 0; i < n; i++ {
		s.log.PushBack(now)
	}
	return Verdict{
		Allowed:   true,
		Remaining: s.limit - s.log.Len(),
	}, nil
}

// trimLocked drops entries older than `now - window`.
//
// Caller MUST hold s.mu.
func (s *SlidingWindow) trimLocked(now time.Time) {
	cutoff := now.Add(-s.window)
	for {
		front := s.log.Front()
		if front == nil {
			return
		}
		if !front.Value.(time.Time).Before(cutoff) {
			return
		}
		s.log.Remove(front)
	}
}

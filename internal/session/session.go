// Package session tracks live client sessions and the ephemeral znodes
// they own. It is deliberately transport-agnostic: the server layer drives
// heartbeats and expiry, and converts expired sessions' ephemeral paths
// into Delete transactions.
package session

import (
	"errors"
	"sort"
	"sync"
	"time"

	"kansi/internal/tree"
)

// Session-layer errors.
var (
	ErrUnknownSession = errors.New("unknown session")
	ErrInvalidTimeout = errors.New("timeout must be positive")
)

// Session is the server-side record of a live client session.
type Session struct {
	ID         tree.SessionID
	Timeout    time.Duration
	Deadline   time.Time
	Ephemerals map[string]struct{}
}

// Expired reports a session that was removed by Expire, along with the
// ephemeral paths it owned so the caller can issue delete txns for them.
type Expired struct {
	ID    tree.SessionID
	Paths []string
}

// Manager tracks live sessions. All methods are safe for concurrent use.
type Manager struct {
	mu       sync.Mutex
	clock    Clock
	nextID   int64
	sessions map[tree.SessionID]*Session
}

// NewManager returns a Manager that measures deadlines against clock.
func NewManager(clock Clock) *Manager {
	return &Manager{
		clock:    clock,
		nextID:   1,
		sessions: make(map[tree.SessionID]*Session),
	}
}

// Open allocates a fresh session with the given timeout. The initial
// deadline is now + timeout.
func (m *Manager) Open(timeout time.Duration) (tree.SessionID, error) {
	if timeout <= 0 {
		return 0, ErrInvalidTimeout
	}
	m.mu.Lock()
	defer m.mu.Unlock()

	id := tree.SessionID(m.nextID)
	m.nextID++
	m.sessions[id] = &Session{
		ID:         id,
		Timeout:    timeout,
		Deadline:   m.clock.Now().Add(timeout),
		Ephemerals: make(map[string]struct{}),
	}
	return id, nil
}

// Renew pushes the session's deadline to now + its configured timeout.
func (m *Manager) Renew(id tree.SessionID) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	s, ok := m.sessions[id]
	if !ok {
		return ErrUnknownSession
	}
	s.Deadline = m.clock.Now().Add(s.Timeout)
	return nil
}

// AddEphemeral records that the session owns the znode at path. Call
// this after the matching create txn has been applied.
func (m *Manager) AddEphemeral(id tree.SessionID, path string) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	s, ok := m.sessions[id]
	if !ok {
		return ErrUnknownSession
	}
	s.Ephemerals[path] = struct{}{}
	return nil
}

// RemoveEphemeral drops a path from the session's owned set. Used when
// an ephemeral is deleted for a reason other than session expiry.
func (m *Manager) RemoveEphemeral(id tree.SessionID, path string) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	s, ok := m.sessions[id]
	if !ok {
		return ErrUnknownSession
	}
	delete(s.Ephemerals, path)
	return nil
}

// Close removes the session and returns the ephemeral paths it owned.
func (m *Manager) Close(id tree.SessionID) ([]string, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	s, ok := m.sessions[id]
	if !ok {
		return nil, ErrUnknownSession
	}
	delete(m.sessions, id)
	return sortedPaths(s.Ephemerals), nil
}

// Get returns a copy of the session record. The Ephemerals map in the
// returned value is independent of the manager's internal state.
func (m *Manager) Get(id tree.SessionID) (Session, bool) {
	m.mu.Lock()
	defer m.mu.Unlock()
	s, ok := m.sessions[id]
	if !ok {
		return Session{}, false
	}
	return Session{
		ID:         s.ID,
		Timeout:    s.Timeout,
		Deadline:   s.Deadline,
		Ephemerals: copySet(s.Ephemerals),
	}, true
}

// Expire removes every session whose deadline is at or before the current
// clock time, and returns their IDs + owned paths sorted by ID.
func (m *Manager) Expire() []Expired {
	m.mu.Lock()
	defer m.mu.Unlock()
	now := m.clock.Now()
	var out []Expired
	for id, s := range m.sessions {
		if !now.Before(s.Deadline) {
			out = append(out, Expired{ID: id, Paths: sortedPaths(s.Ephemerals)})
			delete(m.sessions, id)
		}
	}
	sort.Slice(out, func(i, j int) bool { return out[i].ID < out[j].ID })
	return out
}

// Len returns the number of live sessions. Intended for tests and metrics.
func (m *Manager) Len() int {
	m.mu.Lock()
	defer m.mu.Unlock()
	return len(m.sessions)
}

func sortedPaths(set map[string]struct{}) []string {
	if len(set) == 0 {
		return nil
	}
	out := make([]string, 0, len(set))
	for p := range set {
		out = append(out, p)
	}
	sort.Strings(out)
	return out
}

func copySet(set map[string]struct{}) map[string]struct{} {
	out := make(map[string]struct{}, len(set))
	for k := range set {
		out[k] = struct{}{}
	}
	return out
}

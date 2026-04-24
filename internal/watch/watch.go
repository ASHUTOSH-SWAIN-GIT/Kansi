// Package watch implements ZooKeeper one-shot watches.
//
// A client may install three kinds of watches:
//   - data watch on a path (fires on SetData, Delete, Create-of-this-path)
//   - exists watch on a path (fires on Create, Delete, SetData)
//   - child watch on a path (fires on Create/Delete of any direct child)
//
// Watches are one-shot: the first event that matches fires and removes
// the watch. A session may register at most one watch of each kind on
// each path.
package watch

import (
	"sync"

	"kansi/internal/proto"
	"kansi/internal/tree"
)

// Kind categorises a watch.
type Kind int

// Watch kinds.
const (
	Data Kind = iota
	Exists
	Children
)

// Event is a fired watch: which session it belonged to and what to send.
type Event struct {
	Session tree.SessionID
	Event   proto.WatchEvent
}

// Manager stores and fires watches. Safe for concurrent use.
type Manager struct {
	mu sync.Mutex

	// path -> set of sessions with a pending watch of this kind
	data     map[string]map[tree.SessionID]struct{}
	exists   map[string]map[tree.SessionID]struct{}
	children map[string]map[tree.SessionID]struct{}
}

// NewManager returns an empty watch manager.
func NewManager() *Manager {
	return &Manager{
		data:     make(map[string]map[tree.SessionID]struct{}),
		exists:   make(map[string]map[tree.SessionID]struct{}),
		children: make(map[string]map[tree.SessionID]struct{}),
	}
}

// Add installs a watch.
func (m *Manager) Add(k Kind, path string, sid tree.SessionID) {
	m.mu.Lock()
	defer m.mu.Unlock()
	bucket := m.pick(k)
	set, ok := bucket[path]
	if !ok {
		set = make(map[tree.SessionID]struct{})
		bucket[path] = set
	}
	set[sid] = struct{}{}
}

// RemoveSession drops every watch belonging to sid. Called on session close.
func (m *Manager) RemoveSession(sid tree.SessionID) {
	m.mu.Lock()
	defer m.mu.Unlock()
	for _, bucket := range []map[string]map[tree.SessionID]struct{}{m.data, m.exists, m.children} {
		for path, set := range bucket {
			delete(set, sid)
			if len(set) == 0 {
				delete(bucket, path)
			}
		}
	}
}

// FireCreate reports creation of path. Fires exists-watches on path and
// child-watches on its parent.
func (m *Manager) FireCreate(path, parent string) []Event {
	m.mu.Lock()
	defer m.mu.Unlock()
	var out []Event
	out = m.drain(out, m.exists, path, proto.EventNodeCreated)
	out = m.drain(out, m.children, parent, proto.EventNodeChildrenChanged)
	return out
}

// FireDelete reports deletion of path. Fires data, exists, and children
// watches on path; child-watches on parent.
func (m *Manager) FireDelete(path, parent string) []Event {
	m.mu.Lock()
	defer m.mu.Unlock()
	var out []Event
	out = m.drain(out, m.data, path, proto.EventNodeDeleted)
	out = m.drain(out, m.exists, path, proto.EventNodeDeleted)
	out = m.drain(out, m.children, path, proto.EventNodeDeleted)
	out = m.drain(out, m.children, parent, proto.EventNodeChildrenChanged)
	return out
}

// FireSetData reports a data mutation on path.
func (m *Manager) FireSetData(path string) []Event {
	m.mu.Lock()
	defer m.mu.Unlock()
	var out []Event
	out = m.drain(out, m.data, path, proto.EventNodeDataChanged)
	out = m.drain(out, m.exists, path, proto.EventNodeDataChanged)
	return out
}

func (m *Manager) pick(k Kind) map[string]map[tree.SessionID]struct{} {
	switch k {
	case Data:
		return m.data
	case Exists:
		return m.exists
	case Children:
		return m.children
	}
	return nil
}

func (m *Manager) drain(out []Event, bucket map[string]map[tree.SessionID]struct{}, path string, evt proto.EventType) []Event {
	set, ok := bucket[path]
	if !ok {
		return out
	}
	delete(bucket, path)
	for sid := range set {
		out = append(out, Event{Session: sid, Event: proto.WatchEvent{Type: evt, Path: path}})
	}
	return out
}

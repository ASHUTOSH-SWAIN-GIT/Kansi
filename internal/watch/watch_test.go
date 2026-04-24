package watch

import (
	"testing"

	"kansi/internal/proto"
	"kansi/internal/tree"
)

func TestDataWatchFiresOnce(t *testing.T) {
	m := NewManager()
	m.Add(Data, "/a", tree.SessionID(1))

	evts := m.FireSetData("/a")
	if len(evts) != 1 || evts[0].Event.Type != proto.EventNodeDataChanged {
		t.Fatalf("first fire: %v", evts)
	}
	// second fire should be a no-op (one-shot)
	evts = m.FireSetData("/a")
	if len(evts) != 0 {
		t.Errorf("watch fired twice: %v", evts)
	}
}

func TestChildWatchFiresOnParent(t *testing.T) {
	m := NewManager()
	m.Add(Children, "/parent", tree.SessionID(1))

	evts := m.FireCreate("/parent/x", "/parent")
	if len(evts) != 1 || evts[0].Event.Type != proto.EventNodeChildrenChanged {
		t.Fatalf("expected children-changed on /parent: %v", evts)
	}
}

func TestDeleteFiresDataAndChildrenAndExists(t *testing.T) {
	m := NewManager()
	sid := tree.SessionID(1)
	m.Add(Data, "/a", sid)
	m.Add(Exists, "/a", sid)

	evts := m.FireDelete("/a", "/")
	if len(evts) < 2 {
		t.Fatalf("expected >=2 events, got %v", evts)
	}
}

func TestRemoveSessionClearsWatches(t *testing.T) {
	m := NewManager()
	m.Add(Data, "/a", tree.SessionID(1))
	m.Add(Data, "/b", tree.SessionID(1))
	m.Add(Data, "/a", tree.SessionID(2))

	m.RemoveSession(tree.SessionID(1))

	// /a should still fire for session 2
	evts := m.FireSetData("/a")
	if len(evts) != 1 || evts[0].Session != tree.SessionID(2) {
		t.Errorf("/a: %+v", evts)
	}
	// /b should not fire at all
	evts = m.FireSetData("/b")
	if len(evts) != 0 {
		t.Errorf("/b fired after session removed: %v", evts)
	}
}

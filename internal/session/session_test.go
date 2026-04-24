package session

import (
	"reflect"
	"testing"
	"time"

	"kansi/internal/tree"
)

func newTestManager() (*Manager, *FakeClock) {
	clk := NewFakeClock(time.Unix(1_700_000_000, 0))
	return NewManager(clk), clk
}

func TestOpenAssignsMonotonicIDs(t *testing.T) {
	m, _ := newTestManager()
	a, err := m.Open(5 * time.Second)
	if err != nil {
		t.Fatal(err)
	}
	b, err := m.Open(5 * time.Second)
	if err != nil {
		t.Fatal(err)
	}
	if a == 0 || b <= a {
		t.Fatalf("ids not monotonic: a=%d b=%d", a, b)
	}
}

func TestOpenRejectsNonPositiveTimeout(t *testing.T) {
	m, _ := newTestManager()
	if _, err := m.Open(0); err != ErrInvalidTimeout {
		t.Fatalf("want ErrInvalidTimeout, got %v", err)
	}
	if _, err := m.Open(-time.Second); err != ErrInvalidTimeout {
		t.Fatalf("want ErrInvalidTimeout, got %v", err)
	}
}

func TestRenewExtendsDeadline(t *testing.T) {
	m, clk := newTestManager()
	id, err := m.Open(10 * time.Second)
	if err != nil {
		t.Fatal(err)
	}

	clk.Advance(7 * time.Second)
	if err := m.Renew(id); err != nil {
		t.Fatal(err)
	}

	// deadline should be now+timeout = start+17s
	s, ok := m.Get(id)
	if !ok {
		t.Fatal("session missing after renew")
	}
	want := clk.Now().Add(10 * time.Second)
	if !s.Deadline.Equal(want) {
		t.Fatalf("deadline = %v, want %v", s.Deadline, want)
	}

	// advance 9s more (total 16s, deadline is at 17s) — still alive
	clk.Advance(9 * time.Second)
	if expired := m.Expire(); len(expired) != 0 {
		t.Fatalf("unexpected expiry: %v", expired)
	}
}

func TestRenewUnknownSession(t *testing.T) {
	m, _ := newTestManager()
	if err := m.Renew(42); err != ErrUnknownSession {
		t.Fatalf("want ErrUnknownSession, got %v", err)
	}
}

func TestExpireRemovesAtOrPastDeadline(t *testing.T) {
	m, clk := newTestManager()
	id, _ := m.Open(5 * time.Second)

	clk.Advance(5 * time.Second) // exactly at deadline
	exp := m.Expire()
	if len(exp) != 1 || exp[0].ID != id {
		t.Fatalf("want [%d], got %+v", id, exp)
	}
	if _, ok := m.Get(id); ok {
		t.Fatal("session should be gone after Expire")
	}
}

func TestExpireKeepsFresh(t *testing.T) {
	m, clk := newTestManager()
	m.Open(10 * time.Second)
	clk.Advance(3 * time.Second)
	if exp := m.Expire(); len(exp) != 0 {
		t.Fatalf("unexpected expiry: %v", exp)
	}
	if m.Len() != 1 {
		t.Fatalf("Len = %d, want 1", m.Len())
	}
}

func TestExpireReturnsEphemerals(t *testing.T) {
	m, clk := newTestManager()
	id, _ := m.Open(time.Second)
	for _, p := range []string{"/b", "/a/x", "/a"} {
		if err := m.AddEphemeral(id, p); err != nil {
			t.Fatal(err)
		}
	}
	clk.Advance(2 * time.Second)

	exp := m.Expire()
	if len(exp) != 1 {
		t.Fatalf("want 1 expired, got %d", len(exp))
	}
	want := []string{"/a", "/a/x", "/b"}
	if !reflect.DeepEqual(exp[0].Paths, want) {
		t.Fatalf("paths = %v, want %v", exp[0].Paths, want)
	}
}

func TestCloseReturnsEphemeralsAndRemoves(t *testing.T) {
	m, _ := newTestManager()
	id, _ := m.Open(time.Second)
	_ = m.AddEphemeral(id, "/a")
	_ = m.AddEphemeral(id, "/b")

	paths, err := m.Close(id)
	if err != nil {
		t.Fatal(err)
	}
	if !reflect.DeepEqual(paths, []string{"/a", "/b"}) {
		t.Fatalf("paths = %v", paths)
	}
	if _, ok := m.Get(id); ok {
		t.Fatal("session should be gone after Close")
	}
	if _, err := m.Close(id); err != ErrUnknownSession {
		t.Fatalf("second close: want ErrUnknownSession, got %v", err)
	}
}

func TestAddRemoveEphemeral(t *testing.T) {
	m, _ := newTestManager()
	id, _ := m.Open(time.Second)

	if err := m.AddEphemeral(id+999, "/x"); err != ErrUnknownSession {
		t.Fatalf("add unknown: want ErrUnknownSession, got %v", err)
	}
	if err := m.RemoveEphemeral(id+999, "/x"); err != ErrUnknownSession {
		t.Fatalf("remove unknown: want ErrUnknownSession, got %v", err)
	}

	_ = m.AddEphemeral(id, "/a")
	_ = m.AddEphemeral(id, "/b")
	if err := m.RemoveEphemeral(id, "/a"); err != nil {
		t.Fatal(err)
	}
	s, _ := m.Get(id)
	if _, still := s.Ephemerals["/a"]; still {
		t.Fatal("/a should be gone")
	}
	if _, ok := s.Ephemerals["/b"]; !ok {
		t.Fatal("/b should remain")
	}
}

func TestGetReturnsIndependentCopy(t *testing.T) {
	m, _ := newTestManager()
	id, _ := m.Open(time.Second)
	_ = m.AddEphemeral(id, "/a")

	s, _ := m.Get(id)
	delete(s.Ephemerals, "/a")
	s.Deadline = time.Time{}

	again, _ := m.Get(id)
	if _, ok := again.Ephemerals["/a"]; !ok {
		t.Fatal("mutating the returned copy affected the manager")
	}
	if again.Deadline.IsZero() {
		t.Fatal("deadline mutated through returned copy")
	}
}

func TestExpireMixedSessions(t *testing.T) {
	m, clk := newTestManager()
	short, _ := m.Open(1 * time.Second)
	long, _ := m.Open(10 * time.Second)
	_ = m.AddEphemeral(short, "/s")
	_ = m.AddEphemeral(long, "/l")

	clk.Advance(2 * time.Second)
	exp := m.Expire()
	if len(exp) != 1 || exp[0].ID != short {
		t.Fatalf("want just short expired, got %+v", exp)
	}
	if !reflect.DeepEqual(exp[0].Paths, []string{"/s"}) {
		t.Fatalf("short paths = %v", exp[0].Paths)
	}
	if _, ok := m.Get(long); !ok {
		t.Fatal("long session should still be alive")
	}
}

// sanity: SessionID type matches the tree package so ephemeral owners line up.
var _ tree.SessionID = tree.SessionID(0)

package server_test

import (
	"os"
	"path/filepath"
	"testing"
	"time"

	"kansi/internal/client"
	"kansi/internal/server"
	"kansi/internal/session"
	"kansi/internal/tree"
	"kansi/internal/txnlog"
	"kansi/internal/watch"
)

// startServer stands up a one-off server for a test and returns its
// address and a shutdown function.
func startServer(t *testing.T) (string, func()) {
	t.Helper()
	dir := t.TempDir()
	w, err := txnlog.Create(filepath.Join(dir, "log"))
	if err != nil {
		t.Fatal(err)
	}
	tr := tree.NewTree()
	sm := session.NewManager(session.RealClock{})
	wm := watch.NewManager()
	store := server.NewStore(tr, sm, wm, &server.LogCommitter{W: w}, 0)
	s := server.New(store, sm, wm)

	errCh := make(chan error, 1)
	go func() { errCh <- s.Serve("127.0.0.1:0") }()

	// wait for Addr
	for i := 0; i < 100; i++ {
		if s.Addr() != "" {
			break
		}
		time.Sleep(5 * time.Millisecond)
	}
	if s.Addr() == "" {
		t.Fatal("server never bound")
	}

	return s.Addr(), func() {
		s.Stop()
		_ = w.Close()
		_ = os.RemoveAll(dir)
	}
}

func TestEndToEndCRUD(t *testing.T) {
	addr, stop := startServer(t)
	defer stop()

	c, err := client.Dial(addr, 2*time.Second)
	if err != nil {
		t.Fatal(err)
	}
	defer c.Close()

	if _, err := c.Create("/a", []byte("hello"), false, false); err != nil {
		t.Fatalf("Create: %v", err)
	}

	data, _, err := c.Get("/a", false)
	if err != nil {
		t.Fatalf("Get: %v", err)
	}
	if string(data) != "hello" {
		t.Errorf("got %q", data)
	}

	if _, err := c.Set("/a", []byte("world"), 0); err != nil {
		t.Fatalf("Set: %v", err)
	}
	data, _, _ = c.Get("/a", false)
	if string(data) != "world" {
		t.Errorf("after set: %q", data)
	}

	if err := c.Delete("/a", -1); err != nil {
		t.Fatalf("Delete: %v", err)
	}
	exists, _, _ := c.Exists("/a", false)
	if exists {
		t.Error("/a still exists")
	}
}

func TestWatchFires(t *testing.T) {
	addr, stop := startServer(t)
	defer stop()

	c, err := client.Dial(addr, 2*time.Second)
	if err != nil {
		t.Fatal(err)
	}
	defer c.Close()

	watches := c.Watches()

	// install an exists watch on /a before creating it
	if _, _, err := c.Exists("/a", true); err != nil {
		t.Fatal(err)
	}
	if _, err := c.Create("/a", nil, false, false); err != nil {
		t.Fatal(err)
	}

	select {
	case ev := <-watches:
		if ev.Path != "/a" {
			t.Errorf("wrong path: %q", ev.Path)
		}
	case <-time.After(time.Second):
		t.Fatal("watch never fired")
	}
}

func TestEphemeralDeletedOnClose(t *testing.T) {
	addr, stop := startServer(t)
	defer stop()

	c1, _ := client.Dial(addr, 500*time.Millisecond)
	if _, err := c1.Create("/eph", nil, true, false); err != nil {
		t.Fatal(err)
	}
	// drop the session by closing without pinging; the server's sweeper
	// should delete the ephemeral within ~timeout + 200ms
	c1.Close()

	time.Sleep(1500 * time.Millisecond)

	c2, _ := client.Dial(addr, 2*time.Second)
	defer c2.Close()
	exists, _, _ := c2.Exists("/eph", false)
	if exists {
		t.Error("/eph still exists after session expired")
	}
}

func TestMultipleClients(t *testing.T) {
	addr, stop := startServer(t)
	defer stop()

	c1, _ := client.Dial(addr, 2*time.Second)
	defer c1.Close()
	c2, _ := client.Dial(addr, 2*time.Second)
	defer c2.Close()

	if _, err := c1.Create("/shared", []byte("from-c1"), false, false); err != nil {
		t.Fatal(err)
	}
	data, _, err := c2.Get("/shared", false)
	if err != nil {
		t.Fatal(err)
	}
	if string(data) != "from-c1" {
		t.Errorf("c2 saw %q", data)
	}
}

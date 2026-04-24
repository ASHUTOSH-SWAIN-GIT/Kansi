package zab_test

import (
	"path/filepath"
	"testing"
	"time"

	"kansi/internal/client"
	"kansi/internal/server"
	"kansi/internal/session"
	"kansi/internal/tree"
	"kansi/internal/txn"
	"kansi/internal/txnlog"
	"kansi/internal/watch"
	"kansi/internal/zab"
)

// node bundles one cluster member's server, zab role, and shutdown.
type node struct {
	addr     string // client-facing TCP
	zabAddr  string // peer-to-peer zab TCP (leader only)
	store    *server.Store
	srv      *server.Server
	tree     *tree.Tree
	leader   *zab.Leader
	follower *zab.Follower
	logW     *txnlog.Writer
	stop     func()
}

func startLeader(t *testing.T, peers int) *node {
	t.Helper()
	dir := t.TempDir()
	w, err := txnlog.Create(filepath.Join(dir, "log"))
	if err != nil {
		t.Fatal(err)
	}
	tr := tree.NewTree()
	sm := session.NewManager(session.RealClock{})
	wm := watch.NewManager()

	// The leader's applier applies locally without acquiring store.mu —
	// the goroutine driving zab.Propose already holds it.
	applier := func(tx *txn.Txn) { _ = txn.Apply(tr, tx) }

	leader := zab.NewLeader(peers, w, applier)
	zabAddr, err := leader.Listen("127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}

	store := server.NewStore(tr, sm, wm, &server.ZabCommitter{L: leader, Timeout: 3 * time.Second}, 0)
	srv := server.New(store, sm, wm)
	go srv.Serve("127.0.0.1:0")
	for i := 0; i < 200 && srv.Addr() == ""; i++ {
		time.Sleep(5 * time.Millisecond)
	}

	return &node{
		addr: srv.Addr(), zabAddr: zabAddr,
		store: store, srv: srv, tree: tr,
		leader: leader, logW: w,
		stop: func() {
			srv.Stop()
			leader.Stop()
			_ = w.Close()
		},
	}
}

func startFollower(t *testing.T, leaderZabAddr string) *node {
	t.Helper()
	dir := t.TempDir()
	w, err := txnlog.Create(filepath.Join(dir, "log"))
	if err != nil {
		t.Fatal(err)
	}
	tr := tree.NewTree()
	sm := session.NewManager(session.RealClock{})
	wm := watch.NewManager()
	store := server.NewStore(tr, sm, wm, server.RejectCommitter{}, 0)
	store.SetFollowerEventDelivery(func(_ []watch.Event) {})

	fol := zab.NewFollower(w, store.ApplyReplicated)
	go func() { _ = fol.Run(leaderZabAddr, 0) }()

	srv := server.New(store, sm, wm)
	go srv.Serve("127.0.0.1:0")
	for i := 0; i < 200 && srv.Addr() == ""; i++ {
		time.Sleep(5 * time.Millisecond)
	}

	return &node{
		addr:  srv.Addr(),
		store: store, srv: srv, tree: tr,
		follower: fol, logW: w,
		stop: func() {
			srv.Stop()
			fol.Stop()
			_ = w.Close()
		},
	}
}

func waitExists(t *testing.T, c *client.Client, path string, timeout time.Duration) bool {
	t.Helper()
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		ok, _, _ := c.Exists(path, false)
		if ok {
			return true
		}
		time.Sleep(20 * time.Millisecond)
	}
	return false
}

func TestThreeNodeReplication(t *testing.T) {
	leader := startLeader(t, 3)
	defer leader.stop()

	f1 := startFollower(t, leader.zabAddr)
	defer f1.stop()
	f2 := startFollower(t, leader.zabAddr)
	defer f2.stop()

	// let followers connect + handshake
	time.Sleep(300 * time.Millisecond)

	c, err := client.Dial(leader.addr, 2*time.Second)
	if err != nil {
		t.Fatal(err)
	}
	defer c.Close()

	if _, err := c.Create("/replicated", []byte("hello"), false, false); err != nil {
		t.Fatalf("create: %v", err)
	}

	for _, f := range []*node{f1, f2} {
		fc, err := client.Dial(f.addr, 2*time.Second)
		if err != nil {
			t.Fatal(err)
		}
		if !waitExists(t, fc, "/replicated", 2*time.Second) {
			t.Errorf("follower %s did not replicate /replicated", f.addr)
		}
		fc.Close()
	}
}

func TestFollowerRejectsWrites(t *testing.T) {
	leader := startLeader(t, 2)
	defer leader.stop()
	f := startFollower(t, leader.zabAddr)
	defer f.stop()

	time.Sleep(200 * time.Millisecond)

	c, err := client.Dial(f.addr, 2*time.Second)
	if err != nil {
		t.Fatal(err)
	}
	defer c.Close()

	if _, err := c.Create("/x", nil, false, false); err == nil {
		t.Error("follower accepted a write")
	}
}

func TestLateFollowerCatchesUp(t *testing.T) {
	// Commit a txn on the leader before the follower ever connects.
	// When the follower joins, the leader's SyncReq handshake should
	// replay history so the follower ends up consistent.
	// peers=1: leader commits without follower acks, so it can write
	// before any follower exists. The late follower then catches up
	// via the SyncReq handshake replaying leader history.
	leader := startLeader(t, 1)
	defer leader.stop()

	c, err := client.Dial(leader.addr, 2*time.Second)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := c.Create("/early", []byte("first"), false, false); err != nil {
		t.Fatal(err)
	}
	c.Close()

	f := startFollower(t, leader.zabAddr)
	defer f.stop()

	fc, err := client.Dial(f.addr, 2*time.Second)
	if err != nil {
		t.Fatal(err)
	}
	defer fc.Close()
	if !waitExists(t, fc, "/early", 2*time.Second) {
		t.Error("follower did not catch up via history replay")
	}
}

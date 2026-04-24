package zab

import (
	"fmt"
	"log"
	"net"
	"sync"
	"sync/atomic"
	"time"

	"kansi/internal/txn"
	"kansi/internal/txnlog"
)

// Follower connects to a leader, syncs, and applies committed txns.
type Follower struct {
	log     *txnlog.Writer
	apply   Applier
	stopCh  chan struct{}
	stopped atomic.Bool

	mu      sync.Mutex
	pending map[uint64]*txn.Txn // zxid -> txn awaiting commit
}

// NewFollower returns a Follower that appends to w and calls apply on
// each committed txn.
func NewFollower(w *txnlog.Writer, apply Applier) *Follower {
	return &Follower{
		log:     w,
		apply:   apply,
		stopCh:  make(chan struct{}),
		pending: make(map[uint64]*txn.Txn),
	}
}

// Stop terminates any active Run loop.
func (f *Follower) Stop() {
	if f.stopped.CompareAndSwap(false, true) {
		close(f.stopCh)
	}
}

// Run connects to leaderAddr and processes messages until Stop or the
// connection dies. startZxid is the follower's currently-applied zxid;
// the leader will catch us up from there.
//
// Reconnection on error is left to the caller (test harness).
func (f *Follower) Run(leaderAddr string, startZxid uint64) error {
	c, err := dialWithRetry(leaderAddr, f.stopCh)
	if err != nil {
		return err
	}
	defer func() { _ = c.Close() }()

	// send sync request
	if err := WriteMsg(c, &Msg{Type: MsgSyncReq, Zxid: startZxid}); err != nil {
		return err
	}

	for {
		select {
		case <-f.stopCh:
			return nil
		default:
		}
		m, err := ReadMsg(c)
		if err != nil {
			return err
		}
		switch m.Type {
		case MsgProposal:
			if err := f.log.Append(m.Txn); err != nil {
				return fmt.Errorf("follower log append: %w", err)
			}
			f.mu.Lock()
			f.pending[uint64(m.Txn.Zxid)] = m.Txn
			f.mu.Unlock()
			if err := WriteMsg(c, &Msg{Type: MsgAck, Zxid: uint64(m.Txn.Zxid)}); err != nil {
				return err
			}
		case MsgCommit:
			f.mu.Lock()
			t, ok := f.pending[m.Zxid]
			if ok {
				delete(f.pending, m.Zxid)
			}
			f.mu.Unlock()
			if ok {
				f.apply(t)
			}
		case MsgSyncEnd:
			// end of catch-up; keep the loop alive for live stream
		default:
			log.Printf("zab follower: unexpected msg %d", m.Type)
		}
	}
}

func dialWithRetry(addr string, stop <-chan struct{}) (net.Conn, error) {
	deadline := time.Now().Add(5 * time.Second)
	for {
		select {
		case <-stop:
			return nil, fmt.Errorf("follower stopped")
		default:
		}
		c, err := net.DialTimeout("tcp", addr, 500*time.Millisecond)
		if err == nil {
			return c, nil
		}
		if time.Now().After(deadline) {
			return nil, err
		}
		time.Sleep(100 * time.Millisecond)
	}
}

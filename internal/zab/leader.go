package zab

import (
	"fmt"
	"io"
	"log"
	"net"
	"sync"
	"time"

	"kansi/internal/txn"
	"kansi/internal/txnlog"
)

// Applier is the callback invoked when a txn has been durably committed
// by a majority. The store layer plugs its tree.Apply here.
type Applier func(*txn.Txn)

// Leader accepts proposals, replicates them to followers over TCP, and
// commits once a majority (including the leader itself) has acked.
//
// Peers is the total ensemble size; the leader counts as one vote.
type Leader struct {
	peers   int
	applier Applier
	log     *txnlog.Writer

	mu       sync.Mutex
	history  []*txn.Txn // committed txns, newest last; for follower sync
	pending  map[uint64]*pending
	follows  map[string]net.Conn // addr -> conn
	stopCh   chan struct{}
	listener net.Listener
}

type pending struct {
	txn    *txn.Txn
	acks   int
	done   chan struct{}
	closed bool
}

// NewLeader returns a leader that will replicate to peers total nodes.
// The leader itself is included in that count.
func NewLeader(peers int, w *txnlog.Writer, apply Applier) *Leader {
	return &Leader{
		peers:   peers,
		applier: apply,
		log:     w,
		pending: make(map[uint64]*pending),
		follows: make(map[string]net.Conn),
		stopCh:  make(chan struct{}),
	}
}

// Listen starts accepting follower connections on addr. Non-blocking.
func (l *Leader) Listen(addr string) (string, error) {
	ln, err := net.Listen("tcp", addr)
	if err != nil {
		return "", err
	}
	l.listener = ln
	go l.acceptLoop()
	return ln.Addr().String(), nil
}

// Stop closes the listener and all follower connections.
func (l *Leader) Stop() {
	close(l.stopCh)
	if l.listener != nil {
		_ = l.listener.Close()
	}
	l.mu.Lock()
	for _, c := range l.follows {
		_ = c.Close()
	}
	l.mu.Unlock()
}

func (l *Leader) acceptLoop() {
	for {
		c, err := l.listener.Accept()
		if err != nil {
			select {
			case <-l.stopCh:
				return
			default:
			}
			return
		}
		go l.handleFollower(c)
	}
}

// handleFollower runs one follower connection: first handles the sync
// handshake, then streams live proposals/commits, then consumes acks.
func (l *Leader) handleFollower(c net.Conn) {
	defer func() { _ = c.Close() }()
	// first message must be a SyncReq so we know follower's LastZxid
	m, err := ReadMsg(c)
	if err != nil || m.Type != MsgSyncReq {
		return
	}
	fromZxid := m.Zxid

	l.mu.Lock()
	// catch follower up by replaying history
	toSend := make([]*txn.Txn, 0)
	for _, t := range l.history {
		if uint64(t.Zxid) > fromZxid {
			toSend = append(toSend, t)
		}
	}
	addr := c.RemoteAddr().String()
	l.follows[addr] = c
	l.mu.Unlock()

	for _, t := range toSend {
		if err := WriteMsg(c, &Msg{Type: MsgProposal, Txn: t}); err != nil {
			return
		}
		if err := WriteMsg(c, &Msg{Type: MsgCommit, Zxid: uint64(t.Zxid)}); err != nil {
			return
		}
	}
	if err := WriteMsg(c, &Msg{Type: MsgSyncEnd}); err != nil {
		return
	}

	// read acks for live proposals
	for {
		m, err := ReadMsg(c)
		if err != nil {
			if err != io.EOF {
				log.Printf("zab leader: follower read: %v", err)
			}
			l.mu.Lock()
			delete(l.follows, addr)
			l.mu.Unlock()
			return
		}
		if m.Type == MsgAck {
			l.receiveAck(m.Zxid)
		}
	}
}

// receiveAck records one ack and closes the proposal's done chan if
// quorum is reached.
func (l *Leader) receiveAck(zxid uint64) {
	l.mu.Lock()
	p, ok := l.pending[zxid]
	if !ok || p.closed {
		l.mu.Unlock()
		return
	}
	p.acks++
	if p.acks+1 >= quorum(l.peers) { // +1 for leader's own
		p.closed = true
		close(p.done)
	}
	l.mu.Unlock()
}

func quorum(n int) int { return n/2 + 1 }

// Propose replicates t, waits for quorum, commits, and returns. Blocks.
// timeout protects against hanging if a follower is stuck.
func (l *Leader) Propose(t *txn.Txn, timeout time.Duration) error {
	p := &pending{txn: t, done: make(chan struct{})}
	zxid := uint64(t.Zxid)

	l.mu.Lock()
	l.pending[zxid] = p
	follows := make([]net.Conn, 0, len(l.follows))
	for _, c := range l.follows {
		follows = append(follows, c)
	}
	l.mu.Unlock()

	// leader durability first
	if err := l.log.Append(t); err != nil {
		return fmt.Errorf("leader log append: %w", err)
	}

	// broadcast proposal
	for _, c := range follows {
		if err := WriteMsg(c, &Msg{Type: MsgProposal, Txn: t}); err != nil {
			// follower likely gone; the accept loop will clean it up
			continue
		}
	}

	// 1-node cluster: no followers needed, commit immediately
	if l.peers == 1 {
		l.mu.Lock()
		p.closed = true
		close(p.done)
		l.mu.Unlock()
	}

	select {
	case <-p.done:
	case <-time.After(timeout):
		l.mu.Lock()
		delete(l.pending, zxid)
		l.mu.Unlock()
		return fmt.Errorf("zab: quorum timeout for zxid %d", zxid)
	}

	// broadcast commit
	l.mu.Lock()
	delete(l.pending, zxid)
	l.history = append(l.history, t)
	follows = follows[:0]
	for _, c := range l.follows {
		follows = append(follows, c)
	}
	l.mu.Unlock()

	for _, c := range follows {
		_ = WriteMsg(c, &Msg{Type: MsgCommit, Zxid: zxid})
	}

	// leader applies to its own tree
	l.applier(t)
	return nil
}

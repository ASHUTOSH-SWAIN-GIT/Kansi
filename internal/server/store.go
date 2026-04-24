// Package server ties the tree, log, processor, sessions, and watches
// into a running TCP service.
package server

import (
	"sync"

	"kansi/internal/processor"
	"kansi/internal/proto"
	"kansi/internal/session"
	"kansi/internal/tree"
	"kansi/internal/txn"
	"kansi/internal/txnlog"
	"kansi/internal/watch"
)

// Committer decides how a processed txn gets durably committed.
// In single-node mode, it's log-append-then-apply.
// In replicated mode (Stage 5), a Zab-backed committer replaces this.
type Committer interface {
	// Commit makes the txn durable and returns nil iff it can be applied.
	Commit(t *txn.Txn) error
}

// Store is the server's state: tree, log, sessions, watches, zxid clock.
// All mutating operations take the store-wide mutex to keep zxid
// allocation and application strictly serial.
type Store struct {
	mu sync.Mutex

	tree           *tree.Tree
	proc           *processor.Processor
	sessions       *session.Manager
	watches        *watch.Manager
	committer      Committer
	nextZxid       uint64
	followerEvents func([]watch.Event)
}

// NewStore creates a store wrapping the given tree.
func NewStore(tr *tree.Tree, sm *session.Manager, wm *watch.Manager, c Committer, startZxid uint64) *Store {
	return &Store{
		tree:      tr,
		proc:      processor.New(),
		sessions:  sm,
		watches:   wm,
		committer: c,
		nextZxid:  startZxid + 1,
	}
}

// LogCommitter appends txns to a WAL and returns. Apply is done by
// the Store after Commit succeeds.
type LogCommitter struct {
	W *txnlog.Writer
}

// Commit fsync-appends t to the log.
func (l *LogCommitter) Commit(t *txn.Txn) error { return l.W.Append(t) }

// ApplyReplicated applies a txn that arrived from zab on a follower,
// firing watches and updating ephemeral bookkeeping. Unlike applyWrite
// it does not go through the processor or allocate a zxid — the txn
// is already fully-formed.
func (s *Store) ApplyReplicated(t *txn.Txn) {
	s.mu.Lock()
	if uint64(t.Zxid) >= s.nextZxid {
		s.nextZxid = uint64(t.Zxid) + 1
	}
	if err := txn.Apply(s.tree, t); err != nil {
		s.mu.Unlock()
		return
	}
	var events []watch.Event
	switch t.Type {
	case txn.TypeCreate:
		events = s.watches.FireCreate(t.Create.Path, tree.ParentPath(t.Create.Path))
	case txn.TypeDelete:
		events = s.watches.FireDelete(t.Delete.Path, tree.ParentPath(t.Delete.Path))
	case txn.TypeSetData:
		events = s.watches.FireSetData(t.SetData.Path)
	}
	s.mu.Unlock()
	if s.followerEvents != nil && len(events) > 0 {
		s.followerEvents(events)
	}
}

// SetFollowerEventDelivery installs a callback used on follower nodes
// to route watch events that fire during replication apply. (On
// leaders, watch events are routed by the per-request path.)
func (s *Store) SetFollowerEventDelivery(fn func([]watch.Event)) {
	s.followerEvents = fn
}

// allocZxid returns the next zxid. Caller must hold mu.
func (s *Store) allocZxid() uint64 {
	z := s.nextZxid
	s.nextZxid++
	return z
}

// CurrentZxid returns the last-allocated zxid (or 0 if none).
func (s *Store) CurrentZxid() uint64 {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.nextZxid - 1
}

// writeResult is returned by mutations that apply a txn. It carries
// data the server sends back to the client.
type writeResult struct {
	Zxid         uint64
	ErrCode      processor.ErrCode
	ResolvedPath string // Create only
	NewVersion   int64  // SetData only
	Events       []watch.Event
}

// applyWrite runs processor → committer → tree.Apply → watches.
// Callers must NOT hold mu.
func (s *Store) applyWrite(sid tree.SessionID, req processor.Request) *writeResult {
	s.mu.Lock()
	defer s.mu.Unlock()

	zxid := s.allocZxid()
	res := s.proc.Process(s.tree, txn.Zxid(zxid), req)
	if res.Rejected() {
		// Reject without committing. zxid is still consumed so ordering
		// is visible to the client (matches paper behavior loosely).
		return &writeResult{Zxid: zxid, ErrCode: res.ErrCode}
	}
	commitErr := s.committer.Commit(res.Txn)
	if commitErr != nil && commitErr != ErrAppliedByZab {
		// Durability failed. Client will get a generic error.
		return &writeResult{Zxid: zxid, ErrCode: processor.ErrUnknown}
	}
	// ErrAppliedByZab means the zab leader's applier already applied
	// the txn to the tree; skip our own apply to avoid double-apply.
	if commitErr != ErrAppliedByZab {
		if err := txn.Apply(s.tree, res.Txn); err != nil {
			return &writeResult{Zxid: zxid, ErrCode: processor.ErrUnknown}
		}
	}
	wr := &writeResult{Zxid: zxid, ResolvedPath: res.ResolvedPath}

	switch res.Txn.Type {
	case txn.TypeCreate:
		parent := tree.ParentPath(res.Txn.Create.Path)
		wr.Events = s.watches.FireCreate(res.Txn.Create.Path, parent)
		// ephemeral bookkeeping
		if res.Txn.Create.EphemeralOwner != 0 {
			_ = s.sessions.AddEphemeral(sid, res.Txn.Create.Path)
		}
	case txn.TypeDelete:
		parent := tree.ParentPath(res.Txn.Delete.Path)
		wr.Events = s.watches.FireDelete(res.Txn.Delete.Path, parent)
		_ = s.sessions.RemoveEphemeral(sid, res.Txn.Delete.Path)
	case txn.TypeSetData:
		wr.NewVersion = res.Txn.SetData.NewVersion
		wr.Events = s.watches.FireSetData(res.Txn.SetData.Path)
	}
	return wr
}

// applyExpiredSession deletes the ephemerals a session owned and
// fires their watches. Called from the expiry sweeper when a session
// times out.
func (s *Store) applyExpiredSession(paths []string) []watch.Event {
	s.mu.Lock()
	defer s.mu.Unlock()
	var events []watch.Event
	for _, p := range paths {
		zxid := s.allocZxid()
		st, err := s.tree.Stat(p)
		if err != nil {
			continue
		}
		t := &txn.Txn{
			Zxid: txn.Zxid(zxid), Type: txn.TypeDelete,
			Delete: &txn.DeleteTxn{Path: p, TargetCzxid: st.Czxid},
		}
		if err := s.committer.Commit(t); err != nil {
			continue
		}
		if err := txn.Apply(s.tree, t); err != nil {
			continue
		}
		events = append(events, s.watches.FireDelete(p, tree.ParentPath(p))...)
	}
	return events
}

// readGetData reads data and optionally installs a data watch.
func (s *Store) readGetData(sid tree.SessionID, path string, wantWatch bool) (*proto.GetDataResp, processor.ErrCode, uint64) {
	s.mu.Lock()
	defer s.mu.Unlock()
	data, _, err := s.tree.GetData(path)
	if err != nil {
		return nil, processor.ErrNoNode, s.nextZxid - 1
	}
	st, _ := s.tree.Stat(path)
	if wantWatch {
		s.watches.Add(watch.Data, path, sid)
	}
	return &proto.GetDataResp{Data: data, Stat: st}, processor.ErrOK, s.nextZxid - 1
}

// readExists checks existence and optionally installs an exists watch.
func (s *Store) readExists(sid tree.SessionID, path string, wantWatch bool) (*proto.ExistsResp, uint64) {
	s.mu.Lock()
	defer s.mu.Unlock()
	st, err := s.tree.Stat(path)
	if wantWatch {
		s.watches.Add(watch.Exists, path, sid)
	}
	if err != nil {
		return &proto.ExistsResp{Exists: false}, s.nextZxid - 1
	}
	return &proto.ExistsResp{Exists: true, Stat: st}, s.nextZxid - 1
}

// readGetChildren lists children and optionally installs a child watch.
func (s *Store) readGetChildren(sid tree.SessionID, path string, wantWatch bool) (*proto.GetChildrenResp, processor.ErrCode, uint64) {
	s.mu.Lock()
	defer s.mu.Unlock()
	kids, err := s.tree.GetChildren(path)
	if err != nil {
		return nil, processor.ErrNoNode, s.nextZxid - 1
	}
	if wantWatch {
		s.watches.Add(watch.Children, path, sid)
	}
	return &proto.GetChildrenResp{Children: kids}, processor.ErrOK, s.nextZxid - 1
}

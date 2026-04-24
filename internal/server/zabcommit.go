package server

import (
	"errors"
	"time"

	"kansi/internal/txn"
	"kansi/internal/zab"
)

// ZabCommitter plugs a zab leader into the Store.Commit path.
// Propose blocks until a quorum has acked; only then does Commit
// return nil and the Store apply the txn locally.
type ZabCommitter struct {
	L       *zab.Leader
	Timeout time.Duration
}

// Commit broadcasts t through zab and blocks until committed.
func (z *ZabCommitter) Commit(t *txn.Txn) error {
	// zab will also log+apply on the leader via its applier callback.
	// We bypass Store's apply-after-Commit by returning the sentinel.
	if err := z.L.Propose(t, z.Timeout); err != nil {
		return err
	}
	return ErrAppliedByZab
}

// ErrAppliedByZab is returned by ZabCommitter.Commit to signal that
// the zab leader has already applied the txn (via its applier
// callback). The Store recognises this and skips its own apply.
var ErrAppliedByZab = errors.New("applied by zab")

// RejectCommitter is used on follower nodes: writes from clients are
// rejected because only the leader can propose.
type RejectCommitter struct{}

// ErrNotLeader is returned when a follower is asked to commit.
var ErrNotLeader = errors.New("not leader")

// Commit always returns ErrNotLeader.
func (RejectCommitter) Commit(*txn.Txn) error { return ErrNotLeader }

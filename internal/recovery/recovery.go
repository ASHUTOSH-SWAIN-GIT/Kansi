// Package recovery reconstructs the in-memory tree at server startup
// from durable state: latest snapshot + replayed tail of the txn log.
//
// Paper section 4.3: "when loading a snapshot, we replay transactions
// with zxid >= startZxid. Because our transactions are idempotent, it
// is fine if we apply a transaction more than once."
package recovery

import (
	"errors"
	"io"
	"os"

	"kansi/internal/snapshot"
	"kansi/internal/tree"
	"kansi/internal/txn"
	"kansi/internal/txnlog"
)

// Result is what Recover returns: the reconstructed tree and the
// highest zxid observed (snapshot or log, whichever is later).
type Result struct {
	Tree     *tree.Tree
	LastZxid txn.Zxid
}

// Recover loads snapshotPath (if it exists) into a tree, then replays
// all records in logPath whose zxid is >= snapshot.StartZxid. Missing
// files are treated as empty inputs.
//
// Replay stops cleanly at a torn tail in the log (the paper's expected
// crash-mid-write case). Mid-log corruption is returned as an error.
func Recover(snapshotPath, logPath string) (*Result, error) {
	var tr *tree.Tree
	var startZxid txn.Zxid

	if snapshotPath != "" && fileExists(snapshotPath) {
		var err error
		tr, startZxid, err = snapshot.Read(snapshotPath)
		if err != nil {
			return nil, err
		}
	} else {
		tr = tree.NewTree()
		startZxid = 0
	}

	lastZxid := startZxid

	if logPath != "" && fileExists(logPath) {
		r, err := txnlog.Open(logPath)
		if err != nil {
			return nil, err
		}
		defer func() { _ = r.Close() }()

		for {
			t, err := r.Next()
			if errors.Is(err, io.EOF) {
				break
			}
			if err != nil {
				return nil, err
			}
			if t.Zxid < startZxid {
				continue // already covered by the snapshot
			}
			if err := txn.Apply(tr, t); err != nil {
				return nil, err
			}
			if t.Zxid > lastZxid {
				lastZxid = t.Zxid
			}
		}
	}

	return &Result{Tree: tr, LastZxid: lastZxid}, nil
}

func fileExists(p string) bool {
	_, err := os.Stat(p)
	return err == nil
}

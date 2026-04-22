// Package snapshot writes and reads snapshots of the znode tree.
//
// A snapshot is a header followed by a stream of znode records, one per
// znode, emitted in depth-first pre-order so that parents precede their
// children during restore.
//
// Snapshots are "fuzzy" in the paper's sense: the writer does not hold
// a global lock across the full walk, so individual znode records may
// reflect different points in the transaction stream. Correctness is
// restored at replay time — after loading a snapshot, the recovery
// procedure replays the txn log from the header's StartZxid
// forward, and Apply's idempotency guarantees the final state is
// consistent.
//
// File layout:
//
//	Header { Magic, Version, StartZxid }
//	Record { Path, Znode }  × N
//	       (Path == "" sentinel marks end of stream)
package snapshot

import (
	"encoding/gob"
	"errors"
	"fmt"
	"io"
	"os"

	"kansi/internal/tree"
	"kansi/internal/txn"
)

const (
	magic     uint32 = 0x6B616E73 // "kans"
	version   uint32 = 1
	endOfSnap        = "" // sentinel path marking stream end
)

// Header prefixes every snapshot file.
//
// StartZxid is the zxid at which the snapshot walk began; recovery
// replays log records with zxid >= StartZxid to cover any txns that
// may or may not have been captured by the fuzzy walk.
type Header struct {
	Magic     uint32
	Version   uint32
	StartZxid txn.Zxid
}

// record is one znode in the stream.
type record struct {
	Path  string
	Znode tree.Znode
}

// Write serialises the tree at path. startZxid is the zxid of
// the last txn reflected in this snapshot (written to the header so
// recovery knows where to resume log replay).
//
// The walk is non-blocking on the tree in a concurrent setting — the
// tree layer does not lock here. The caller is responsible for any
// serialisation needed.
func Write(path string, tr *tree.Tree, startZxid txn.Zxid) error {
	f, err := os.Create(path)
	if err != nil {
		return err
	}
	defer func() { _ = f.Close() }()

	enc := gob.NewEncoder(f)
	if err := enc.Encode(Header{Magic: magic, Version: version, StartZxid: startZxid}); err != nil {
		return fmt.Errorf("snapshot: write header: %w", err)
	}

	var walkErr error
	tr.Walk(func(p string, z *tree.Znode) {
		if walkErr != nil {
			return
		}
		// copy to decouple from live znode
		r := record{
			Path: p,
			Znode: tree.Znode{
				Data:              append([]byte(nil), z.Data...),
				DataVersion:       z.DataVersion,
				ChildrenVersion:   z.ChildrenVersion,
				ACLversion:        z.ACLversion,
				Ctime:             z.Ctime,
				Mtime:             z.Mtime,
				EphemeralOwner:    z.EphemeralOwner,
				SequentialCounter: z.SequentialCounter,
			},
		}
		if err := enc.Encode(r); err != nil {
			walkErr = err
		}
	})
	if walkErr != nil {
		return fmt.Errorf("snapshot: write record: %w", walkErr)
	}

	// end sentinel so reader can tell clean EOF from torn tail
	if err := enc.Encode(record{Path: endOfSnap}); err != nil {
		return err
	}
	return f.Sync()
}

// ErrBadMagic is returned when a snapshot file's header doesn't match.
var ErrBadMagic = errors.New("snapshot: bad magic; not a snapshot file")

// ErrTruncated is returned when the stream ended before the sentinel.
var ErrTruncated = errors.New("snapshot: file truncated (no end sentinel)")

// Read loads a snapshot into a fresh tree and returns the tree plus
// the StartZxid from the header.
func Read(path string) (*tree.Tree, txn.Zxid, error) {
	f, err := os.Open(path)
	if err != nil {
		return nil, 0, err
	}
	defer func() { _ = f.Close() }()

	dec := gob.NewDecoder(f)

	var h Header
	if err := dec.Decode(&h); err != nil {
		return nil, 0, fmt.Errorf("snapshot: read header: %w", err)
	}
	if h.Magic != magic {
		return nil, 0, ErrBadMagic
	}
	if h.Version != version {
		return nil, 0, fmt.Errorf("snapshot: unknown version %d", h.Version)
	}

	tr := tree.NewTree()
	for {
		var r record
		if err := dec.Decode(&r); err != nil {
			if err == io.EOF {
				// EOF before sentinel -> torn snapshot write
				return nil, 0, ErrTruncated
			}
			return nil, 0, fmt.Errorf("snapshot: read record: %w", err)
		}
		if r.Path == endOfSnap {
			return tr, h.StartZxid, nil
		}
		if err := tr.Restore(r.Path, &r.Znode); err != nil {
			return nil, 0, fmt.Errorf("snapshot: restore %q: %w", r.Path, err)
		}
	}
}

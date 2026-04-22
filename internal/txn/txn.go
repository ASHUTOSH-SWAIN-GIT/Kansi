package txn

import (
	"encoding/gob"

	"kansi/internal/tree"
)

// Zxid is a transaction identifier: (epoch << 32) | counter.
// Strictly monotonic across all committed transactions.
type Zxid uint64

// MakeZxid packs (epoch, counter) into a single 64-bit zxid.
func MakeZxid(epoch uint32, counter uint32) Zxid {
	return Zxid(uint64(epoch)<<32 | uint64(counter))
}

// Epoch returns the leader epoch component of the zxid.
func (z Zxid) Epoch() uint32 { return uint32(z >> 32) }

// Counter returns the per-epoch counter component of the zxid.
func (z Zxid) Counter() uint32 { return uint32(z) }

// Type identifies what kind of transaction a record holds.
type Type int32

// Transaction type constants.
const (
	TypeCreate Type = iota + 1
	TypeDelete
	TypeSetData
	TypeError
)

// Txn is one committed transaction. Exactly one of the payload fields
// is non-nil, matching Type.
type Txn struct {
	Zxid    Zxid
	Type    Type
	Create  *CreateTxn
	Delete  *DeleteTxn
	SetData *SetDataTxn
	Error   *ErrorTxn
}

// CreateTxn fully specifies a create. Sequential naming is already resolved
// (Path is the final name). Ephemeral owner is already baked in.
type CreateTxn struct {
	Path           string
	Data           []byte
	EphemeralOwner tree.SessionID
	Ctime          int64
}

// DeleteTxn carries the path of a znode to remove.
type DeleteTxn struct {
	Path string
}

// SetDataTxn carries the exact new state. NewVersion is what the znode's
// DataVersion should become after applying.
type SetDataTxn struct {
	Path       string
	Data       []byte
	NewVersion int64
	Mtime      int64
}

// ErrorTxn is recorded when a client request fails validation.
// It still gets a zxid and a slot in the log so Zab ordering is preserved
// and so clients get ordered responses.
type ErrorTxn struct {
	Code int32
}

// Register types with gob so encoding works through the interface-ish Txn wrapper.
func init() {
	gob.Register(&CreateTxn{})
	gob.Register(&DeleteTxn{})
	gob.Register(&SetDataTxn{})
	gob.Register(&ErrorTxn{})
}

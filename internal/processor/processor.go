// Package processor turns validated client requests into concrete,
// self-contained transactions. Transactions emitted here have all
// version numbers, timestamps, and zxids baked in so Apply is a pure
// function over (tree, txn) — no conditional logic at apply time.
//
// This is Stage 3 of the build order: the layer between the server
// and the txn log / Zab broadcast.
package processor

import (
	"errors"
	"fmt"
	"time"

	"kansi/internal/tree"
	"kansi/internal/txn"
)

// ErrCode is a stable wire-friendly error code for rejected requests.
type ErrCode int32

// Error codes. Kept small and numeric for protocol stability.
const (
	ErrOK ErrCode = iota
	ErrNoNode
	ErrNodeExists
	ErrBadVersion
	ErrNotEmpty
	ErrNoParent
	ErrEphemeralParent
	ErrInvalidPath
	ErrUnknown
)

// Request is the union of all client write requests. The processor
// accepts one of these and emits a concrete Txn.
type Request interface{ isRequest() }

// CreateRequest asks to create a znode.
type CreateRequest struct {
	Path       string
	Data       []byte
	Owner      tree.SessionID // non-zero iff Ephemeral is true
	Ephemeral  bool
	Sequential bool
}

// DeleteRequest asks to remove a znode at expectedVersion.
type DeleteRequest struct {
	Path            string
	ExpectedVersion int64 // -1 = skip version check
}

// SetDataRequest asks to write data at expectedVersion.
type SetDataRequest struct {
	Path            string
	Data            []byte
	ExpectedVersion int64 // -1 = skip version check
}

func (CreateRequest) isRequest()  {}
func (DeleteRequest) isRequest()  {}
func (SetDataRequest) isRequest() {}

// Result wraps either a Txn or a rejection. A rejection still has a
// zxid allocated — the server may choose to skip logging rejections
// in this minimal build; see server.go.
type Result struct {
	Txn     *txn.Txn
	ErrCode ErrCode
	// ResolvedPath is only set for CreateRequest (sequential naming resolves here).
	ResolvedPath string
}

// Rejected reports whether the processor declined to turn the request
// into a state-changing txn.
func (r *Result) Rejected() bool { return r.ErrCode != ErrOK }

// Processor stateless-ly translates requests into txns using the
// current tree state.
type Processor struct {
	// now is injectable for deterministic tests.
	now func() int64
}

// New returns a Processor using wall-clock timestamps.
func New() *Processor { return &Processor{now: func() int64 { return time.Now().UnixMilli() }} }

// WithClock returns a Processor whose timestamps come from fn (ms).
func WithClock(fn func() int64) *Processor { return &Processor{now: fn} }

// Process validates req against tr and, on success, returns a Txn
// stamped with zxid. tr is read-only here — the caller applies the
// txn after it's durable.
func (p *Processor) Process(tr *tree.Tree, zxid txn.Zxid, req Request) *Result {
	switch r := req.(type) {
	case CreateRequest:
		return p.create(tr, zxid, r)
	case DeleteRequest:
		return p.delete(tr, zxid, r)
	case SetDataRequest:
		return p.setData(tr, zxid, r)
	default:
		return &Result{ErrCode: ErrUnknown}
	}
}

func (p *Processor) create(tr *tree.Tree, zxid txn.Zxid, r CreateRequest) *Result {
	if r.Path == "/" {
		return &Result{ErrCode: ErrNodeExists}
	}
	if err := tree.ValidatePath(r.Path); err != nil {
		return &Result{ErrCode: ErrInvalidPath}
	}
	parentPath := tree.ParentPath(r.Path)
	parentStat, err := tr.Stat(parentPath)
	if err != nil {
		return &Result{ErrCode: ErrNoParent}
	}
	if parentStat.EphemeralOwner != 0 {
		return &Result{ErrCode: ErrEphemeralParent}
	}

	// Resolve sequential suffix: final name uses the parent's current
	// counter. The counter will bump when the txn is applied (the
	// ApplyCreate code in tree infers the bump from the suffix).
	base := tree.BaseName(r.Path)
	if r.Sequential {
		base = fmt.Sprintf("%s%010d", base, parentStat.SequentialCounter)
	}
	finalPath := joinPath(parentPath, base)

	// Existence check (only meaningful for non-sequential since sequential
	// always generates a fresh name).
	if ok, _ := tr.Exists(finalPath); ok {
		return &Result{ErrCode: ErrNodeExists}
	}

	var owner tree.SessionID
	if r.Ephemeral {
		owner = r.Owner
	}

	return &Result{
		ResolvedPath: finalPath,
		Txn: &txn.Txn{
			Zxid: zxid, Type: txn.TypeCreate,
			Create: &txn.CreateTxn{
				Path:           finalPath,
				Data:           append([]byte(nil), r.Data...),
				EphemeralOwner: owner,
				Ctime:          p.now(),
			},
		},
	}
}

func (p *Processor) delete(tr *tree.Tree, zxid txn.Zxid, r DeleteRequest) *Result {
	if r.Path == "/" {
		return &Result{ErrCode: ErrInvalidPath}
	}
	st, err := tr.Stat(r.Path)
	if errors.Is(err, tree.ErrNoNode) {
		return &Result{ErrCode: ErrNoNode}
	}
	if err != nil {
		return &Result{ErrCode: ErrInvalidPath}
	}
	if r.ExpectedVersion != -1 && st.DataVersion != r.ExpectedVersion {
		return &Result{ErrCode: ErrBadVersion}
	}
	if st.NumChildren > 0 {
		return &Result{ErrCode: ErrNotEmpty}
	}
	return &Result{
		Txn: &txn.Txn{
			Zxid: zxid, Type: txn.TypeDelete,
			Delete: &txn.DeleteTxn{
				Path:        r.Path,
				TargetCzxid: st.Czxid,
				Mtime:       p.now(),
			},
		},
	}
}

func (p *Processor) setData(tr *tree.Tree, zxid txn.Zxid, r SetDataRequest) *Result {
	st, err := tr.Stat(r.Path)
	if errors.Is(err, tree.ErrNoNode) {
		return &Result{ErrCode: ErrNoNode}
	}
	if err != nil {
		return &Result{ErrCode: ErrInvalidPath}
	}
	if r.ExpectedVersion != -1 && st.DataVersion != r.ExpectedVersion {
		return &Result{ErrCode: ErrBadVersion}
	}
	return &Result{
		Txn: &txn.Txn{
			Zxid: zxid, Type: txn.TypeSetData,
			SetData: &txn.SetDataTxn{
				Path:        r.Path,
				Data:        append([]byte(nil), r.Data...),
				TargetCzxid: st.Czxid,
				NewVersion:  st.DataVersion + 1,
				Mtime:       p.now(),
			},
		},
	}
}

func joinPath(parent, base string) string {
	if parent == "/" {
		return "/" + base
	}
	return parent + "/" + base
}

package txn

import (
	"fmt"

	"kansi/internal/tree"
)

// Apply mutates the tree to reflect the given transaction.
//
// Apply is pure relative to (tree, txn): same inputs -> same result.
// It performs no validation — the request processor has already baked
// the exact outcome into the txn, and the log guarantees ordering.
//
// Apply is idempotent: replaying a txn that has already been applied
// is a no-op. This is what makes fuzzy snapshot recovery correct —
// after loading a snapshot we replay the tail of the log, and the
// snapshot may have already captured some of those txns.
func Apply(tr *tree.Tree, t *Txn) error {
	switch t.Type {
	case TypeCreate:
		if t.Create == nil {
			return fmt.Errorf("txn %d: create payload missing", t.Zxid)
		}
		if ok, _ := tr.Exists(t.Create.Path); ok {
			return nil // already applied
		}
		return tr.ApplyCreate(t.Create.Path, t.Create.Data, t.Create.EphemeralOwner, t.Create.Ctime)

	case TypeDelete:
		if t.Delete == nil {
			return fmt.Errorf("txn %d: delete payload missing", t.Zxid)
		}
		if ok, _ := tr.Exists(t.Delete.Path); !ok {
			return nil // already applied
		}
		return tr.ApplyDelete(t.Delete.Path)

	case TypeSetData:
		if t.SetData == nil {
			return fmt.Errorf("txn %d: setdata payload missing", t.Zxid)
		}
		// ApplySetData handles the version-based idempotency check.
		return tr.ApplySetData(t.SetData.Path, t.SetData.Data, t.SetData.NewVersion, t.SetData.Mtime)

	case TypeError:
		// Error txns occupy a log slot for ordering but don't touch state.
		return nil

	default:
		return fmt.Errorf("txn %d: unknown type %d", t.Zxid, t.Type)
	}
}

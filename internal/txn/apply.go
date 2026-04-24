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
		if st, err := tr.Stat(t.Create.Path); err == nil {
			createZxid := uint64(t.Zxid)
			if createZxid == 0 || st.Czxid == 0 || st.Czxid >= createZxid {
				return nil // already applied, or superseded by a newer incarnation
			}
			return tree.ErrNodeExists
		} else if err != tree.ErrNoNode {
			return err
		}
		return tr.ApplyCreate(t.Create.Path, t.Create.Data, t.Create.EphemeralOwner, t.Create.Ctime, uint64(t.Zxid))

	case TypeDelete:
		if t.Delete == nil {
			return fmt.Errorf("txn %d: delete payload missing", t.Zxid)
		}
		st, err := tr.Stat(t.Delete.Path)
		if err == tree.ErrNoNode {
			return nil // already applied
		}
		if err != nil {
			return err
		}
		if t.Delete.TargetCzxid != 0 && st.Czxid != t.Delete.TargetCzxid {
			return nil // delete targeted an older incarnation at the same path
		}
		return tr.ApplyDelete(t.Delete.Path, uint64(t.Zxid), t.Delete.Mtime)

	case TypeSetData:
		if t.SetData == nil {
			return fmt.Errorf("txn %d: setdata payload missing", t.Zxid)
		}
		st, err := tr.Stat(t.SetData.Path)
		if err == tree.ErrNoNode && t.SetData.TargetCzxid != 0 {
			return nil // the targeted incarnation was already deleted
		}
		if err != nil {
			return err
		}
		if t.SetData.TargetCzxid != 0 && st.Czxid != t.SetData.TargetCzxid {
			return nil // set targeted a different incarnation at the same path
		}
		// ApplySetData handles the version-based idempotency check.
		return tr.ApplySetData(t.SetData.Path, t.SetData.Data, t.SetData.NewVersion, t.SetData.Mtime, uint64(t.Zxid))

	case TypeError:
		// Error txns occupy a log slot for ordering but don't touch state.
		return nil

	default:
		return fmt.Errorf("txn %d: unknown type %d", t.Zxid, t.Type)
	}
}

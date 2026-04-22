package txn

import (
	"bytes"
	"testing"

	"kansi/internal/tree"
)

func TestApplyCreate(t *testing.T) {
	tr := tree.NewTree()
	err := Apply(tr, &Txn{
		Zxid: 1, Type: TypeCreate,
		Create: &CreateTxn{Path: "/foo", Data: []byte("hi"), Ctime: 100},
	})
	if err != nil {
		t.Fatalf("Apply: %v", err)
	}

	data, v, err := tr.GetData("/foo")
	if err != nil {
		t.Fatalf("GetData: %v", err)
	}
	if !bytes.Equal(data, []byte("hi")) || v != 0 {
		t.Errorf("data=%q version=%d", data, v)
	}
}

func TestApplyCreateIdempotent(t *testing.T) {
	tr := tree.NewTree()
	txn := &Txn{
		Zxid: 1, Type: TypeCreate,
		Create: &CreateTxn{Path: "/foo", Data: []byte("hi"), Ctime: 100},
	}
	if err := Apply(tr, txn); err != nil {
		t.Fatal(err)
	}
	// replay — must not error, must not duplicate
	if err := Apply(tr, txn); err != nil {
		t.Fatalf("replay: %v", err)
	}
}

func TestApplySetDataIdempotent(t *testing.T) {
	tr := tree.NewTree()
	_ = Apply(tr, &Txn{
		Zxid: 1, Type: TypeCreate,
		Create: &CreateTxn{Path: "/foo", Data: []byte("v0")},
	})

	set := &Txn{
		Zxid: 2, Type: TypeSetData,
		SetData: &SetDataTxn{Path: "/foo", Data: []byte("v1"), NewVersion: 1, Mtime: 200},
	}
	if err := Apply(tr, set); err != nil {
		t.Fatal(err)
	}
	// replay should be a no-op (znode is already at version 1)
	if err := Apply(tr, set); err != nil {
		t.Fatal(err)
	}
	data, v, _ := tr.GetData("/foo")
	if !bytes.Equal(data, []byte("v1")) || v != 1 {
		t.Errorf("data=%q version=%d", data, v)
	}
}

func TestApplyDeleteIdempotent(t *testing.T) {
	tr := tree.NewTree()
	_ = Apply(tr, &Txn{
		Zxid: 1, Type: TypeCreate,
		Create: &CreateTxn{Path: "/foo"},
	})
	del := &Txn{Zxid: 2, Type: TypeDelete, Delete: &DeleteTxn{Path: "/foo"}}
	if err := Apply(tr, del); err != nil {
		t.Fatal(err)
	}
	// replay
	if err := Apply(tr, del); err != nil {
		t.Fatal(err)
	}
	if ok, _ := tr.Exists("/foo"); ok {
		t.Error("/foo still exists after delete")
	}
}

func TestApplyError(t *testing.T) {
	tr := tree.NewTree()
	// error txns don't touch state
	err := Apply(tr, &Txn{Zxid: 1, Type: TypeError, Error: &ErrorTxn{Code: 42}})
	if err != nil {
		t.Fatal(err)
	}
}

package recovery

import (
	"bytes"
	"os"
	"path/filepath"
	"testing"

	"kansi/internal/snapshot"
	"kansi/internal/tree"
	"kansi/internal/txn"
	"kansi/internal/txnlog"
)

func paths(t *testing.T) (string, string) {
	t.Helper()
	d := t.TempDir()
	return filepath.Join(d, "snap"), filepath.Join(d, "log")
}

func TestRecoverEmptyState(t *testing.T) {
	snap, log := paths(t)
	res, err := Recover(snap, log)
	if err != nil {
		t.Fatal(err)
	}
	if res.LastZxid != 0 {
		t.Errorf("lastZxid = %d", res.LastZxid)
	}
	if ok, _ := res.Tree.Exists("/"); !ok {
		t.Error("root missing")
	}
}

func TestRecoverFromLogOnly(t *testing.T) {
	// no snapshot; replay the whole log from zero
	snap, logPath := paths(t)
	w, _ := txnlog.Create(logPath)
	w.Append(&txn.Txn{Zxid: 1, Type: txn.TypeCreate, Create: &txn.CreateTxn{Path: "/a", Data: []byte("v0")}})
	w.Append(&txn.Txn{Zxid: 2, Type: txn.TypeSetData, SetData: &txn.SetDataTxn{Path: "/a", Data: []byte("v1"), NewVersion: 1}})
	w.Close()

	res, err := Recover(snap, logPath)
	if err != nil {
		t.Fatal(err)
	}
	if res.LastZxid != 2 {
		t.Errorf("lastZxid = %d", res.LastZxid)
	}
	data, v, _ := res.Tree.GetData("/a")
	if !bytes.Equal(data, []byte("v1")) || v != 1 {
		t.Errorf("data=%q version=%d", data, v)
	}
}

func TestRecoverFromSnapshotAndLog(t *testing.T) {
	// snapshot captures state up to zxid 10; log extends from 11 onward
	snap, logPath := paths(t)

	// build initial state, snapshot it
	tr := tree.NewTree()
	_, _ = tr.Create("/a", []byte("old"), 0, tree.CreateFlags{})
	if err := snapshot.Write(snap, tr, 10); err != nil {
		t.Fatal(err)
	}

	// log contains txns after the snapshot point
	w, _ := txnlog.Create(logPath)
	w.Append(&txn.Txn{Zxid: 11, Type: txn.TypeSetData, SetData: &txn.SetDataTxn{Path: "/a", Data: []byte("new"), NewVersion: 1}})
	w.Append(&txn.Txn{Zxid: 12, Type: txn.TypeCreate, Create: &txn.CreateTxn{Path: "/b"}})
	w.Close()

	res, err := Recover(snap, logPath)
	if err != nil {
		t.Fatal(err)
	}
	if res.LastZxid != 12 {
		t.Errorf("lastZxid = %d", res.LastZxid)
	}
	data, v, _ := res.Tree.GetData("/a")
	if !bytes.Equal(data, []byte("new")) || v != 1 {
		t.Errorf("/a data=%q version=%d", data, v)
	}
	if ok, _ := res.Tree.Exists("/b"); !ok {
		t.Error("/b missing")
	}
}

func TestRecoverIdempotentReplay(t *testing.T) {
	// log contains txns that the snapshot ALSO already captured.
	// Replay must be safe — final state should match.
	snap, logPath := paths(t)

	tr := tree.NewTree()
	_, _ = tr.Create("/a", []byte("v0"), 0, tree.CreateFlags{})
	_, _ = tr.SetData("/a", []byte("v1"), 0) // DataVersion now 1
	if err := snapshot.Write(snap, tr, 1); err != nil {
		t.Fatal(err)
	}

	// log starts at zxid 1 (same as snapshot.StartZxid) and includes
	// the SetData that's already in the snapshot, plus a newer one
	w, _ := txnlog.Create(logPath)
	w.Append(&txn.Txn{Zxid: 1, Type: txn.TypeCreate, Create: &txn.CreateTxn{Path: "/a", Data: []byte("v0")}})
	w.Append(&txn.Txn{Zxid: 2, Type: txn.TypeSetData, SetData: &txn.SetDataTxn{Path: "/a", Data: []byte("v1"), NewVersion: 1}})
	w.Append(&txn.Txn{Zxid: 3, Type: txn.TypeSetData, SetData: &txn.SetDataTxn{Path: "/a", Data: []byte("v2"), NewVersion: 2}})
	w.Close()

	res, err := Recover(snap, logPath)
	if err != nil {
		t.Fatal(err)
	}
	data, v, _ := res.Tree.GetData("/a")
	if !bytes.Equal(data, []byte("v2")) || v != 2 {
		t.Errorf("data=%q version=%d (want v2/2)", data, v)
	}
}

func TestRecoverTornLogTail(t *testing.T) {
	// log has a torn final record. Recovery must not return an error —
	// good records before the tear must be applied.
	snap, logPath := paths(t)

	w, _ := txnlog.Create(logPath)
	w.Append(&txn.Txn{Zxid: 1, Type: txn.TypeCreate, Create: &txn.CreateTxn{Path: "/a"}})
	w.Close()
	// tack on a partial header — simulates crash mid-append
	f, _ := os.OpenFile(logPath, os.O_WRONLY|os.O_APPEND, 0)
	f.Write([]byte{0x10, 0x00, 0x00}) // 3 bytes of a 4-byte length field
	f.Close()

	res, err := Recover(snap, logPath)
	if err != nil {
		t.Fatalf("Recover: %v", err)
	}
	if ok, _ := res.Tree.Exists("/a"); !ok {
		t.Error("/a missing after recovery past torn tail")
	}
}

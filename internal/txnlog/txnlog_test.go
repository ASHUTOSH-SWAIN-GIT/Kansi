package txnlog

import (
	"os"
	"path/filepath"
	"testing"

	"kansi/internal/txn"
)

func logPath(t *testing.T) string {
	t.Helper()
	return filepath.Join(t.TempDir(), "log")
}

func mustAppend(t *testing.T, w *Writer, tx *txn.Txn) {
	t.Helper()
	if err := w.Append(tx); err != nil {
		t.Fatalf("Append: %v", err)
	}
}

func TestRoundTrip(t *testing.T) {
	path := logPath(t)
	w, err := Create(path)
	if err != nil {
		t.Fatal(err)
	}
	mustAppend(t, w, &txn.Txn{
		Zxid: 1, Type: txn.TypeCreate,
		Create: &txn.CreateTxn{Path: "/a", Data: []byte("hello"), Ctime: 1},
	})
	mustAppend(t, w, &txn.Txn{
		Zxid: 2, Type: txn.TypeSetData,
		SetData: &txn.SetDataTxn{Path: "/a", Data: []byte("world"), NewVersion: 1, Mtime: 2},
	})
	w.Close()

	txns, err := ReadAll(path)
	if err != nil {
		t.Fatal(err)
	}
	if len(txns) != 2 {
		t.Fatalf("got %d txns, want 2", len(txns))
	}
	if txns[0].Zxid != 1 || txns[1].Zxid != 2 {
		t.Errorf("zxids: %d %d", txns[0].Zxid, txns[1].Zxid)
	}
	if string(txns[1].SetData.Data) != "world" {
		t.Errorf("payload corrupted: %q", txns[1].SetData.Data)
	}
}

func TestTornTailHeader(t *testing.T) {
	// crash between writing part of the header and the rest
	path := logPath(t)
	w, _ := Create(path)
	mustAppend(t, w, &txn.Txn{Zxid: 1, Type: txn.TypeCreate, Create: &txn.CreateTxn{Path: "/a"}})
	w.Close()

	// append 3 junk bytes (partial header)
	f, _ := os.OpenFile(path, os.O_WRONLY|os.O_APPEND, 0)
	f.Write([]byte{0xff, 0xff, 0xff})
	f.Close()

	txns, err := ReadAll(path)
	if err != nil {
		t.Fatalf("ReadAll: %v", err)
	}
	if len(txns) != 1 {
		t.Errorf("got %d txns, want 1 (torn tail should be skipped)", len(txns))
	}
}

func TestTornTailPayload(t *testing.T) {
	// write a valid record, then a header whose payload is truncated
	path := logPath(t)
	w, _ := Create(path)
	mustAppend(t, w, &txn.Txn{Zxid: 1, Type: txn.TypeCreate, Create: &txn.CreateTxn{Path: "/a"}})
	w.Close()

	// header claims 1000 bytes, only write 5
	f, _ := os.OpenFile(path, os.O_WRONLY|os.O_APPEND, 0)
	header := []byte{
		0xe8, 0x03, 0x00, 0x00, // length = 1000
		0x00, 0x00, 0x00, 0x00, // crc
	}
	f.Write(header)
	f.Write([]byte("short"))
	f.Close()

	txns, err := ReadAll(path)
	if err != nil {
		t.Fatalf("ReadAll: %v", err)
	}
	if len(txns) != 1 {
		t.Errorf("got %d, want 1", len(txns))
	}
}

func TestMidLogCorruptionDetected(t *testing.T) {
	// corrupt a byte in the middle record's payload -> CRC mismatch
	path := logPath(t)
	w, _ := Create(path)
	mustAppend(t, w, &txn.Txn{Zxid: 1, Type: txn.TypeCreate, Create: &txn.CreateTxn{Path: "/a"}})
	mustAppend(t, w, &txn.Txn{Zxid: 2, Type: txn.TypeCreate, Create: &txn.CreateTxn{Path: "/b"}})
	mustAppend(t, w, &txn.Txn{Zxid: 3, Type: txn.TypeCreate, Create: &txn.CreateTxn{Path: "/c"}})
	w.Close()

	// flip a byte somewhere in the middle
	f, _ := os.OpenFile(path, os.O_RDWR, 0)
	info, _ := f.Stat()
	mid := info.Size() / 2
	f.Seek(mid, 0)
	var b [1]byte
	f.Read(b[:])
	b[0] ^= 0xff
	f.Seek(mid, 0)
	f.Write(b[:])
	f.Close()

	_, err := ReadAll(path)
	if err != ErrCorrupt {
		// either ErrCorrupt or a gob decode error is acceptable as
		// "not silent"; just make sure we don't return nil
		if err == nil {
			t.Error("corrupted mid-log returned nil error")
		}
	}
}

func TestAppendFsyncs(t *testing.T) {
	// not a real crash test, just confirms Append returns no error
	// and the record is readable through a fresh open (i.e. actually
	// on disk, not just in a buffer).
	path := logPath(t)
	w, _ := Create(path)
	mustAppend(t, w, &txn.Txn{Zxid: 42, Type: txn.TypeCreate, Create: &txn.CreateTxn{Path: "/x"}})
	// don't close! simulate a crash after fsync but before close
	txns, err := ReadAll(path)
	if err != nil {
		t.Fatal(err)
	}
	if len(txns) != 1 || txns[0].Zxid != 42 {
		t.Errorf("txns=%v", txns)
	}
	w.Close()
}

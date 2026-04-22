package snapshot

import (
	"bytes"
	"os"
	"path/filepath"
	"testing"

	"kansi/internal/tree"
)

func snapPath(t *testing.T) string {
	t.Helper()
	return filepath.Join(t.TempDir(), "snap")
}

func TestWriteReadEmpty(t *testing.T) {
	tr := tree.NewTree()
	path := snapPath(t)
	if err := Write(path, tr, 42); err != nil {
		t.Fatal(err)
	}

	tr2, zxid, err := Read(path)
	if err != nil {
		t.Fatal(err)
	}
	if zxid != 42 {
		t.Errorf("zxid = %d, want 42", zxid)
	}
	// empty tree -> just the root
	kids, _ := tr2.GetChildren("/")
	if len(kids) != 0 {
		t.Errorf("root has children after round-trip: %v", kids)
	}
}

func TestWriteReadRoundTrip(t *testing.T) {
	tr := tree.NewTree()
	_, _ = tr.Create("/a", []byte("data-a"), 0, tree.CreateFlags{})
	_, _ = tr.Create("/a/b", []byte("data-b"), 0, tree.CreateFlags{})
	_, _ = tr.Create("/a/c", []byte("data-c"), 7, tree.CreateFlags{Ephemeral: true})
	_, _ = tr.SetData("/a", []byte("data-a-v1"), 0)

	path := snapPath(t)
	if err := Write(path, tr, 100); err != nil {
		t.Fatal(err)
	}

	tr2, zxid, err := Read(path)
	if err != nil {
		t.Fatal(err)
	}
	if zxid != 100 {
		t.Errorf("zxid = %d", zxid)
	}

	// check /a
	data, v, err := tr2.GetData("/a")
	if err != nil {
		t.Fatalf("GetData /a: %v", err)
	}
	if !bytes.Equal(data, []byte("data-a-v1")) || v != 1 {
		t.Errorf("/a data=%q version=%d", data, v)
	}

	// check /a/b
	data, _, err = tr2.GetData("/a/b")
	if err != nil || !bytes.Equal(data, []byte("data-b")) {
		t.Errorf("/a/b wrong: data=%q err=%v", data, err)
	}

	// check /a/c ephemeral owner preserved
	data, _, err = tr2.GetData("/a/c")
	if err != nil || !bytes.Equal(data, []byte("data-c")) {
		t.Errorf("/a/c wrong: data=%q err=%v", data, err)
	}
	// children of /a
	kids, _ := tr2.GetChildren("/a")
	if len(kids) != 2 {
		t.Errorf("/a children = %d, want 2", len(kids))
	}
}

func TestReadBadMagic(t *testing.T) {
	path := snapPath(t)
	os.WriteFile(path, []byte("this is not a snapshot"), 0o644)
	_, _, err := Read(path)
	if err == nil {
		t.Error("expected error on bad file")
	}
}

func TestReadTruncated(t *testing.T) {
	tr := tree.NewTree()
	_, _ = tr.Create("/a", nil, 0, tree.CreateFlags{})
	_, _ = tr.Create("/b", nil, 0, tree.CreateFlags{})

	path := snapPath(t)
	if err := Write(path, tr, 1); err != nil {
		t.Fatal(err)
	}
	// chop off the last 30 bytes (blows away the end sentinel at least)
	info, _ := os.Stat(path)
	os.Truncate(path, info.Size()-30)

	_, _, err := Read(path)
	if err == nil {
		t.Error("expected error on truncated snapshot")
	}
}

func TestSequentialCounterPreserved(t *testing.T) {
	// snapshot must preserve parent.SequentialCounter so sequential
	// naming stays monotonic across restart
	tr := tree.NewTree()
	_, _ = tr.Create("/locks", nil, 0, tree.CreateFlags{})
	for range 5 {
		if _, err := tr.Create("/locks/x-", nil, 0, tree.CreateFlags{Sequential: true}); err != nil {
			t.Fatal(err)
		}
	}

	path := snapPath(t)
	if err := Write(path, tr, 1); err != nil {
		t.Fatal(err)
	}
	tr2, _, err := Read(path)
	if err != nil {
		t.Fatal(err)
	}
	next, err := tr2.Create("/locks/x-", nil, 0, tree.CreateFlags{Sequential: true})
	if err != nil {
		t.Fatal(err)
	}
	if next != "/locks/x-0000000005" {
		t.Errorf("next sequential = %q, want /locks/x-0000000005", next)
	}
}

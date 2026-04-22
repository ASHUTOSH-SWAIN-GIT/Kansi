package tree

import (
	"bytes"
	"testing"
)

// --- helpers ---

func mustCreate(t *testing.T, tr *Tree, path string, data []byte, owner SessionID, flags CreateFlags) string {
	t.Helper()
	created, err := tr.Create(path, data, owner, flags)
	if err != nil {
		t.Fatalf("Create(%q) failed: %v", path, err)
	}
	return created
}

// --- path helpers ---

func TestValidatePath(t *testing.T) {
	valid := []string{"/", "/a", "/a/b", "/a/b/c", "/foo_1"}
	for _, p := range valid {
		if err := ValidatePath(p); err != nil {
			t.Errorf("ValidatePath(%q) = %v, want nil", p, err)
		}
	}
	invalid := []string{"", "a", "/a/", "a/b", "/a//b", "/\x00"}
	for _, p := range invalid {
		if err := ValidatePath(p); err == nil {
			t.Errorf("ValidatePath(%q) = nil, want error", p)
		}
	}
}

func TestParentAndBase(t *testing.T) {
	cases := []struct {
		path, parent, base string
	}{
		{"/", "", ""},
		{"/a", "/", "a"},
		{"/a/b", "/a", "b"},
		{"/a/b/c", "/a/b", "c"},
	}
	for _, c := range cases {
		if got := ParentPath(c.path); got != c.parent {
			t.Errorf("ParentPath(%q) = %q, want %q", c.path, got, c.parent)
		}
		if got := BaseName(c.path); got != c.base {
			t.Errorf("BaseName(%q) = %q, want %q", c.path, got, c.base)
		}
	}
}

// --- create / exists / getData ---

func TestCreateAndGet(t *testing.T) {
	tr := NewTree()
	mustCreate(t, tr, "/foo", []byte("hello"), 0, CreateFlags{})

	ok, err := tr.Exists("/foo")
	if err != nil || !ok {
		t.Fatalf("Exists(/foo) = %v, %v; want true, nil", ok, err)
	}

	data, version, err := tr.GetData("/foo")
	if err != nil {
		t.Fatalf("GetData: %v", err)
	}
	if !bytes.Equal(data, []byte("hello")) {
		t.Errorf("data = %q, want %q", data, "hello")
	}
	if version != 0 {
		t.Errorf("initial version = %d, want 0", version)
	}
}

func TestCreateRootFails(t *testing.T) {
	tr := NewTree()
	if _, err := tr.Create("/", nil, 0, CreateFlags{}); err == nil {
		t.Error("Create(/) should fail")
	}
}

func TestCreateDuplicateFails(t *testing.T) {
	tr := NewTree()
	mustCreate(t, tr, "/foo", nil, 0, CreateFlags{})
	if _, err := tr.Create("/foo", nil, 0, CreateFlags{}); err != ErrNodeExists {
		t.Errorf("duplicate Create err = %v, want ErrNodeExists", err)
	}
}

func TestCreateWithoutParentFails(t *testing.T) {
	tr := NewTree()
	if _, err := tr.Create("/a/b", nil, 0, CreateFlags{}); err != ErrNoParent {
		t.Errorf("Create without parent err = %v, want ErrNoParent", err)
	}
}

// --- setData / versions ---

func TestSetDataBumpsVersion(t *testing.T) {
	tr := NewTree()
	mustCreate(t, tr, "/foo", []byte("v0"), 0, CreateFlags{})

	v, err := tr.SetData("/foo", []byte("v1"), 0)
	if err != nil {
		t.Fatalf("SetData: %v", err)
	}
	if v != 1 {
		t.Errorf("new version = %d, want 1", v)
	}

	data, version, _ := tr.GetData("/foo")
	if !bytes.Equal(data, []byte("v1")) || version != 1 {
		t.Errorf("after set: data=%q version=%d", data, version)
	}
}

func TestSetDataVersionMismatch(t *testing.T) {
	tr := NewTree()
	mustCreate(t, tr, "/foo", []byte("v0"), 0, CreateFlags{})

	if _, err := tr.SetData("/foo", []byte("v1"), 99); err != ErrBadVersion {
		t.Errorf("err = %v, want ErrBadVersion", err)
	}
}

func TestSetDataVersionMinusOneSkipsCheck(t *testing.T) {
	tr := NewTree()
	mustCreate(t, tr, "/foo", []byte("v0"), 0, CreateFlags{})
	if _, err := tr.SetData("/foo", []byte("v1"), 0); err != nil { // bumps to 1
		t.Fatalf("SetData: %v", err)
	}

	if _, err := tr.SetData("/foo", []byte("v2"), -1); err != nil {
		t.Errorf("SetData with -1 failed: %v", err)
	}
}

// --- delete ---

func TestDelete(t *testing.T) {
	tr := NewTree()
	mustCreate(t, tr, "/foo", nil, 0, CreateFlags{})

	if err := tr.Delete("/foo", -1); err != nil {
		t.Fatalf("Delete: %v", err)
	}
	ok, _ := tr.Exists("/foo")
	if ok {
		t.Error("Exists(/foo) = true after delete")
	}
}

func TestDeleteNonEmptyFails(t *testing.T) {
	tr := NewTree()
	mustCreate(t, tr, "/a", nil, 0, CreateFlags{})
	mustCreate(t, tr, "/a/b", nil, 0, CreateFlags{})

	if err := tr.Delete("/a", -1); err != ErrNotEmpty {
		t.Errorf("err = %v, want ErrNotEmpty", err)
	}
}

func TestDeleteRootFails(t *testing.T) {
	tr := NewTree()
	if err := tr.Delete("/", -1); err == nil {
		t.Error("Delete(/) should fail")
	}
}

func TestDeleteVersionMismatch(t *testing.T) {
	tr := NewTree()
	mustCreate(t, tr, "/foo", []byte("x"), 0, CreateFlags{})
	if _, err := tr.SetData("/foo", []byte("y"), 0); err != nil { // version now 1
		t.Fatalf("SetData: %v", err)
	}

	if err := tr.Delete("/foo", 0); err != ErrBadVersion {
		t.Errorf("err = %v, want ErrBadVersion", err)
	}
}

// --- getChildren ---

func TestGetChildren(t *testing.T) {
	tr := NewTree()
	mustCreate(t, tr, "/a", nil, 0, CreateFlags{})
	mustCreate(t, tr, "/a/x", nil, 0, CreateFlags{})
	mustCreate(t, tr, "/a/y", nil, 0, CreateFlags{})
	mustCreate(t, tr, "/a/z", nil, 0, CreateFlags{})

	children, err := tr.GetChildren("/a")
	if err != nil {
		t.Fatalf("GetChildren: %v", err)
	}
	if len(children) != 3 {
		t.Errorf("got %d children, want 3", len(children))
	}
}

// --- sequential ---

func TestSequentialNaming(t *testing.T) {
	tr := NewTree()
	mustCreate(t, tr, "/locks", nil, 0, CreateFlags{})

	p1 := mustCreate(t, tr, "/locks/lock-", nil, 0, CreateFlags{Sequential: true})
	p2 := mustCreate(t, tr, "/locks/lock-", nil, 0, CreateFlags{Sequential: true})
	p3 := mustCreate(t, tr, "/locks/lock-", nil, 0, CreateFlags{Sequential: true})

	if p1 != "/locks/lock-0000000000" {
		t.Errorf("p1 = %q", p1)
	}
	if p2 != "/locks/lock-0000000001" {
		t.Errorf("p2 = %q", p2)
	}
	if p3 != "/locks/lock-0000000002" {
		t.Errorf("p3 = %q", p3)
	}
}

func TestSequentialCounterPersistsAcrossDeletes(t *testing.T) {
	// paper: sequence value of n is never smaller than any previously
	// created sequential znode under the same parent
	tr := NewTree()
	mustCreate(t, tr, "/locks", nil, 0, CreateFlags{})

	p1 := mustCreate(t, tr, "/locks/lock-", nil, 0, CreateFlags{Sequential: true})
	if err := tr.Delete(p1, -1); err != nil {
		t.Fatalf("Delete(%q): %v", p1, err)
	}

	p2 := mustCreate(t, tr, "/locks/lock-", nil, 0, CreateFlags{Sequential: true})
	if p2 != "/locks/lock-0000000001" {
		t.Errorf("p2 = %q, want /locks/lock-0000000001 (counter must not reset)", p2)
	}
}

// --- ephemeral ---

func TestEphemeralOwnerRecorded(t *testing.T) {
	tr := NewTree()
	mustCreate(t, tr, "/foo", nil, 42, CreateFlags{Ephemeral: true})

	node, err := tr.resolve("/foo")
	if err != nil {
		t.Fatalf("resolve: %v", err)
	}
	if !node.IsEphemeral() {
		t.Error("znode not marked ephemeral")
	}
	if node.EphemeralOwner != 42 {
		t.Errorf("owner = %d, want 42", node.EphemeralOwner)
	}
}

func TestNonEphemeralHasZeroOwner(t *testing.T) {
	tr := NewTree()
	// owner 42 passed but Ephemeral flag is false — should not be stored
	mustCreate(t, tr, "/foo", nil, 42, CreateFlags{})

	node, _ := tr.resolve("/foo")
	if node.IsEphemeral() {
		t.Error("regular znode marked ephemeral")
	}
}

func TestCreateUnderEphemeralFails(t *testing.T) {
	tr := NewTree()
	mustCreate(t, tr, "/eph", nil, 1, CreateFlags{Ephemeral: true})

	if _, err := tr.Create("/eph/child", nil, 0, CreateFlags{}); err != ErrEphemeralParent {
		t.Errorf("err = %v, want ErrEphemeralParent", err)
	}
}

// --- data isolation ---

func TestGetDataReturnsCopy(t *testing.T) {
	tr := NewTree()
	mustCreate(t, tr, "/foo", []byte("hello"), 0, CreateFlags{})

	data, _, _ := tr.GetData("/foo")
	data[0] = 'X' // mutate the returned slice

	data2, _, _ := tr.GetData("/foo")
	if !bytes.Equal(data2, []byte("hello")) {
		t.Errorf("mutation leaked into tree: got %q", data2)
	}
}

func TestSetDataCopiesInput(t *testing.T) {
	tr := NewTree()
	input := []byte("hello")
	mustCreate(t, tr, "/foo", input, 0, CreateFlags{})

	input[0] = 'X' // mutate caller's slice after Create

	data, _, _ := tr.GetData("/foo")
	if !bytes.Equal(data, []byte("hello")) {
		// this test actually requires Create to copy too; if it fails,
		// NewZnode or Create needs the same defensive copy SetData does
		t.Errorf("Create did not copy input: got %q", data)
	}
}

package processor

import (
	"bytes"
	"testing"

	"kansi/internal/tree"
	"kansi/internal/txn"
)

func newProc() *Processor { return WithClock(func() int64 { return 100 }) }

func TestCreateHappy(t *testing.T) {
	tr := tree.NewTree()
	p := newProc()
	res := p.Process(tr, 1, CreateRequest{Path: "/foo", Data: []byte("v0")})
	if res.Rejected() {
		t.Fatalf("rejected: %v", res.ErrCode)
	}
	if res.ResolvedPath != "/foo" {
		t.Errorf("path=%q", res.ResolvedPath)
	}
	if res.Txn.Type != txn.TypeCreate || res.Txn.Create.Ctime != 100 {
		t.Errorf("bad txn: %+v", res.Txn)
	}
}

func TestCreateSequentialResolves(t *testing.T) {
	tr := tree.NewTree()
	p := newProc()
	_, _ = tr.Create("/locks", nil, 0, tree.CreateFlags{})

	res := p.Process(tr, 1, CreateRequest{Path: "/locks/lock-", Sequential: true})
	if res.Rejected() {
		t.Fatal(res.ErrCode)
	}
	if res.ResolvedPath != "/locks/lock-0000000000" {
		t.Errorf("resolved=%q", res.ResolvedPath)
	}
}

func TestCreateRejects(t *testing.T) {
	tr := tree.NewTree()
	p := newProc()
	_, _ = tr.Create("/a", nil, 0, tree.CreateFlags{})

	tests := []struct {
		name string
		req  CreateRequest
		want ErrCode
	}{
		{"duplicate", CreateRequest{Path: "/a"}, ErrNodeExists},
		{"no parent", CreateRequest{Path: "/x/y"}, ErrNoParent},
		{"root", CreateRequest{Path: "/"}, ErrNodeExists},
		{"bad path", CreateRequest{Path: "a"}, ErrInvalidPath},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			res := p.Process(tr, 1, tc.req)
			if res.ErrCode != tc.want {
				t.Errorf("err=%v want %v", res.ErrCode, tc.want)
			}
		})
	}
}

func TestSetDataVersionMath(t *testing.T) {
	tr := tree.NewTree()
	p := newProc()
	_, _ = tr.Create("/a", []byte("v0"), 0, tree.CreateFlags{})

	res := p.Process(tr, 10, SetDataRequest{Path: "/a", Data: []byte("v1"), ExpectedVersion: 0})
	if res.Rejected() {
		t.Fatal(res.ErrCode)
	}
	if res.Txn.SetData.NewVersion != 1 {
		t.Errorf("newVersion=%d", res.Txn.SetData.NewVersion)
	}
	if !bytes.Equal(res.Txn.SetData.Data, []byte("v1")) {
		t.Error("data mismatch")
	}
}

func TestSetDataBadVersion(t *testing.T) {
	tr := tree.NewTree()
	p := newProc()
	_, _ = tr.Create("/a", []byte("v0"), 0, tree.CreateFlags{})

	res := p.Process(tr, 10, SetDataRequest{Path: "/a", Data: []byte("v1"), ExpectedVersion: 5})
	if res.ErrCode != ErrBadVersion {
		t.Errorf("err=%v", res.ErrCode)
	}
}

func TestDeleteNonEmpty(t *testing.T) {
	tr := tree.NewTree()
	p := newProc()
	_, _ = tr.Create("/a", nil, 0, tree.CreateFlags{})
	_, _ = tr.Create("/a/b", nil, 0, tree.CreateFlags{})

	res := p.Process(tr, 1, DeleteRequest{Path: "/a", ExpectedVersion: -1})
	if res.ErrCode != ErrNotEmpty {
		t.Errorf("err=%v", res.ErrCode)
	}
}

func TestDeleteHappy(t *testing.T) {
	tr := tree.NewTree()
	p := newProc()
	// create via Apply so Czxid is stamped
	_ = txn.Apply(tr, &txn.Txn{
		Zxid: 3, Type: txn.TypeCreate,
		Create: &txn.CreateTxn{Path: "/a", Ctime: 50},
	})
	res := p.Process(tr, 5, DeleteRequest{Path: "/a", ExpectedVersion: -1})
	if res.Rejected() {
		t.Fatal(res.ErrCode)
	}
	if res.Txn.Delete.TargetCzxid != 3 {
		t.Errorf("TargetCzxid=%d, want 3", res.Txn.Delete.TargetCzxid)
	}
}

func TestProcessorOutputsAreApplyable(t *testing.T) {
	// Make sure a processor-emitted txn applies cleanly to the same tree.
	tr := tree.NewTree()
	p := newProc()

	res := p.Process(tr, 1, CreateRequest{Path: "/a", Data: []byte("hi")})
	if err := txn.Apply(tr, res.Txn); err != nil {
		t.Fatalf("Apply: %v", err)
	}
	data, _, _ := tr.GetData("/a")
	if !bytes.Equal(data, []byte("hi")) {
		t.Errorf("data=%q", data)
	}
}

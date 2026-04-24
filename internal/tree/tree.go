package tree

import (
	"errors"
	"fmt"
	"time"
)

// Tree operation errors.
var (
	ErrNoNode          = errors.New("znode does not exist")
	ErrNodeExists      = errors.New("znode already exists")
	ErrBadVersion      = errors.New("version mismatch")
	ErrNotEmpty        = errors.New("znode has children")
	ErrNoParent        = errors.New("parent does not exist")
	ErrEphemeralParent = errors.New("ephemeral znode cannot have children")
)

// CreateFlags controls znode creation behavior.
type CreateFlags struct {
	Ephemeral  bool
	Sequential bool
}

// Tree is the in-memory znode hierarchy. Not safe for concurrent use;
// the server layer will wrap it with a mutex.
type Tree struct {
	root *Znode
}

// Stat is a stable copy of a znode's metadata.
type Stat struct {
	DataVersion       int64
	ChildrenVersion   int64
	ACLversion        int64
	Ctime             int64
	Mtime             int64
	EphemeralOwner    SessionID
	SequentialCounter int64
	NumChildren       int
	Czxid             uint64
	Mzxid             uint64
	Pzxid             uint64
}

// NewTree returns a Tree with an empty, non-ephemeral root znode.
func NewTree() *Tree {
	// root always exists, is never ephemeral, has no data
	return &Tree{root: NewZnode(nil, 0)}
}

// resolve walks the tree to find the znode at the path
// returns the znode or ErrNode if any segment along the way is missing
func (t *Tree) resolve(path string) (*Znode, error) {
	if err := ValidatePath(path); err != nil {
		return nil, err
	}
	node := t.root
	for _, seg := range SplitPath(path) {
		child, ok := node.Children[seg]
		if !ok {
			return nil, ErrNoNode
		}
		node = child
	}
	return node, nil
}

// Create adds a new node at path and returns the actual created path
// (which differs from the input when flags.Sequential is set).
func (t *Tree) Create(path string, data []byte, owner SessionID, flags CreateFlags) (string, error) {
	if err := ValidatePath(path); err != nil {
		return "", err
	}
	if path == "/" {
		return "", ErrNodeExists
	}

	parentPath := ParentPath(path)
	base := BaseName(path)

	parent, err := t.resolve(parentPath)
	if err != nil {
		return "", ErrNoParent
	}
	if parent.IsEphemeral() {
		return "", ErrEphemeralParent
	}

	// sequential flag: append zero-padded counter to the requested name
	if flags.Sequential {
		base = fmt.Sprintf("%s%010d", base, parent.SequentialCounter)
		parent.SequentialCounter++
	}

	if _, exists := parent.Children[base]; exists {
		return "", ErrNodeExists
	}

	var ownerID SessionID
	if flags.Ephemeral {
		ownerID = owner
	}

	child := NewZnode(data, ownerID)
	parent.Children[base] = child

	// parent's children list changed
	parent.ChildrenVersion++
	parent.Mtime = time.Now().UnixMilli()

	// actual path may differ from input when sequential
	if parentPath == "/" {
		return "/" + base, nil
	}
	return parentPath + "/" + base, nil
}

// Delete removes the znode at path. The expected version must match the
// znode's data version, or be -1 to skip the check. The znode must have
// no children.
func (t *Tree) Delete(path string, expectedVersion int64) error {
	if path == "/" {
		return ErrInvalidPath
	}

	node, err := t.resolve(path)
	if err != nil {
		return err
	}
	if expectedVersion != -1 && node.DataVersion != expectedVersion {
		return ErrBadVersion
	}
	if len(node.Children) > 0 {
		return ErrNotEmpty
	}

	parent, err := t.resolve(ParentPath(path))
	if err != nil {
		return err
	}
	delete(parent.Children, BaseName(path))
	parent.ChildrenVersion++
	parent.Mtime = time.Now().UnixMilli()
	return nil
}

// Exists returns true if the znode at path exists.
func (t *Tree) Exists(path string) (bool, error) {
	_, err := t.resolve(path)
	if err == ErrNoNode {
		return false, nil
	}
	if err != nil {
		return false, err
	}
	return true, nil
}

// SetData writes new data if expectedVersion matches (or is -1).
// Returns the new version.
func (t *Tree) SetData(path string, data []byte, expectedVersion int64) (int64, error) {
	node, err := t.resolve(path)
	if err != nil {
		return 0, err
	}
	if expectedVersion != -1 && node.DataVersion != expectedVersion {
		return 0, ErrBadVersion
	}
	// copy incoming data so external mutation doesn't affect stored state
	node.Data = append([]byte(nil), data...)
	node.DataVersion++
	node.Mtime = time.Now().UnixMilli()
	return node.DataVersion, nil
}

// GetChildren returns the names of the immediate children of the znode at path.
func (t *Tree) GetChildren(path string) ([]string, error) {
	node, err := t.resolve(path)
	if err != nil {
		return nil, err
	}
	names := make([]string, 0, len(node.Children))
	for name := range node.Children {
		names = append(names, name)
	}
	return names, nil
}

// GetData returns a copy of the znode's data and its current version.
// The copy is defensive — callers must not see writes mutate their slice.
func (t *Tree) GetData(path string) ([]byte, int64, error) {
	node, err := t.resolve(path)
	if err != nil {
		return nil, 0, err
	}
	out := make([]byte, len(node.Data))
	copy(out, node.Data)
	return out, node.DataVersion, nil
}

// Stat returns a copy of the znode metadata at path.
func (t *Tree) Stat(path string) (Stat, error) {
	node, err := t.resolve(path)
	if err != nil {
		return Stat{}, err
	}
	return Stat{
		DataVersion:       node.DataVersion,
		ChildrenVersion:   node.ChildrenVersion,
		ACLversion:        node.ACLversion,
		Ctime:             node.Ctime,
		Mtime:             node.Mtime,
		EphemeralOwner:    node.EphemeralOwner,
		SequentialCounter: node.SequentialCounter,
		NumChildren:       len(node.Children),
		Czxid:             node.Czxid,
		Mzxid:             node.Mzxid,
		Pzxid:             node.Pzxid,
	}, nil
}

// ApplyCreate directly inserts a znode. No validation — the caller
// has already verified the txn is valid. Used for txn log replay
// and for applying committed transactions from Zab.
func (t *Tree) ApplyCreate(path string, data []byte, owner SessionID, ctime int64, zxid uint64) error {
	parent, err := t.resolve(ParentPath(path))
	if err != nil {
		return err
	}
	base := BaseName(path)

	node := &Znode{
		Data:           append([]byte(nil), data...),
		Ctime:          ctime,
		Mtime:          ctime,
		EphemeralOwner: owner,
		Children:       make(map[string]*Znode),
		Czxid:          zxid,
		Mzxid:          zxid,
		Pzxid:          zxid,
	}
	parent.Children[base] = node
	parent.ChildrenVersion++
	parent.Pzxid = zxid
	parent.Mtime = ctime

	// keep the parent's sequential counter consistent with any
	// sequential suffix in base (e.g. "lock-0000000005" -> counter >= 6)
	if n := parseSequentialSuffix(base); n >= 0 && int64(n)+1 > parent.SequentialCounter {
		parent.SequentialCounter = int64(n) + 1
	}
	return nil
}

// ApplyDelete removes a znode after the caller has verified its identity.
func (t *Tree) ApplyDelete(path string, zxid uint64, mtime int64) error {
	parent, err := t.resolve(ParentPath(path))
	if err != nil {
		return err
	}
	delete(parent.Children, BaseName(path))
	parent.ChildrenVersion++
	parent.Pzxid = zxid
	parent.Mtime = mtime
	return nil
}

// ApplySetData writes data and sets the version directly.
// Idempotent: if the znode is already at a version >= newVersion,
// the write is skipped. This is what makes replay safe after a
// fuzzy snapshot (paper section 4.3).
func (t *Tree) ApplySetData(path string, data []byte, newVersion int64, mtime int64, zxid uint64) error {
	node, err := t.resolve(path)
	if err != nil {
		return err
	}
	if node.DataVersion >= newVersion {
		return nil // already applied; replay is a no-op
	}
	node.Data = append([]byte(nil), data...)
	node.DataVersion = newVersion
	node.Mzxid = zxid
	node.Mtime = mtime
	return nil
}

// Walk invokes visit for every znode in the tree in depth-first order,
// parents before children. Root is visited as path "/".
//
// Visit receives a live pointer to the znode — callers that need a
// stable copy (e.g. snapshotters) must copy fields they care about.
func (t *Tree) Walk(visit func(path string, z *Znode)) {
	walkNode(t.root, "/", visit)
}

func walkNode(z *Znode, path string, visit func(string, *Znode)) {
	visit(path, z)
	for name, child := range z.Children {
		var childPath string
		if path == "/" {
			childPath = "/" + name
		} else {
			childPath = path + "/" + name
		}
		walkNode(child, childPath, visit)
	}
}

// Restore inserts a fully-formed znode at path. Used during snapshot
// load — the caller is restoring pre-validated state, so no checks.
// Parent must already exist (the DFS snapshot walk ensures this).
// The root ("/") is restored by overwriting the existing root fields.
func (t *Tree) Restore(path string, z *Znode) error {
	if path == "/" {
		// preserve the root pointer but copy in the restored metadata;
		// children will be filled in by subsequent Restore calls.
		t.root.Data = append([]byte(nil), z.Data...)
		t.root.DataVersion = z.DataVersion
		t.root.ChildrenVersion = z.ChildrenVersion
		t.root.ACLversion = z.ACLversion
		t.root.Ctime = z.Ctime
		t.root.Mtime = z.Mtime
		t.root.EphemeralOwner = z.EphemeralOwner
		t.root.SequentialCounter = z.SequentialCounter
		t.root.Czxid = z.Czxid
		t.root.Mzxid = z.Mzxid
		t.root.Pzxid = z.Pzxid
		if t.root.Children == nil {
			t.root.Children = make(map[string]*Znode)
		}
		return nil
	}
	parent, err := t.resolve(ParentPath(path))
	if err != nil {
		return err
	}
	node := &Znode{
		Data:              append([]byte(nil), z.Data...),
		DataVersion:       z.DataVersion,
		ChildrenVersion:   z.ChildrenVersion,
		ACLversion:        z.ACLversion,
		Ctime:             z.Ctime,
		Mtime:             z.Mtime,
		EphemeralOwner:    z.EphemeralOwner,
		SequentialCounter: z.SequentialCounter,
		Czxid:             z.Czxid,
		Mzxid:             z.Mzxid,
		Pzxid:             z.Pzxid,
		Children:          make(map[string]*Znode),
	}
	parent.Children[BaseName(path)] = node
	return nil
}

// parseSequentialSuffix returns the numeric suffix of a sequential name,
// or -1 if the name doesn't end in 10 digits.
func parseSequentialSuffix(name string) int {
	if len(name) < 10 {
		return -1
	}
	suffix := name[len(name)-10:]
	n := 0
	for _, r := range suffix {
		if r < '0' || r > '9' {
			return -1
		}
		n = n*10 + int(r-'0')
	}
	return n
}

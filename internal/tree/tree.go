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

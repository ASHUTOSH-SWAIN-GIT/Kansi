package tree

import "time"

// SessionID identifies the client session that owns an ephemeral znode
// (the node is removed when the session dies). Zero means not ephemeral.
type SessionID int64

// Znode is the in-memory representation of a single ZooKeeper-style node.
type Znode struct {
	Data              []byte
	ChildrenVersion   int64
	ACLversion        int64
	Ctime             int64
	Mtime             int64
	EphemeralOwner    SessionID
	Children          map[string]*Znode
	SequentialCounter int64
	DataVersion       int64
}

// NewZnode constructs a fresh znode with creation/modification timestamps
// set to the current time. A non-zero owner makes the node ephemeral.
func NewZnode(data []byte, owner SessionID) *Znode {
	now := time.Now().UnixMilli()
	return &Znode{
		Data:           append([]byte(nil), data...),
		Ctime:          now,
		Mtime:          now,
		EphemeralOwner: owner,
		Children:       make(map[string]*Znode),
	}
}

// IsEphemeral reports whether the znode is owned by a client session.
func (z *Znode) IsEphemeral() bool {
	return z.EphemeralOwner != 0
}

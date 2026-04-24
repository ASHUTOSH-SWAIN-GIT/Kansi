// Package proto defines the kansi client-server wire format.
//
// Framing: every message on the wire is a 4-byte big-endian length
// prefix followed by a gob-encoded Envelope. The Envelope carries
// exactly one request or response payload plus a correlation id so
// the client can pair up async responses with the request that
// started them. Responses also carry the current zxid per the paper:
// clients use it to validate reconnects.
//
// Watch events are sent as unsolicited server-to-client Envelopes
// with Xid == XidWatch.
package proto

import (
	"bytes"
	"encoding/binary"
	"encoding/gob"
	"fmt"
	"io"

	"kansi/internal/processor"
	"kansi/internal/tree"
)

// XidWatch marks an Envelope as an unsolicited watch event, not a
// response to a request.
const XidWatch int64 = -1

// XidPing is the correlation id used for heartbeats.
const XidPing int64 = -2

// Envelope wraps every message on the wire.
type Envelope struct {
	Xid  int64 // correlation id; XidWatch for watch events
	Zxid uint64

	// Exactly one of these is non-nil.
	ConnectReq  *ConnectReq
	ConnectResp *ConnectResp

	CreateReq  *CreateReq
	CreateResp *CreateResp

	DeleteReq  *DeleteReq
	DeleteResp *DeleteResp

	SetDataReq  *SetDataReq
	SetDataResp *SetDataResp

	GetDataReq  *GetDataReq
	GetDataResp *GetDataResp

	ExistsReq  *ExistsReq
	ExistsResp *ExistsResp

	GetChildrenReq  *GetChildrenReq
	GetChildrenResp *GetChildrenResp

	PingReq  *PingReq
	PingResp *PingResp

	WatchEvent *WatchEvent

	// Generic error path (also used alongside *Resp when ErrCode != ok).
	ErrCode processor.ErrCode

	// Zab peer-to-peer payload (opaque to this package; zab handles it).
	ZabMsg []byte
}

// ConnectReq is the first message a client sends. SessionID=0 means
// "new session"; non-zero means "reattach to existing".
type ConnectReq struct {
	SessionID    int64
	TimeoutMS    int32
	LastZxidSeen uint64
}

// ConnectResp completes the handshake.
type ConnectResp struct {
	SessionID int64
	TimeoutMS int32 // server may downgrade
}

// CreateReq asks to create a znode.
type CreateReq struct {
	Path       string
	Data       []byte
	Ephemeral  bool
	Sequential bool
}

// CreateResp returns the actual path (differs from request when Sequential).
type CreateResp struct {
	Path string
}

// DeleteReq asks to remove a znode.
type DeleteReq struct {
	Path            string
	ExpectedVersion int64
}

// DeleteResp is empty on success.
type DeleteResp struct{}

// SetDataReq writes data.
type SetDataReq struct {
	Path            string
	Data            []byte
	ExpectedVersion int64
}

// SetDataResp returns the new version.
type SetDataResp struct {
	NewVersion int64
}

// GetDataReq reads data.
type GetDataReq struct {
	Path  string
	Watch bool // install a data watch
}

// GetDataResp carries the current data + stat.
type GetDataResp struct {
	Data []byte
	Stat tree.Stat
}

// ExistsReq tests for existence.
type ExistsReq struct {
	Path  string
	Watch bool // install an exists watch
}

// ExistsResp reports existence + stat if it exists.
type ExistsResp struct {
	Exists bool
	Stat   tree.Stat
}

// GetChildrenReq lists children.
type GetChildrenReq struct {
	Path  string
	Watch bool // install a child watch
}

// GetChildrenResp returns children names.
type GetChildrenResp struct {
	Children []string
}

// PingReq is a heartbeat.
type PingReq struct{}

// PingResp is a heartbeat reply.
type PingResp struct{}

// EventType enumerates watch event kinds.
type EventType int32

// Watch event types.
const (
	EventNodeCreated EventType = iota + 1
	EventNodeDeleted
	EventNodeDataChanged
	EventNodeChildrenChanged
)

// WatchEvent is an unsolicited server-to-client notification.
type WatchEvent struct {
	Type EventType
	Path string
}

// Encoder/decoder helpers -----------------------------------------------------

// Write sends one envelope with length prefix.
func Write(w io.Writer, e *Envelope) error {
	var buf []byte
	tmp := &frame{Env: e}
	b, err := encode(tmp)
	if err != nil {
		return err
	}
	buf = make([]byte, 4+len(b))
	binary.BigEndian.PutUint32(buf[:4], uint32(len(b)))
	copy(buf[4:], b)
	_, err = w.Write(buf)
	return err
}

// Read receives one envelope.
func Read(r io.Reader) (*Envelope, error) {
	var lenbuf [4]byte
	if _, err := io.ReadFull(r, lenbuf[:]); err != nil {
		return nil, err
	}
	n := binary.BigEndian.Uint32(lenbuf[:])
	if n > 16*1024*1024 { // 16MiB sanity cap
		return nil, fmt.Errorf("proto: frame too large: %d", n)
	}
	buf := make([]byte, n)
	if _, err := io.ReadFull(r, buf); err != nil {
		return nil, err
	}
	var f frame
	if err := decode(buf, &f); err != nil {
		return nil, err
	}
	return f.Env, nil
}

// frame lets us evolve the on-wire format without swapping encoders.
type frame struct {
	Env *Envelope
}

func encode(f *frame) ([]byte, error) {
	var buf bytes.Buffer
	if err := gob.NewEncoder(&buf).Encode(f); err != nil {
		return nil, err
	}
	return buf.Bytes(), nil
}

func decode(b []byte, f *frame) error {
	return gob.NewDecoder(bytes.NewReader(b)).Decode(f)
}

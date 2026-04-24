// Package zab implements atomic broadcast with a static leader.
//
// This is the minimum viable Zab: no election, no TRUNC, no epoch
// bumps. A configured leader accepts proposals, replicates them to
// followers over TCP, and commits when a majority (including itself)
// has acknowledged.
package zab

import (
	"bytes"
	"encoding/gob"
	"io"

	"kansi/internal/proto"
	"kansi/internal/txn"
)

// MsgType identifies a zab message kind.
type MsgType int32

// Message types on the zab peer-to-peer protocol.
const (
	MsgProposal MsgType = iota + 1
	MsgAck
	MsgCommit
	MsgSyncReq // follower -> leader: "catch me up from LastZxid+1"
	MsgSyncEnd // leader   -> follower: "you're caught up; resume live stream"
)

// Msg is one message on the zab wire.
type Msg struct {
	Type MsgType
	Txn  *txn.Txn // set for Proposal; set for Sync replay
	Zxid uint64   // set for Ack, Commit, SyncReq
}

// WriteMsg sends m to w using the same length-prefix gob framing as
// the client protocol, to keep dependencies small.
func WriteMsg(w io.Writer, m *Msg) error {
	return proto.Write(w, &proto.Envelope{ZabMsg: encodeMsg(m)})
}

// ReadMsg receives a Msg from r.
func ReadMsg(r io.Reader) (*Msg, error) {
	env, err := proto.Read(r)
	if err != nil {
		return nil, err
	}
	return decodeMsg(env.ZabMsg)
}

// encodeMsg / decodeMsg pack a zab Msg into bytes so we can piggyback
// on proto.Envelope (keeps one codec path).
func encodeMsg(m *Msg) []byte {
	var b bytes.Buffer
	_ = gob.NewEncoder(&b).Encode(m)
	return b.Bytes()
}

func decodeMsg(b []byte) (*Msg, error) {
	var m Msg
	if err := gob.NewDecoder(bytes.NewReader(b)).Decode(&m); err != nil {
		return nil, err
	}
	return &m, nil
}

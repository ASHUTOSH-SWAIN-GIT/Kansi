package proto

import (
	"bytes"
	"testing"
)

func TestRoundTrip(t *testing.T) {
	var buf bytes.Buffer
	out := &Envelope{
		Xid:       42,
		Zxid:      100,
		CreateReq: &CreateReq{Path: "/foo", Data: []byte("hi"), Ephemeral: true},
	}
	if err := Write(&buf, out); err != nil {
		t.Fatal(err)
	}
	in, err := Read(&buf)
	if err != nil {
		t.Fatal(err)
	}
	if in.Xid != 42 || in.Zxid != 100 || in.CreateReq == nil {
		t.Fatalf("lost data: %+v", in)
	}
	if in.CreateReq.Path != "/foo" || !bytes.Equal(in.CreateReq.Data, []byte("hi")) {
		t.Errorf("body=%+v", in.CreateReq)
	}
}

func TestMultipleFrames(t *testing.T) {
	var buf bytes.Buffer
	for i := int64(0); i < 3; i++ {
		Write(&buf, &Envelope{Xid: i, PingReq: &PingReq{}})
	}
	for i := int64(0); i < 3; i++ {
		e, err := Read(&buf)
		if err != nil {
			t.Fatal(err)
		}
		if e.Xid != i {
			t.Errorf("got xid %d want %d", e.Xid, i)
		}
	}
}

func TestWatchEvent(t *testing.T) {
	var buf bytes.Buffer
	Write(&buf, &Envelope{Xid: XidWatch, WatchEvent: &WatchEvent{Type: EventNodeDeleted, Path: "/x"}})
	e, err := Read(&buf)
	if err != nil {
		t.Fatal(err)
	}
	if e.Xid != XidWatch || e.WatchEvent.Path != "/x" {
		t.Errorf("%+v", e)
	}
}

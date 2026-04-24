// Package client is the kansi client library.
//
// It connects to a kansi server, establishes a session, pings on a
// timer, and exposes the familiar ZooKeeper-ish API (Create, Delete,
// Set, Get, Exists, Children) plus watch channels.
package client

import (
	"errors"
	"fmt"
	"net"
	"sync"
	"sync/atomic"
	"time"

	"kansi/internal/processor"
	"kansi/internal/proto"
	"kansi/internal/tree"
)

// ErrClosed is returned after the client is closed.
var ErrClosed = errors.New("client: closed")

// WatchEvent is delivered through a channel returned from Watch* APIs.
type WatchEvent = proto.WatchEvent

// Client is one connected client.
type Client struct {
	conn     net.Conn
	sid      int64
	timeout  time.Duration
	nextXid  atomic.Int64
	mu       sync.Mutex
	pending  map[int64]chan *proto.Envelope
	watchers map[int64]chan WatchEvent
	closed   atomic.Bool
	doneCh   chan struct{}
}

// Dial connects to addr, opens a session, and starts the background
// reader + heartbeat goroutines.
func Dial(addr string, timeout time.Duration) (*Client, error) {
	c, err := net.Dial("tcp", addr)
	if err != nil {
		return nil, err
	}
	cl := &Client{
		conn:     c,
		timeout:  timeout,
		pending:  make(map[int64]chan *proto.Envelope),
		watchers: make(map[int64]chan WatchEvent),
		doneCh:   make(chan struct{}),
	}

	// handshake
	xid := cl.allocXid()
	if err := proto.Write(c, &proto.Envelope{
		Xid:        xid,
		ConnectReq: &proto.ConnectReq{TimeoutMS: int32(timeout / time.Millisecond)},
	}); err != nil {
		_ = c.Close()
		return nil, err
	}
	resp, err := proto.Read(c)
	if err != nil {
		_ = c.Close()
		return nil, err
	}
	if resp.ConnectResp == nil {
		_ = c.Close()
		return nil, fmt.Errorf("client: unexpected connect response")
	}
	cl.sid = resp.ConnectResp.SessionID

	go cl.readLoop()
	go cl.pingLoop()

	return cl, nil
}

// SessionID returns the server-assigned session id.
func (c *Client) SessionID() int64 { return c.sid }

// Close terminates the client.
func (c *Client) Close() error {
	if !c.closed.CompareAndSwap(false, true) {
		return nil
	}
	close(c.doneCh)
	return c.conn.Close()
}

func (c *Client) allocXid() int64 { return c.nextXid.Add(1) }

// call sends req and waits for its paired response. Watch events
// route through a different path.
func (c *Client) call(req *proto.Envelope) (*proto.Envelope, error) {
	if c.closed.Load() {
		return nil, ErrClosed
	}
	xid := c.allocXid()
	req.Xid = xid
	ch := make(chan *proto.Envelope, 1)

	c.mu.Lock()
	c.pending[xid] = ch
	c.mu.Unlock()

	defer func() {
		c.mu.Lock()
		delete(c.pending, xid)
		c.mu.Unlock()
	}()

	if err := proto.Write(c.conn, req); err != nil {
		return nil, err
	}
	select {
	case resp := <-ch:
		return resp, nil
	case <-c.doneCh:
		return nil, ErrClosed
	}
}

func (c *Client) readLoop() {
	for {
		env, err := proto.Read(c.conn)
		if err != nil {
			_ = c.Close()
			return
		}
		if env.Xid == proto.XidWatch {
			c.dispatchWatch(env)
			continue
		}
		c.mu.Lock()
		ch, ok := c.pending[env.Xid]
		c.mu.Unlock()
		if ok {
			ch <- env
		}
	}
}

func (c *Client) pingLoop() {
	interval := c.timeout / 3
	if interval <= 0 {
		interval = time.Second
	}
	t := time.NewTicker(interval)
	defer t.Stop()
	for {
		select {
		case <-c.doneCh:
			return
		case <-t.C:
			_, _ = c.call(&proto.Envelope{PingReq: &proto.PingReq{}})
		}
	}
}

// dispatchWatch delivers a watch event to every subscribed channel.
// Watches are one-shot; we send to all current watchers and let the
// channel reader discard duplicates (cheap; expected count is small).
func (c *Client) dispatchWatch(env *proto.Envelope) {
	c.mu.Lock()
	watchers := make([]chan WatchEvent, 0, len(c.watchers))
	for _, ch := range c.watchers {
		watchers = append(watchers, ch)
	}
	c.mu.Unlock()
	for _, ch := range watchers {
		select {
		case ch <- *env.WatchEvent:
		default:
		}
	}
}

// Watches returns a channel that receives every watch event the
// server delivers to this session. The channel is closed on Close.
func (c *Client) Watches() <-chan WatchEvent {
	ch := make(chan WatchEvent, 16)
	c.mu.Lock()
	id := c.allocXid()
	c.watchers[id] = ch
	c.mu.Unlock()
	go func() {
		<-c.doneCh
		close(ch)
	}()
	return ch
}

// --- RPC wrappers ---

// Create makes a znode.
func (c *Client) Create(path string, data []byte, ephemeral, sequential bool) (string, error) {
	resp, err := c.call(&proto.Envelope{
		CreateReq: &proto.CreateReq{Path: path, Data: data, Ephemeral: ephemeral, Sequential: sequential},
	})
	if err != nil {
		return "", err
	}
	if resp.ErrCode != processor.ErrOK {
		return "", codeToErr(resp.ErrCode)
	}
	return resp.CreateResp.Path, nil
}

// Delete removes a znode.
func (c *Client) Delete(path string, expectedVersion int64) error {
	resp, err := c.call(&proto.Envelope{
		DeleteReq: &proto.DeleteReq{Path: path, ExpectedVersion: expectedVersion},
	})
	if err != nil {
		return err
	}
	if resp.ErrCode != processor.ErrOK {
		return codeToErr(resp.ErrCode)
	}
	return nil
}

// Set writes data.
func (c *Client) Set(path string, data []byte, expectedVersion int64) (int64, error) {
	resp, err := c.call(&proto.Envelope{
		SetDataReq: &proto.SetDataReq{Path: path, Data: data, ExpectedVersion: expectedVersion},
	})
	if err != nil {
		return 0, err
	}
	if resp.ErrCode != processor.ErrOK {
		return 0, codeToErr(resp.ErrCode)
	}
	return resp.SetDataResp.NewVersion, nil
}

// Get reads data.
func (c *Client) Get(path string, watch bool) ([]byte, tree.Stat, error) {
	resp, err := c.call(&proto.Envelope{
		GetDataReq: &proto.GetDataReq{Path: path, Watch: watch},
	})
	if err != nil {
		return nil, tree.Stat{}, err
	}
	if resp.ErrCode != processor.ErrOK {
		return nil, tree.Stat{}, codeToErr(resp.ErrCode)
	}
	return resp.GetDataResp.Data, resp.GetDataResp.Stat, nil
}

// Exists checks for existence.
func (c *Client) Exists(path string, watch bool) (bool, tree.Stat, error) {
	resp, err := c.call(&proto.Envelope{
		ExistsReq: &proto.ExistsReq{Path: path, Watch: watch},
	})
	if err != nil {
		return false, tree.Stat{}, err
	}
	return resp.ExistsResp.Exists, resp.ExistsResp.Stat, nil
}

// Children lists children.
func (c *Client) Children(path string, watch bool) ([]string, error) {
	resp, err := c.call(&proto.Envelope{
		GetChildrenReq: &proto.GetChildrenReq{Path: path, Watch: watch},
	})
	if err != nil {
		return nil, err
	}
	if resp.ErrCode != processor.ErrOK {
		return nil, codeToErr(resp.ErrCode)
	}
	return resp.GetChildrenResp.Children, nil
}

func codeToErr(c processor.ErrCode) error {
	switch c {
	case processor.ErrOK:
		return nil
	case processor.ErrNoNode:
		return errors.New("no such znode")
	case processor.ErrNodeExists:
		return errors.New("znode exists")
	case processor.ErrBadVersion:
		return errors.New("bad version")
	case processor.ErrNotEmpty:
		return errors.New("not empty")
	case processor.ErrNoParent:
		return errors.New("no parent")
	case processor.ErrEphemeralParent:
		return errors.New("ephemeral parent")
	case processor.ErrInvalidPath:
		return errors.New("invalid path")
	default:
		return errors.New("unknown server error")
	}
}

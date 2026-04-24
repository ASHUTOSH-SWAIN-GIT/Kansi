package server

import (
	"errors"
	"io"
	"log"
	"net"
	"sync"
	"time"

	"kansi/internal/processor"
	"kansi/internal/proto"
	"kansi/internal/session"
	"kansi/internal/tree"
	"kansi/internal/watch"
)

// Server accepts TCP connections and serves the kansi protocol.
type Server struct {
	store    *Store
	sessions *session.Manager
	watches  *watch.Manager

	mu      sync.Mutex
	outbox  map[tree.SessionID]chan<- *proto.Envelope // per-session delivery
	ln      net.Listener
	stopCh  chan struct{}
	stopped bool
}

// New constructs a Server over the given Store, session manager, and
// watch manager. The server does not start accepting until Serve.
func New(store *Store, sm *session.Manager, wm *watch.Manager) *Server {
	return &Server{
		store:    store,
		sessions: sm,
		watches:  wm,
		outbox:   make(map[tree.SessionID]chan<- *proto.Envelope),
		stopCh:   make(chan struct{}),
	}
}

// Serve listens on addr and blocks until Stop is called.
func (s *Server) Serve(addr string) error {
	ln, err := net.Listen("tcp", addr)
	if err != nil {
		return err
	}
	s.ln = ln

	go s.expirySweeper()

	for {
		c, err := ln.Accept()
		if err != nil {
			select {
			case <-s.stopCh:
				return nil
			default:
			}
			return err
		}
		go s.handleConn(c)
	}
}

// Addr returns the listener's address (for tests binding to :0).
func (s *Server) Addr() string {
	if s.ln == nil {
		return ""
	}
	return s.ln.Addr().String()
}

// Stop closes the listener and signals goroutines to exit.
func (s *Server) Stop() {
	s.mu.Lock()
	if s.stopped {
		s.mu.Unlock()
		return
	}
	s.stopped = true
	close(s.stopCh)
	s.mu.Unlock()
	if s.ln != nil {
		_ = s.ln.Close()
	}
}

// register maps a session to its outbox channel for watch delivery.
func (s *Server) register(sid tree.SessionID, out chan<- *proto.Envelope) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.outbox[sid] = out
}

func (s *Server) unregister(sid tree.SessionID) {
	s.mu.Lock()
	defer s.mu.Unlock()
	delete(s.outbox, sid)
}

// deliverEvents routes fired events to the owning session's outbox.
// If the session's conn is gone, events are silently dropped (paper
// semantics: server-local watches are lost on disconnect).
func (s *Server) deliverEvents(events []watch.Event) {
	s.mu.Lock()
	conns := make(map[tree.SessionID]chan<- *proto.Envelope, len(events))
	for _, e := range events {
		if out, ok := s.outbox[e.Session]; ok {
			conns[e.Session] = out
		}
	}
	s.mu.Unlock()

	for _, e := range events {
		out, ok := conns[e.Session]
		if !ok {
			continue
		}
		select {
		case out <- &proto.Envelope{Xid: proto.XidWatch, WatchEvent: &e.Event}:
		default:
			// outbox full; drop (learning-project tradeoff)
		}
	}
}

// expirySweeper periodically scans for timed-out sessions.
func (s *Server) expirySweeper() {
	t := time.NewTicker(200 * time.Millisecond)
	defer t.Stop()
	for {
		select {
		case <-s.stopCh:
			return
		case <-t.C:
			for _, ex := range s.sessions.Expire() {
				events := s.store.applyExpiredSession(ex.Paths)
				s.watches.RemoveSession(ex.ID)
				s.unregister(ex.ID)
				s.deliverEvents(events)
			}
		}
	}
}

// handleConn runs one client connection's lifecycle: handshake,
// request loop, cleanup.
func (s *Server) handleConn(c net.Conn) {
	defer func() { _ = c.Close() }()

	// handshake
	env, err := proto.Read(c)
	if err != nil || env.ConnectReq == nil {
		return
	}
	timeout := time.Duration(env.ConnectReq.TimeoutMS) * time.Millisecond
	if timeout <= 0 {
		timeout = 5 * time.Second
	}

	var sid tree.SessionID
	if env.ConnectReq.SessionID != 0 {
		// reattach
		if _, ok := s.sessions.Get(tree.SessionID(env.ConnectReq.SessionID)); ok {
			sid = tree.SessionID(env.ConnectReq.SessionID)
			_ = s.sessions.Renew(sid)
		}
	}
	if sid == 0 {
		id, err := s.sessions.Open(timeout)
		if err != nil {
			return
		}
		sid = id
	}

	out := make(chan *proto.Envelope, 64)
	s.register(sid, out)

	// write pump
	writeDone := make(chan struct{})
	go func() {
		defer close(writeDone)
		for env := range out {
			if err := proto.Write(c, env); err != nil {
				return
			}
		}
	}()

	// send ConnectResp
	out <- &proto.Envelope{
		Xid:  env.Xid,
		Zxid: s.store.CurrentZxid(),
		ConnectResp: &proto.ConnectResp{
			SessionID: int64(sid),
			TimeoutMS: int32(timeout / time.Millisecond),
		},
	}

	// request loop
	for {
		env, err := proto.Read(c)
		if err != nil {
			if !errors.Is(err, io.EOF) {
				log.Printf("kansi: conn read: %v", err)
			}
			break
		}
		s.dispatch(sid, out, env)
	}

	// cleanup
	close(out)
	<-writeDone

	// DO NOT expire the session here — client may reconnect with the
	// same session id before timeout. The sweeper will reap it if not.
	s.unregister(sid)
}

// dispatch handles one request envelope, writing 0+ replies to out.
func (s *Server) dispatch(sid tree.SessionID, out chan<- *proto.Envelope, env *proto.Envelope) {
	resp := &proto.Envelope{Xid: env.Xid}
	_ = s.sessions.Renew(sid) // any request counts as activity

	switch {
	case env.PingReq != nil:
		resp.PingResp = &proto.PingResp{}
		resp.Zxid = s.store.CurrentZxid()

	case env.CreateReq != nil:
		r := env.CreateReq
		req := processor.CreateRequest{
			Path: r.Path, Data: r.Data,
			Owner: sid, Ephemeral: r.Ephemeral, Sequential: r.Sequential,
		}
		wr := s.store.applyWrite(sid, req)
		resp.Zxid = wr.Zxid
		if wr.ErrCode != processor.ErrOK {
			resp.ErrCode = wr.ErrCode
		} else {
			resp.CreateResp = &proto.CreateResp{Path: wr.ResolvedPath}
		}
		// deliver watch events asynchronously so the response isn't
		// blocked behind the outbox for this conn
		if len(wr.Events) > 0 {
			go s.deliverEvents(wr.Events)
		}

	case env.DeleteReq != nil:
		wr := s.store.applyWrite(sid, processor.DeleteRequest{
			Path: env.DeleteReq.Path, ExpectedVersion: env.DeleteReq.ExpectedVersion,
		})
		resp.Zxid = wr.Zxid
		if wr.ErrCode != processor.ErrOK {
			resp.ErrCode = wr.ErrCode
		} else {
			resp.DeleteResp = &proto.DeleteResp{}
		}
		if len(wr.Events) > 0 {
			go s.deliverEvents(wr.Events)
		}

	case env.SetDataReq != nil:
		wr := s.store.applyWrite(sid, processor.SetDataRequest{
			Path: env.SetDataReq.Path, Data: env.SetDataReq.Data,
			ExpectedVersion: env.SetDataReq.ExpectedVersion,
		})
		resp.Zxid = wr.Zxid
		if wr.ErrCode != processor.ErrOK {
			resp.ErrCode = wr.ErrCode
		} else {
			resp.SetDataResp = &proto.SetDataResp{NewVersion: wr.NewVersion}
		}
		if len(wr.Events) > 0 {
			go s.deliverEvents(wr.Events)
		}

	case env.GetDataReq != nil:
		r, code, zxid := s.store.readGetData(sid, env.GetDataReq.Path, env.GetDataReq.Watch)
		resp.Zxid = zxid
		if code != processor.ErrOK {
			resp.ErrCode = code
		} else {
			resp.GetDataResp = r
		}

	case env.ExistsReq != nil:
		r, zxid := s.store.readExists(sid, env.ExistsReq.Path, env.ExistsReq.Watch)
		resp.Zxid = zxid
		resp.ExistsResp = r

	case env.GetChildrenReq != nil:
		r, code, zxid := s.store.readGetChildren(sid, env.GetChildrenReq.Path, env.GetChildrenReq.Watch)
		resp.Zxid = zxid
		if code != processor.ErrOK {
			resp.ErrCode = code
		} else {
			resp.GetChildrenResp = r
		}

	default:
		resp.ErrCode = processor.ErrUnknown
	}

	select {
	case out <- resp:
	case <-s.stopCh:
	}
}

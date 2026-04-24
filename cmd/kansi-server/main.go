// Command kansi-server runs a standalone (Stage 4) kansi node.
//
// Usage:
//
//	kansi-server -addr :9000 -data ./data
package main

import (
	"flag"
	"log"
	"os"
	"path/filepath"

	"kansi/internal/recovery"
	"kansi/internal/server"
	"kansi/internal/session"
	"kansi/internal/txnlog"
	"kansi/internal/watch"
)

func main() {
	addr := flag.String("addr", ":9000", "listen address")
	dataDir := flag.String("data", "./data", "data directory (log + snapshot)")
	flag.Parse()

	if err := os.MkdirAll(*dataDir, 0o755); err != nil {
		log.Fatalf("mkdir data: %v", err)
	}
	logPath := filepath.Join(*dataDir, "txn.log")
	snapPath := filepath.Join(*dataDir, "snap")

	res, err := recovery.Recover(snapPath, logPath)
	if err != nil {
		log.Fatalf("recover: %v", err)
	}
	log.Printf("recovered: lastZxid=%d", res.LastZxid)

	w, err := txnlog.Create(logPath)
	if err != nil {
		log.Fatalf("open log: %v", err)
	}

	sm := session.NewManager(session.RealClock{})
	wm := watch.NewManager()
	store := server.NewStore(res.Tree, sm, wm, &server.LogCommitter{W: w}, uint64(res.LastZxid))
	s := server.New(store, sm, wm)

	log.Printf("kansi-server listening on %s", *addr)
	serveErr := s.Serve(*addr)
	_ = w.Close()
	if serveErr != nil {
		log.Fatalf("serve: %v", serveErr)
	}
}

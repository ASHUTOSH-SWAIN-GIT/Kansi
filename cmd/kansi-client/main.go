// Command kansi-client is a tiny CLI for poking at a kansi server.
//
// Usage examples:
//
//	kansi-client -addr :9000 create /foo hello
//	kansi-client -addr :9000 get /foo
//	kansi-client -addr :9000 set /foo world
//	kansi-client -addr :9000 delete /foo
//	kansi-client -addr :9000 ls /
package main

import (
	"flag"
	"fmt"
	"os"
	"time"

	"kansi/internal/client"
)

func usage() {
	fmt.Fprintln(os.Stderr, `usage: kansi-client -addr <host:port> <cmd> [args]
  create <path> [data]
  delete <path>
  set    <path> <data>
  get    <path>
  exists <path>
  ls     <path>`)
	os.Exit(2)
}

func main() {
	addr := flag.String("addr", "127.0.0.1:9000", "server address")
	flag.Parse()
	args := flag.Args()
	if len(args) == 0 {
		usage()
	}

	c, err := client.Dial(*addr, 5*time.Second)
	if err != nil {
		fmt.Fprintln(os.Stderr, "dial:", err)
		os.Exit(1)
	}
	defer func() { _ = c.Close() }()

	switch args[0] {
	case "create":
		if len(args) < 2 {
			usage()
		}
		var data []byte
		if len(args) >= 3 {
			data = []byte(args[2])
		}
		p, err := c.Create(args[1], data, false, false)
		must(err)
		fmt.Println(p)

	case "delete":
		if len(args) < 2 {
			usage()
		}
		must(c.Delete(args[1], -1))

	case "set":
		if len(args) < 3 {
			usage()
		}
		v, err := c.Set(args[1], []byte(args[2]), -1)
		must(err)
		fmt.Println("version:", v)

	case "get":
		if len(args) < 2 {
			usage()
		}
		data, st, err := c.Get(args[1], false)
		must(err)
		fmt.Printf("data: %s\nversion: %d czxid: %d mzxid: %d\n",
			data, st.DataVersion, st.Czxid, st.Mzxid)

	case "exists":
		if len(args) < 2 {
			usage()
		}
		ok, _, err := c.Exists(args[1], false)
		must(err)
		fmt.Println(ok)

	case "ls":
		if len(args) < 2 {
			usage()
		}
		kids, err := c.Children(args[1], false)
		must(err)
		for _, k := range kids {
			fmt.Println(k)
		}

	default:
		usage()
	}
}

func must(err error) {
	if err != nil {
		fmt.Fprintln(os.Stderr, "error:", err)
		os.Exit(1)
	}
}

// Package txnlog implements an append-only write-ahead log of committed
// transactions. Each record is length-prefixed and CRC32-checksummed so
// a torn tail write (crash mid-append) can be detected and skipped on
// recovery without losing earlier records.
//
// Record layout on disk:
//
//	+---------+---------+----------------+
//	| len u32 | crc u32 | gob payload... |
//	+---------+---------+----------------+
//
// `crc` is IEEE CRC32 over the gob payload bytes. `len` covers only the
// payload, not the header.
package txnlog

import (
	"bytes"
	"encoding/binary"
	"encoding/gob"
	"errors"
	"fmt"
	"hash/crc32"
	"io"
	"os"

	"kansi/internal/txn"
)

// ErrCorrupt indicates a checksum mismatch that is NOT at the tail of
// the file — i.e. mid-log corruption that the caller cannot safely
// ignore.
var ErrCorrupt = errors.New("txnlog: mid-log checksum mismatch")

const headerSize = 8 // 4 byte length + 4 byte crc

// Writer appends transactions to a log file. Each Append fsyncs before
// returning, so a successful Append is durable.
type Writer struct {
	f *os.File
}

// Create opens (or creates) the file at path for append. If the file
// already exists, new records are appended after the existing ones.
func Create(path string) (*Writer, error) {
	f, err := os.OpenFile(path, os.O_CREATE|os.O_WRONLY|os.O_APPEND, 0o644)
	if err != nil {
		return nil, err
	}
	return &Writer{f: f}, nil
}

// Append writes a single transaction and fsyncs. Per the paper, the log
// record is durable before the caller may ack the client.
func (w *Writer) Append(t *txn.Txn) error {
	var buf bytes.Buffer
	if err := gob.NewEncoder(&buf).Encode(t); err != nil {
		return fmt.Errorf("encode txn: %w", err)
	}
	payload := buf.Bytes()

	var header [headerSize]byte
	binary.LittleEndian.PutUint32(header[0:4], uint32(len(payload)))
	binary.LittleEndian.PutUint32(header[4:8], crc32.ChecksumIEEE(payload))

	if _, err := w.f.Write(header[:]); err != nil {
		return err
	}
	if _, err := w.f.Write(payload); err != nil {
		return err
	}
	return w.f.Sync()
}

// Close closes the underlying file.
func (w *Writer) Close() error { return w.f.Close() }

// Open opens a log for reading. Use Next to iterate records.
func Open(path string) (*Reader, error) {
	f, err := os.Open(path)
	if err != nil {
		return nil, err
	}
	return &Reader{f: f}, nil
}

// Reader iterates transactions from a log file.
type Reader struct {
	f      *os.File
	offset int64 // bytes consumed by successfully decoded records
}

// Next returns the next transaction in the log.
//
// Returns io.EOF at a clean end of file, or at a torn tail write
// (partial header, partial payload, or bad checksum at the final
// record). Mid-log corruption returns ErrCorrupt.
func (r *Reader) Next() (*txn.Txn, error) {
	var header [headerSize]byte
	n, err := io.ReadFull(r.f, header[:])
	if err == io.EOF {
		return nil, io.EOF
	}
	if err == io.ErrUnexpectedEOF || (err == nil && n < headerSize) {
		// partial header at tail -> treat as torn write, end of log
		return nil, io.EOF
	}
	if err != nil {
		return nil, err
	}

	length := binary.LittleEndian.Uint32(header[0:4])
	wantCRC := binary.LittleEndian.Uint32(header[4:8])

	payload := make([]byte, length)
	if _, err := io.ReadFull(r.f, payload); err != nil {
		if err == io.EOF || err == io.ErrUnexpectedEOF {
			// partial payload at tail -> torn write
			return nil, io.EOF
		}
		return nil, err
	}

	if crc32.ChecksumIEEE(payload) != wantCRC {
		// Is this a tail record? Check if there's more data after us.
		// For simplicity we treat any CRC mismatch at the current
		// position as corrupt unless nothing follows — we peek.
		if r.atEOF() {
			return nil, io.EOF
		}
		return nil, ErrCorrupt
	}

	var t txn.Txn
	if err := gob.NewDecoder(bytes.NewReader(payload)).Decode(&t); err != nil {
		return nil, fmt.Errorf("decode txn: %w", err)
	}
	r.offset += int64(headerSize) + int64(length)
	return &t, nil
}

// atEOF returns true if the current file position is at the end.
func (r *Reader) atEOF() bool {
	cur, err := r.f.Seek(0, io.SeekCurrent)
	if err != nil {
		return false
	}
	end, err := r.f.Seek(0, io.SeekEnd)
	if err != nil {
		return false
	}
	// restore position
	_, _ = r.f.Seek(cur, io.SeekStart)
	return cur == end
}

// Close closes the underlying file.
func (r *Reader) Close() error { return r.f.Close() }

// ReadAll is a convenience that reads every txn from path. A torn tail
// is treated as a clean end of log; mid-log corruption surfaces as
// ErrCorrupt.
func ReadAll(path string) ([]*txn.Txn, error) {
	r, err := Open(path)
	if err != nil {
		return nil, err
	}
	defer func() { _ = r.Close() }()

	var txns []*txn.Txn
	for {
		t, err := r.Next()
		if err == io.EOF {
			return txns, nil
		}
		if err != nil {
			return txns, err
		}
		txns = append(txns, t)
	}
}

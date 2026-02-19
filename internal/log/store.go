package log

import (
	"bufio"
	"encoding/binary"
	"os"
	"sync"
)

var byteOrder = binary.BigEndian

const (
	headerWidth = 8
)

type store struct {
	*os.File
	mu   sync.Mutex
	buf  *bufio.Writer
	size uint64
}

func newStore(f *os.File) (*store, error) {
	fi, err := os.Stat(f.Name())
	if err != nil {
		return nil, err
	}

	return &store{
		File: f,
		size: uint64(fi.Size()),
		buf:  bufio.NewWriter(f),
	}, nil
}

func (s *store) Append(p []byte) (n uint64, pos uint64, err error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	pos = s.size
	if err := binary.Write(s.buf, byteOrder, uint64(len(p))); err != nil {
		return 0, 0, err
	}
	writtenBytes, err := s.buf.Write(p)
	if err != nil {
		return 0, 0, err
	}

	writtenBytes += headerWidth
	s.size += uint64(writtenBytes)
	return uint64(writtenBytes), pos, nil
}

func (s *store) Read(pos uint64) ([]byte, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if err := s.buf.Flush(); err != nil {
		return nil, err
	}
	headerBuf := make([]byte, headerWidth)
	if _, err := s.File.ReadAt(headerBuf, int64(pos)); err != nil {
		return nil, err
	}
	lenToRead := byteOrder.Uint64(headerBuf)
	valueBuf := make([]byte, lenToRead)
	if _, err := s.File.ReadAt(valueBuf, int64(pos+headerWidth)); err != nil {
		return nil, err
	}
	return valueBuf, nil
}

func (s *store) ReadAt(p []byte, off int64) (int, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if err := s.buf.Flush(); err != nil {
		return 0, err
	}
	return s.File.ReadAt(p, off)
}

func (s *store) Close() error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if err := s.buf.Flush(); err != nil {
		return err
	}
	return s.File.Close()
}

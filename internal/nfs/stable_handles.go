package nfs

import (
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"sync"

	"github.com/go-git/go-billy/v5"
)

// stableHandles provides deterministic NFS file handles that survive
// server restarts. Each handle is SHA-256(namespaceUUID + "/" + relPath),
// so as long as the file exists at the same path the handle is identical
// across reboots. This eliminates ESTALE errors for clients that cache
// handles — the same approach used by NFS-Ganesha's HandleMap.
//
// The namespace UUID is generated once and stored at
// <root>/.kiwi/nfs-namespace. It provides domain separation so two
// different KiwiFS instances never produce colliding handles.
type stableHandles struct {
	namespace string
	mu        sync.RWMutex
	byHandle  map[string]entry // hex(handle) → entry
	byPath    map[string]string // joinedPath → hex(handle)
}

type entry struct {
	f billy.Filesystem
	p []string
}

func newStableHandles(root string) *stableHandles {
	ns := loadOrCreateNamespace(root)
	return &stableHandles{
		namespace: ns,
		byHandle:  make(map[string]entry),
		byPath:    make(map[string]string),
	}
}

func loadOrCreateNamespace(root string) string {
	dir := filepath.Join(root, ".kiwi")
	os.MkdirAll(dir, 0755)
	p := filepath.Join(dir, "nfs-namespace")
	data, err := os.ReadFile(p)
	if err == nil && len(data) >= 32 {
		return strings.TrimSpace(string(data))
	}
	h := sha256.Sum256([]byte(fmt.Sprintf("%s-%d", root, os.Getpid())))
	ns := hex.EncodeToString(h[:16])
	os.WriteFile(p, []byte(ns+"\n"), 0644)
	return ns
}

func (s *stableHandles) deriveHandle(path string) []byte {
	h := sha256.New()
	h.Write([]byte(s.namespace))
	h.Write([]byte{0})
	h.Write([]byte(path))
	return h.Sum(nil)[:32]
}

func (s *stableHandles) ToHandle(f billy.Filesystem, path []string) []byte {
	joined := f.Join(path...)
	handle := s.deriveHandle(joined)
	hexH := hex.EncodeToString(handle)

	s.mu.Lock()
	defer s.mu.Unlock()

	newPath := make([]string, len(path))
	copy(newPath, path)
	s.byHandle[hexH] = entry{f, newPath}
	s.byPath[joined] = hexH

	return handle
}

func (s *stableHandles) FromHandle(b []byte) (billy.Filesystem, []string, error) {
	hexH := hex.EncodeToString(b)

	s.mu.RLock()
	e, ok := s.byHandle[hexH]
	s.mu.RUnlock()

	if !ok {
		return nil, nil, fmt.Errorf("stale handle")
	}
	return e.f, e.p, nil
}

func (s *stableHandles) InvalidateHandle(f billy.Filesystem, b []byte) error {
	hexH := hex.EncodeToString(b)

	s.mu.Lock()
	defer s.mu.Unlock()

	if e, ok := s.byHandle[hexH]; ok {
		joined := f.Join(e.p...)
		delete(s.byPath, joined)
		delete(s.byHandle, hexH)
	}
	return nil
}

func (s *stableHandles) HandleLimit() int {
	return 0 // unlimited
}

package vectorstore

import (
	"fmt"
	"unicode/utf8"
)

const (
	// DefaultTopK is the number of nearest-neighbour results returned when
	// the caller doesn't specify a topK value.
	DefaultTopK       = 10
	defaultSnippetLen = 240
)

// deterministicUUID produces a stable UUID-shaped string from any input so
// vector stores (Qdrant, Weaviate) that require UUID-formatted IDs can
// accept it. Not a real UUIDv5 — just an FNV-64a hash formatted to fit the
// 8-4-4-4-12 layout.
func deterministicUUID(s string) string {
	h := fnv64a(s)
	return fmt.Sprintf("%08x-%04x-5%03x-8%03x-%012x",
		uint32(h>>32), uint16(h>>16), uint16(h>>48)&0xfff, uint16(h>>4)&0xfff, h&0xffffffffffff)
}

func fnv64a(s string) uint64 {
	const (
		offset = 14695981039346656037
		prime  = 1099511628211
	)
	h := uint64(offset)
	for i := 0; i < len(s); i++ {
		h ^= uint64(s[i])
		h *= prime
	}
	return h
}

// truncate caps a string at n bytes for log/error output.
func truncate(s string, n int) string {
	if len(s) <= n {
		return s
	}
	return s[:n] + "…"
}

// snippet truncates text to approximately n bytes without splitting a
// multi-byte UTF-8 character.
func snippet(text string, n int) string {
	if len(text) <= n {
		return text
	}
	end := 0
	for i, r := range text {
		next := i + utf8.RuneLen(r)
		if next > n {
			break
		}
		end = next
	}
	if end == 0 {
		return "…"
	}
	return text[:end] + "…"
}

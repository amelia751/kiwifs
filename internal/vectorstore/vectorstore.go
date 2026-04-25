// Package vectorstore defines the persistence layer for semantic search and
// ships a pure-Go SQLite-backed implementation. It also hosts the Service
// type that glues a vector Store to an embed.Embedder — chunking text,
// driving the async index queue, and answering semantic search queries.
package vectorstore

import "context"

// Chunk is one indexable unit: a contiguous slice of a document with its
// embedding vector. Documents larger than the chunk size are split into
// multiple chunks; each chunk is stored and searched independently.
type Chunk struct {
	// ID is unique across the store: "<path>#<chunkIdx>".
	ID string
	// Path is the file path relative to the knowledge root.
	Path string
	// ChunkIdx is the 0-based position of this chunk within Path.
	ChunkIdx int
	// Text is the raw chunk content (returned back in search results as a
	// preview / snippet).
	Text string
	// Vector is the embedding. Length == Embedder.Dimensions().
	Vector []float32
}

// Result is a single semantic search hit.
type Result struct {
	Path     string  `json:"path"`
	ChunkIdx int     `json:"chunkIdx"`
	Score    float64 `json:"score"`
	Snippet  string  `json:"snippet"`
}

// Store is the narrow persistence contract. Upsert / RemoveByPath / Search /
// Reset — every provider (sqlite, qdrant, pgvector, pinecone, weaviate,
// milvus) can satisfy this with a modest amount of code.
type Store interface {
	Upsert(ctx context.Context, chunks []Chunk) error
	RemoveByPath(ctx context.Context, path string) error
	Search(ctx context.Context, vector []float32, topK int) ([]Result, error)
	GetVectors(ctx context.Context, path string) ([]Chunk, error)
	Reset(ctx context.Context) error
	Count(ctx context.Context) (int, error)
	Close() error
}

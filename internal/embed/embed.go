// Package embed defines the Embedder interface and ships three built-in
// implementations: OpenAI, Ollama, and a generic HTTP escape hatch. Any
// implementation can be paired with any vector store via config.
package embed

import "context"

// Embedder converts text into fixed-width float32 vectors. Implementations
// are expected to batch their input for throughput; callers should always
// pass the full batch they have.
type Embedder interface {
	// Embed produces one vector per input string, in the same order.
	Embed(ctx context.Context, texts []string) ([][]float32, error)
	// Dimensions reports the vector width. The vector store needs this at
	// init to size its storage. Zero is allowed only when the embedder
	// learns it lazily from the first call.
	Dimensions() int
}

// truncate is shared by the HTTP-backed embedders to bound error-response
// bodies in log/error output.
func truncate(s string, n int) string {
	if len(s) <= n {
		return s
	}
	return s[:n] + "…"
}

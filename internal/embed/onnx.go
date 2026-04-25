package embed

import (
	"context"
	"fmt"
	"os"
	"sync"
)

// ONNX is an in-process embedder backed by an ONNX model file on disk.
//
// Pure-Go ONNX inference is not yet production-ready (the community
// packages all require CGO bindings to onnxruntime). To keep the KiwiFS
// binary CGO-free we ship the ONNX provider as a thin dispatcher: the
// model path is validated at construction time, and runtime inference is
// delegated to an adapter the operator plugs in before serving.
//
// Typical deployment (pure-Go, no CGO):
//   - Run a tiny sidecar that loads the .onnx file and exposes /embed over
//     HTTP on localhost. Configure KiwiFS with provider = "http" and url =
//     "http://127.0.0.1:PORT/embed". That keeps the "everything local,
//     fully offline" guarantee without dragging native code into kiwifs.
//
// Typical deployment (CGO build):
//   - Build with `-tags onnx` and link against onnxruntime. The build-
//     tagged variant (not included in the default binary) satisfies
//     InferenceFn and the provider runs entirely in-process.
//
// SetInferenceFn swaps in an implementation; main.go may register one at
// init time if compiled with the appropriate tag. The default error makes
// the limitation discoverable instead of silently degrading.
type ONNX struct {
	modelPath string
	dims      int

	mu sync.Mutex
	fn InferenceFn
}

// InferenceFn is the pluggable inference callback. Implementations produce
// one vector per input string, preserving order, each of length Dimensions.
type InferenceFn func(ctx context.Context, texts []string) ([][]float32, error)

// onnxRegistry holds a global inference function set at init time by
// build-tagged variants (e.g. onnx_cgo.go) that ship with ONNX runtime
// linked in. The default build leaves this nil and returns a helpful
// error from Embed().
var (
	onnxRegMu sync.Mutex
	onnxReg   InferenceFn
)

// NewONNX constructs an ONNX embedder. modelPath must exist on disk; dims
// is required because callers (vector store config) need it before the
// first Embed() call.
func NewONNX(modelPath string, dims int) (*ONNX, error) {
	if modelPath == "" {
		return nil, fmt.Errorf("onnx: model path is required (set search.vector.embedder.base_url to the .onnx file)")
	}
	if _, err := os.Stat(modelPath); err != nil {
		return nil, fmt.Errorf("onnx: model not found at %s: %w", modelPath, err)
	}
	if dims <= 0 {
		// A sensible default for the spec's all-MiniLM-L6-v2 recommendation.
		dims = 384
	}
	o := &ONNX{modelPath: modelPath, dims: dims}
	onnxRegMu.Lock()
	o.fn = onnxReg
	onnxRegMu.Unlock()
	return o, nil
}

// SetInferenceFn overrides the global inference function for this
// embedder instance — useful in tests.
func (o *ONNX) SetInferenceFn(fn InferenceFn) {
	o.mu.Lock()
	defer o.mu.Unlock()
	o.fn = fn
}

func (o *ONNX) Embed(ctx context.Context, texts []string) ([][]float32, error) {
	o.mu.Lock()
	fn := o.fn
	o.mu.Unlock()
	if fn == nil {
		return nil, fmt.Errorf("onnx: no inference backend registered in this build — either use provider=\"ollama\" for offline embeddings, or run a local onnxruntime-server sidecar and set provider=\"http\"")
	}
	return fn(ctx, texts)
}

func (o *ONNX) Dimensions() int { return o.dims }

// ModelPath exposes the configured .onnx file so build-tagged inference
// registrations can pick it up.
func (o *ONNX) ModelPath() string { return o.modelPath }

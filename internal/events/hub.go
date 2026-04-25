package events

import (
	"encoding/json"
	"errors"
	"sync"
)

// DefaultMaxClients is the per-hub cap for concurrent SSE subscribers. A
// modest upper bound prevents a client from hoarding goroutines + buffered
// channels without an upstream rate-limit. Callers that need a different
// ceiling can set Hub.MaxClients after NewHub.
const DefaultMaxClients = 256

// ErrTooManySubscribers is returned by Subscribe when the hub is at capacity.
// Handlers translate this to HTTP 503.
var ErrTooManySubscribers = errors.New("sse: too many subscribers")

// Event is broadcast to all connected SSE clients.
type Event struct {
	Op    string   `json:"op"`              // "write", "delete", "bulk", "comment.add", "janitor", ...
	Path  string   `json:"path,omitempty"`  // single-file ops
	Paths []string `json:"paths,omitempty"` // bulk ops
	Actor string   `json:"actor,omitempty"`
	ETag  string   `json:"etag,omitempty"`
	// Extra carries event-type-specific payload (janitor scan summary,
	// presence viewer list, etc.). Keep to small primitive maps — SSE
	// consumers don't want to decode a 10 MB janitor issue list over
	// the wire; that comes through the dedicated REST endpoint.
	Extra map[string]any `json:"extra,omitempty"`
}

// Message is what each SSE writer actually pushes to the wire. Carrying the
// op alongside the JSON payload lets the HTTP handler emit a proper
// `event: <op>` line so clients can `addEventListener('write', ...)` rather
// than dispatching on `data.op` themselves.
type Message struct {
	Op   string
	Data []byte
}

// Hub manages a set of SSE client channels. Goroutine-safe.
type Hub struct {
	// MaxClients caps concurrent subscribers. Zero uses DefaultMaxClients.
	MaxClients int

	mu      sync.RWMutex
	clients map[chan Message]struct{}
}

func NewHub() *Hub {
	return &Hub{
		MaxClients: DefaultMaxClients,
		clients:    make(map[chan Message]struct{}),
	}
}

// Subscribe registers a new client channel. Returns ErrTooManySubscribers
// when the hub is already at MaxClients. The caller must call Unsubscribe
// when the SSE connection closes.
func (h *Hub) Subscribe() (chan Message, error) {
	h.mu.Lock()
	defer h.mu.Unlock()
	max := h.MaxClients
	if max <= 0 {
		max = DefaultMaxClients
	}
	if len(h.clients) >= max {
		return nil, ErrTooManySubscribers
	}
	ch := make(chan Message, 16)
	h.clients[ch] = struct{}{}
	return ch, nil
}

func (h *Hub) Unsubscribe(ch chan Message) {
	h.mu.Lock()
	_, ok := h.clients[ch]
	if ok {
		delete(h.clients, ch)
	}
	h.mu.Unlock()
	if ok {
		close(ch)
	}
}

// Count reports the current subscriber count. Useful for /health and tests.
func (h *Hub) Count() int {
	h.mu.RLock()
	defer h.mu.RUnlock()
	return len(h.clients)
}

// Broadcast sends an event to every connected client. Non-blocking: if a
// client's buffer is full the event is dropped for that client rather than
// stalling the writer.
func (h *Hub) Broadcast(ev Event) {
	if ev.Op == "" {
		ev.Op = "message"
	}
	data, err := json.Marshal(ev)
	if err != nil {
		return
	}
	msg := Message{Op: ev.Op, Data: data}
	h.mu.RLock()
	defer h.mu.RUnlock()
	for ch := range h.clients {
		select {
		case ch <- msg:
		default:
		}
	}
}

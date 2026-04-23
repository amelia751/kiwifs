package events

import (
	"encoding/json"
	"errors"
	"testing"
)

func TestBroadcastCarriesOpForSSEEventField(t *testing.T) {
	h := NewHub()
	ch, err := h.Subscribe()
	if err != nil {
		t.Fatalf("subscribe: %v", err)
	}
	defer h.Unsubscribe(ch)

	h.Broadcast(Event{Op: "write", Path: "x.md", Actor: "tester"})
	select {
	case msg := <-ch:
		if msg.Op != "write" {
			t.Fatalf("op mismatch: %s", msg.Op)
		}
		var parsed Event
		if err := json.Unmarshal(msg.Data, &parsed); err != nil {
			t.Fatalf("json: %v", err)
		}
		if parsed.Path != "x.md" {
			t.Fatalf("path mismatch: %s", parsed.Path)
		}
	default:
		t.Fatalf("no message")
	}
}

func TestSubscribeRejectsOverCap(t *testing.T) {
	h := NewHub()
	h.MaxClients = 2
	a, err := h.Subscribe()
	if err != nil {
		t.Fatalf("first: %v", err)
	}
	b, err := h.Subscribe()
	if err != nil {
		t.Fatalf("second: %v", err)
	}
	_, err = h.Subscribe()
	if !errors.Is(err, ErrTooManySubscribers) {
		t.Fatalf("want cap error, got %v", err)
	}
	h.Unsubscribe(a)
	h.Unsubscribe(b)
}

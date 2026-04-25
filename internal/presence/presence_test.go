package presence

import (
	"testing"
	"time"
)

func TestHeartbeatAndList(t *testing.T) {
	tr := New(100 * time.Millisecond)
	tr.Heartbeat("pages/a.md", "alice", RoleEditor)
	tr.Heartbeat("pages/a.md", "bob", RoleViewer)
	live := tr.List("pages/a.md")
	if len(live) != 2 {
		t.Fatalf("want 2 live entries, got %d (%+v)", len(live), live)
	}
	// Editors must sort first so the UI can treat live[0] as "the
	// active editor, if any" without re-sorting.
	if live[0].Role != RoleEditor || live[0].Actor != "alice" {
		t.Fatalf("expected alice (editor) first, got %+v", live[0])
	}
}

func TestExpireAfterTTL(t *testing.T) {
	tr := New(50 * time.Millisecond)
	tr.Heartbeat("pages/b.md", "alice", RoleEditor)
	time.Sleep(80 * time.Millisecond)
	if live := tr.List("pages/b.md"); len(live) != 0 {
		t.Fatalf("expected expiration; got %+v", live)
	}
}

func TestSweepReportsChangedPages(t *testing.T) {
	tr := New(40 * time.Millisecond)
	tr.Heartbeat("a", "x", RoleViewer)
	tr.Heartbeat("b", "y", RoleViewer)
	time.Sleep(60 * time.Millisecond)
	changed := tr.Sweep()
	if len(changed) != 2 {
		t.Fatalf("expected 2 changed pages, got %v", changed)
	}
}

func TestLeaveRemovesActor(t *testing.T) {
	tr := New(time.Second)
	tr.Heartbeat("c", "a", RoleEditor)
	tr.Heartbeat("c", "b", RoleViewer)
	tr.Leave("c", "a")
	live := tr.List("c")
	if len(live) != 1 || live[0].Actor != "b" {
		t.Fatalf("expected only b remaining, got %+v", live)
	}
}

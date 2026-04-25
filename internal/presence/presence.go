// Package presence tracks which actors are currently viewing or editing
// a given page. Clients heartbeat every few seconds; entries that haven't
// been refreshed within presenceTTL are considered stale and dropped.
//
// The tracker is intentionally in-memory only — presence is ephemeral by
// nature and a crash that wipes the map is fine (clients will re-announce
// on reconnect). A single mutex is enough for the low traffic presence
// generates even on 100-page workspaces.
package presence

import (
	"sort"
	"sync"
	"time"
)

// DefaultTTL is how long an entry survives without a heartbeat before
// being garbage-collected. Clients should heartbeat every TTL/2 to stay
// visible through transient network hiccups.
const DefaultTTL = 30 * time.Second

// Role distinguishes passive viewers from active editors. Editors get
// an exclusive "someone is editing" banner on other clients; viewers
// just show as avatars in the corner.
type Role string

const (
	RoleViewer Role = "viewer"
	RoleEditor Role = "editor"
)

// Entry is a single {actor, role} presence record for a page.
type Entry struct {
	Actor string    `json:"actor"`
	Role  Role      `json:"role"`
	Since time.Time `json:"since"`
	Seen  time.Time `json:"seen"`
}

// Tracker keeps per-path presence. Safe for concurrent use.
type Tracker struct {
	ttl time.Duration
	mu  sync.RWMutex
	// pages[path][actor] = Entry. An actor can only have one role per
	// page — if they heartbeat as editor after being a viewer, they
	// upgrade in place.
	pages map[string]map[string]Entry
}

// New returns a tracker with the given entry TTL. A zero ttl uses
// DefaultTTL.
func New(ttl time.Duration) *Tracker {
	if ttl <= 0 {
		ttl = DefaultTTL
	}
	return &Tracker{
		ttl:   ttl,
		pages: make(map[string]map[string]Entry),
	}
}

// Heartbeat records that actor is present on path with the given role.
// Returns the updated list of live entries for the page so callers can
// immediately broadcast a presence SSE event without a second List call.
func (t *Tracker) Heartbeat(path, actor string, role Role) []Entry {
	if path == "" || actor == "" {
		return nil
	}
	if role != RoleEditor {
		role = RoleViewer
	}
	now := time.Now()
	t.mu.Lock()
	p, ok := t.pages[path]
	if !ok {
		p = make(map[string]Entry)
		t.pages[path] = p
	}
	existing, had := p[actor]
	since := now
	if had {
		since = existing.Since
	}
	p[actor] = Entry{Actor: actor, Role: role, Since: since, Seen: now}
	// GC this path's stale entries on the heartbeat path — cheap because
	// page-level maps stay tiny even for popular pages.
	live := collectLiveLocked(p, now, t.ttl)
	t.mu.Unlock()
	return live
}

// Leave removes an actor from a page immediately (on explicit client
// disconnect) and returns the updated live list.
func (t *Tracker) Leave(path, actor string) []Entry {
	if path == "" || actor == "" {
		return nil
	}
	t.mu.Lock()
	p, ok := t.pages[path]
	if !ok {
		t.mu.Unlock()
		return nil
	}
	delete(p, actor)
	if len(p) == 0 {
		delete(t.pages, path)
	}
	live := collectLiveLocked(t.pages[path], time.Now(), t.ttl)
	t.mu.Unlock()
	return live
}

// List returns the currently-live entries for a page, filtered of any
// stale records.
func (t *Tracker) List(path string) []Entry {
	if path == "" {
		return nil
	}
	t.mu.RLock()
	p, ok := t.pages[path]
	if !ok {
		t.mu.RUnlock()
		return nil
	}
	live := collectLiveLocked(p, time.Now(), t.ttl)
	t.mu.RUnlock()
	return live
}

// Snapshot returns live entries for every page that currently has any
// presence. The returned map is a copy; callers may safely iterate.
func (t *Tracker) Snapshot() map[string][]Entry {
	t.mu.RLock()
	defer t.mu.RUnlock()
	now := time.Now()
	out := make(map[string][]Entry, len(t.pages))
	for path, p := range t.pages {
		live := collectLiveLocked(p, now, t.ttl)
		if len(live) > 0 {
			out[path] = live
		}
	}
	return out
}

// Sweep drops all entries whose Seen timestamp is older than TTL and
// returns the list of pages whose live state changed, so callers can
// broadcast updated presence events for them.
func (t *Tracker) Sweep() []string {
	now := time.Now()
	t.mu.Lock()
	defer t.mu.Unlock()
	var changed []string
	for path, p := range t.pages {
		before := len(p)
		for actor, e := range p {
			if now.Sub(e.Seen) > t.ttl {
				delete(p, actor)
			}
		}
		if len(p) != before {
			changed = append(changed, path)
		}
		if len(p) == 0 {
			delete(t.pages, path)
		}
	}
	sort.Strings(changed)
	return changed
}

// collectLiveLocked returns the non-stale entries from a page map. The
// caller must hold t.mu (read or write). Entries are returned sorted by
// (role, actor) for deterministic output — UI avatar lists flicker less
// when the backend sends a stable order.
func collectLiveLocked(p map[string]Entry, now time.Time, ttl time.Duration) []Entry {
	if len(p) == 0 {
		return nil
	}
	live := make([]Entry, 0, len(p))
	for _, e := range p {
		if now.Sub(e.Seen) <= ttl {
			live = append(live, e)
		}
	}
	sort.Slice(live, func(i, j int) bool {
		if live[i].Role != live[j].Role {
			// Editors first so the UI can pull the active editor off
			// the head of the slice without iterating.
			return live[i].Role == RoleEditor
		}
		return live[i].Actor < live[j].Actor
	})
	return live
}

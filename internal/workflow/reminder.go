package workflow

import (
	"context"
	"log"
	"math/rand"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"sync"
	"time"

	"github.com/kiwifs/kiwifs/internal/events"
)

// Reminder is an actionable reminder attached to a workflow page — a
// task past its due date, an approval still pending past its target,
// or a page as a whole whose due-date has passed. We flag each in the
// same structure so the UI "inbox" can render them uniformly.
type Reminder struct {
	Path      string    `json:"path"`
	Kind      string    `json:"kind"` // "task-overdue" | "page-overdue" | "approval-overdue"
	Actor     string    `json:"actor,omitempty"`
	TaskID    string    `json:"taskId,omitempty"`
	TaskTitle string    `json:"taskTitle,omitempty"`
	DueDate   string    `json:"dueDate,omitempty"`
	Severity  string    `json:"severity"` // "warning" | "error"
	Message   string    `json:"message"`
	CreatedAt time.Time `json:"createdAt"`
}

// ReminderSchedulerOptions controls the walk frequency.
type ReminderSchedulerOptions struct {
	// Interval between sweeps. Zero disables the loop entirely.
	Interval time.Duration
	// Jitter randomises scheduling so workspaces sharing a host don't
	// all spike at the same minute.
	Jitter time.Duration
	// WarnWithinDays: reminders fire this many days before the due date
	// so teams get a heads-up rather than only a "you missed it" ping.
	WarnWithinDays int
	// OverdueOnly drops the early-warning pings and only reports
	// reminders whose due date has already passed. Kept separate from
	// WarnWithinDays because "remind me 3 days before but also after"
	// is the common case.
	OverdueOnly bool
}

// ReminderScheduler periodically scans the workspace for overdue
// workflow items and broadcasts SSE "workflow.reminder" events. A
// cached Inbox slice lets the /workflow/reminders endpoint answer
// without a second walk.
type ReminderScheduler struct {
	root string
	hub  *events.Hub
	opts ReminderSchedulerOptions

	mu       sync.RWMutex
	inbox    []Reminder
	lastScan time.Time

	done chan struct{}
	once sync.Once
}

// NewReminderScheduler wires the walker up to the SSE hub.
func NewReminderScheduler(root string, hub *events.Hub, opts ReminderSchedulerOptions) *ReminderScheduler {
	if opts.WarnWithinDays <= 0 {
		opts.WarnWithinDays = 3
	}
	return &ReminderScheduler{
		root: root,
		hub:  hub,
		opts: opts,
		done: make(chan struct{}),
	}
}

// Start runs the sweep loop in a goroutine.
func (r *ReminderScheduler) Start(ctx context.Context) {
	if r == nil || r.opts.Interval <= 0 {
		return
	}
	go r.loop(ctx)
}

// Stop is safe to call multiple times.
func (r *ReminderScheduler) Stop() {
	r.once.Do(func() { close(r.done) })
}

// Inbox returns the latest cached reminder list.
func (r *ReminderScheduler) Inbox() []Reminder {
	r.mu.RLock()
	defer r.mu.RUnlock()
	out := make([]Reminder, len(r.inbox))
	copy(out, r.inbox)
	return out
}

// LastScan exposes the sweep timestamp so UI can render "updated 5m ago".
func (r *ReminderScheduler) LastScan() time.Time {
	r.mu.RLock()
	defer r.mu.RUnlock()
	return r.lastScan
}

func (r *ReminderScheduler) loop(ctx context.Context) {
	// Stagger the first sweep slightly so multiple schedulers sharing a
	// process don't all hit the disk simultaneously at boot.
	jitter := time.Duration(0)
	if r.opts.Jitter > 0 {
		jitter = time.Duration(rand.Int63n(int64(r.opts.Jitter)))
	}
	first := time.NewTimer(jitter)
	defer first.Stop()
	select {
	case <-ctx.Done():
		return
	case <-r.done:
		return
	case <-first.C:
	}
	r.runOnce(ctx)

	t := time.NewTicker(r.opts.Interval)
	defer t.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-r.done:
			return
		case <-t.C:
			r.runOnce(ctx)
		}
	}
}

func (r *ReminderScheduler) runOnce(ctx context.Context) {
	reminders, err := r.walk(ctx)
	if err != nil {
		log.Printf("workflow: reminder sweep: %v", err)
		return
	}
	r.mu.Lock()
	prev := r.inbox
	r.inbox = reminders
	r.lastScan = time.Now().UTC()
	r.mu.Unlock()

	added := diffByKey(reminders, prev)
	if r.hub == nil {
		return
	}
	for _, rem := range added {
		r.hub.Broadcast(events.Event{
			Op:   "workflow.reminder",
			Path: rem.Path,
			Extra: map[string]any{
				"kind":      rem.Kind,
				"taskId":    rem.TaskID,
				"taskTitle": rem.TaskTitle,
				"dueDate":   rem.DueDate,
				"severity":  rem.Severity,
				"message":   rem.Message,
				"actor":     rem.Actor,
				"createdAt": rem.CreatedAt.Format(time.RFC3339),
			},
		})
	}
	// Summary broadcast so the sidebar inbox badge can update on every
	// tick — UIs without an open reminder panel still get a hint that
	// "you have 3 overdue items on 2 pages".
	r.hub.Broadcast(events.Event{
		Op:   "workflow.reminders.summary",
		Path: "",
		Extra: map[string]any{
			"total":    len(reminders),
			"new":      len(added),
			"lastScan": r.lastScan.Format(time.RFC3339),
		},
	})
}

// walk scans every markdown file, parses its workflow frontmatter, and
// accumulates reminders for anything overdue or inside the early-warning
// window. Silently skips non-markdown, non-workflow, and unreadable
// files; the goal is surfaces, not validation.
func (r *ReminderScheduler) walk(ctx context.Context) ([]Reminder, error) {
	now := time.Now().UTC()
	warn := now.AddDate(0, 0, r.opts.WarnWithinDays)
	var out []Reminder

	err := filepath.WalkDir(r.root, func(path string, d os.DirEntry, werr error) error {
		if werr != nil || d.IsDir() {
			return nil
		}
		// Skip hidden dirs / .git / node_modules quickly — the same
		// heuristic the janitor uses.
		rel, rerr := filepath.Rel(r.root, path)
		if rerr != nil {
			return nil
		}
		if strings.HasPrefix(rel, ".") || strings.Contains(rel, "/.") {
			return nil
		}
		if !strings.HasSuffix(path, ".md") {
			return nil
		}
		select {
		case <-ctx.Done():
			return ctx.Err()
		default:
		}
		content, ferr := os.ReadFile(path)
		if ferr != nil {
			return nil
		}
		meta, perr := ParseWorkflow(content)
		if perr != nil || meta == nil {
			return nil
		}
		relPath := filepath.ToSlash(rel)
		out = append(out, remindersForPage(relPath, meta, now, warn)...)
		return nil
	})
	if err != nil {
		return nil, err
	}

	// Severity-first then path for deterministic ordering.
	sort.SliceStable(out, func(i, j int) bool {
		if out[i].Severity != out[j].Severity {
			return out[i].Severity == "error"
		}
		if out[i].Path != out[j].Path {
			return out[i].Path < out[j].Path
		}
		return out[i].TaskID < out[j].TaskID
	})
	return out, nil
}

// remindersForPage returns a slice of reminders for a single parsed
// workflow page. The OverdueOnly option (when true) suppresses the
// advance warnings — the caller still walks the same pages.
func remindersForPage(path string, meta *WorkflowMeta, now, warnCutoff time.Time) []Reminder {
	var out []Reminder

	if meta.DueDate != "" {
		if due, ok := parseDueDate(meta.DueDate); ok {
			if due.Before(now) && meta.Progress < 1.0 {
				out = append(out, Reminder{
					Path:      path,
					Kind:      "page-overdue",
					DueDate:   meta.DueDate,
					Severity:  "error",
					Message:   "Page is past its due date",
					CreatedAt: now,
				})
			} else if due.Before(warnCutoff) && meta.Progress < 1.0 {
				out = append(out, Reminder{
					Path:      path,
					Kind:      "page-overdue",
					DueDate:   meta.DueDate,
					Severity:  "warning",
					Message:   "Page is due soon",
					CreatedAt: now,
				})
			}
		}
	}

	for _, task := range meta.Tasks {
		if task.Status == TaskDone {
			continue
		}
		if task.DueDate == "" {
			continue
		}
		due, ok := parseDueDate(task.DueDate)
		if !ok {
			continue
		}
		var severity, msg string
		switch {
		case due.Before(now):
			severity = "error"
			msg = "Task is overdue"
		case due.Before(warnCutoff):
			severity = "warning"
			msg = "Task is due soon"
		default:
			continue
		}
		out = append(out, Reminder{
			Path:      path,
			Kind:      "task-overdue",
			Actor:     task.Assignee,
			TaskID:    task.ID,
			TaskTitle: task.Title,
			DueDate:   task.DueDate,
			Severity:  severity,
			Message:   msg,
			CreatedAt: now,
		})
	}

	if meta.Approval != nil && meta.Approval.Status == ApprovalPending && meta.DueDate != "" {
		if due, ok := parseDueDate(meta.DueDate); ok && due.Before(now) {
			out = append(out, Reminder{
				Path:      path,
				Kind:      "approval-overdue",
				Actor:     meta.Approval.Approver,
				DueDate:   meta.DueDate,
				Severity:  "error",
				Message:   "Approval still pending past due date",
				CreatedAt: now,
			})
		}
	}

	return out
}

// parseDueDate accepts YYYY-MM-DD or a full RFC3339 timestamp — both
// show up in real workspaces because different clients write frontmatter
// with different serialisers.
func parseDueDate(s string) (time.Time, bool) {
	for _, layout := range []string{"2006-01-02", time.RFC3339, time.DateTime} {
		if t, err := time.Parse(layout, s); err == nil {
			return t.UTC(), true
		}
	}
	return time.Time{}, false
}

// diffByKey returns reminders in "curr" whose (path, kind, taskId) are
// not in "prev". Stops SSE spamming the same ping every Interval.
func diffByKey(curr, prev []Reminder) []Reminder {
	seen := make(map[string]struct{}, len(prev))
	for _, p := range prev {
		seen[reminderKey(p)] = struct{}{}
	}
	var added []Reminder
	for _, c := range curr {
		if _, ok := seen[reminderKey(c)]; !ok {
			added = append(added, c)
		}
	}
	return added
}

func reminderKey(r Reminder) string {
	return r.Path + "|" + r.Kind + "|" + r.TaskID + "|" + r.Severity
}

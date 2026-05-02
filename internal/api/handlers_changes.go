package api

import (
	"fmt"
	"net/http"
	"os/exec"
	"strconv"
	"strings"
	"time"

	"github.com/labstack/echo/v4"
)

type changeEntry struct {
	Seq       string `json:"seq"`
	Path      string `json:"path"`
	Action    string `json:"action"`
	Actor     string `json:"actor"`
	Timestamp string `json:"timestamp"`
}

type changesResponse struct {
	Changes []changeEntry `json:"changes"`
	LastSeq string        `json:"last_seq"`
}

func (h *Handlers) Changes(c echo.Context) error {
	since := c.QueryParam("since")
	limit := parseIntParam(c, "limit", 50)
	if limit <= 0 {
		limit = 50
	}
	if limit > 500 {
		limit = 500
	}

	var args []string
	if since != "" {
		if !isHexHash(since) {
			return echo.NewHTTPError(http.StatusBadRequest, "unknown sequence")
		}
		args = []string{"log", "--format=%H|%an|%at|%s", fmt.Sprintf("%s..HEAD", since), fmt.Sprintf("-%d", limit)}
	} else {
		args = []string{"log", "--format=%H|%an|%at|%s", fmt.Sprintf("-%d", limit)}
	}

	ctx := c.Request().Context()
	cmd := exec.CommandContext(ctx, "git", args...)
	cmd.Dir = h.root
	out, err := cmd.Output()
	if err != nil {
		exitErr, ok := err.(*exec.ExitError)
		if ok && strings.Contains(string(exitErr.Stderr), "unknown revision") {
			return echo.NewHTTPError(http.StatusBadRequest, "unknown sequence")
		}
		if ok && strings.Contains(string(exitErr.Stderr), "does not have any commits") {
			return c.JSON(http.StatusOK, changesResponse{Changes: []changeEntry{}, LastSeq: ""})
		}
		return echo.NewHTTPError(http.StatusInternalServerError, fmt.Sprintf("git log: %v", err))
	}

	lines := strings.Split(strings.TrimSpace(string(out)), "\n")
	changes := make([]changeEntry, 0, len(lines))
	for _, line := range lines {
		if line == "" {
			continue
		}
		parts := strings.SplitN(line, "|", 4)
		if len(parts) < 4 {
			continue
		}
		hash, author, tsStr, subject := parts[0], parts[1], parts[2], parts[3]
		ts, _ := strconv.ParseInt(tsStr, 10, 64)
		action, path := parseCommitSubject(subject)
		changes = append(changes, changeEntry{
			Seq:       hash,
			Path:      path,
			Action:    action,
			Actor:     author,
			Timestamp: time.Unix(ts, 0).UTC().Format(time.RFC3339),
		})
	}

	lastSeq := ""
	if len(changes) > 0 {
		lastSeq = changes[0].Seq
	}

	return c.JSON(http.StatusOK, changesResponse{
		Changes: changes,
		LastSeq: lastSeq,
	})
}

func parseCommitSubject(subject string) (action, path string) {
	subject = strings.TrimSpace(subject)
	// KiwiFS commit messages follow the pattern "actor: action path" or "action path"
	if idx := strings.Index(subject, ": "); idx >= 0 {
		subject = subject[idx+2:]
	}
	subject = strings.TrimSpace(subject)
	parts := strings.SplitN(subject, " ", 2)
	if len(parts) == 2 {
		action = normalizeAction(parts[0])
		path = strings.TrimSpace(parts[1])
		// Handle "rename old → new" format
		if action == "rename" {
			if idx := strings.Index(path, " → "); idx >= 0 {
				path = strings.TrimSpace(path[idx+len(" → "):])
			}
		}
		if action == "bulk" {
			action = "write"
			path = ""
		}
		return action, path
	}
	return "write", subject
}

func normalizeAction(raw string) string {
	switch strings.ToLower(raw) {
	case "write", "create", "update":
		return "write"
	case "delete", "remove":
		return "delete"
	case "rename", "move":
		return "rename"
	case "bulk":
		return "bulk"
	default:
		return "write"
	}
}

func isHexHash(s string) bool {
	if len(s) < 4 || len(s) > 40 {
		return false
	}
	for _, c := range s {
		if !((c >= '0' && c <= '9') || (c >= 'a' && c <= 'f') || (c >= 'A' && c <= 'F')) {
			return false
		}
	}
	return true
}

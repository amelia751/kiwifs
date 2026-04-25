package dataview

import (
	"bytes"
	"context"
	"fmt"
	"strings"

	"github.com/kiwifs/kiwifs/internal/markdown"
	"github.com/kiwifs/kiwifs/internal/storage"
)

const viewMarker = "<!-- kiwi:auto -->"
const viewEndMarker = "<!-- /kiwi-view -->"

// ViewMarker returns the marker string used to delimit auto-generated content.
func ViewMarker() string { return viewMarker }

// RegenerateView reads a computed view file, runs its kiwi-query, renders the
// result as a markdown table, and writes it back only if the output changed.
func RegenerateView(ctx context.Context, store storage.Storage, exec *Executor, path string) (bool, error) {
	content, err := store.Read(ctx, path)
	if err != nil {
		return false, fmt.Errorf("read view %s: %w", path, err)
	}

	parsed, err := markdown.Parse(content)
	if err != nil {
		return false, fmt.Errorf("parse view %s: %w", path, err)
	}
	if parsed.Frontmatter == nil {
		return false, fmt.Errorf("view %s has no frontmatter", path)
	}

	viewFlag, _ := parsed.Frontmatter["kiwi-view"].(bool)
	if !viewFlag {
		return false, fmt.Errorf("%s is not a computed view (missing kiwi-view: true)", path)
	}

	query, _ := parsed.Frontmatter["kiwi-query"].(string)
	query = strings.TrimSpace(query)
	if query == "" {
		return false, fmt.Errorf("view %s has no kiwi-query in frontmatter", path)
	}

	result, err := exec.Query(ctx, query, 0, 0)
	if err != nil {
		return false, fmt.Errorf("query in view %s: %w", path, err)
	}

	format, _ := parsed.Frontmatter["kiwi-format"].(string)
	if format == "" {
		format = "table"
	}
	rendered := Render(result, format)

	// Rebuild the file: replace only between start and end markers
	s := string(content)
	markerIdx := strings.Index(s, viewMarker)

	var newContent string
	if markerIdx >= 0 {
		before := s[:markerIdx]
		after := ""
		rest := s[markerIdx+len(viewMarker):]
		if endIdx := strings.Index(rest, viewEndMarker); endIdx >= 0 {
			after = rest[endIdx+len(viewEndMarker):]
		}
		newContent = before + viewMarker + "\n" + rendered + "\n" + viewEndMarker + after
	} else {
		// No marker found — append after the closing ---
		_, body, splitErr := markdown.SplitFrontmatter(content)
		if splitErr != nil || body == nil {
			return false, fmt.Errorf("view %s: cannot locate frontmatter end", path)
		}
		fmEnd := len(s) - len(body)
		after := s[fmEnd:]
		newContent = s[:fmEnd] + "\n" + viewMarker + "\n" + rendered + "\n" + viewEndMarker + after
	}

	if bytes.Equal(content, []byte(newContent)) {
		return false, nil
	}

	if err := store.Write(ctx, path, []byte(newContent)); err != nil {
		return false, fmt.Errorf("write view %s: %w", path, err)
	}
	return true, nil
}



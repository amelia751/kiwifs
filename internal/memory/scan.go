package memory

import (
	"context"
	"fmt"
	"path/filepath"
	"strings"

	"github.com/kiwifs/kiwifs/internal/markdown"
	"github.com/kiwifs/kiwifs/internal/storage"
	"gopkg.in/yaml.v3"
)

// Report summarizes episodic and consolidation coverage for a knowledge root.
type Report struct {
	EpisodicCount int `json:"episodic_count"`
	// Cumulative merged-from entries seen across the tree (duplicates count).
	MergedFromRefs int `json:"merged_from_refs"`
	// Distinct ref keys: type:id, or type:path:relpath for path-only entries.
	// Omitted in JSON; use for debugging only.
	MergedKeySet  map[string]struct{} `json:"-"`
	Episodes      []EpisodicFile      `json:"episodic_files"`
	Unmerged      []EpisodicFile      `json:"unmerged"`
	Warnings      []string            `json:"warnings,omitempty"`
}

// EpisodicFile is one file classified as holding episodic memory.
type EpisodicFile struct {
	Path       string `json:"path"`
	EpisodeID  string `json:"episode_id,omitempty"`
	MemoryKind string `json:"memory_kind,omitempty"`
}

// Options configures Scan (path prefix and classification rules).
type Options struct {
	// EpisodesPathPrefix, if non-empty, is normalised to end with / and
	// compared against paths using ToSlash. When empty, DefaultEpisodesPathPrefix
	// is used for prefix-based heuristics.
	EpisodesPathPrefix string
}

// Scan walks the knowledge base, classifies episodic files, and determines
// which are not referenced by any merged-from list. Episodes are "covered" if
// a merged-from item has type "episode" and a matching id, or type "episode"
// and a path that matches the episode file path.
func Scan(ctx context.Context, store storage.Storage, opt Options) (*Report, error) {
	prefix := opt.EpisodesPathPrefix
	if prefix == "" {
		prefix = DefaultEpisodesPathPrefix
	}
	prefix = filepath.ToSlash(prefix)
	if prefix != "" && !strings.HasSuffix(prefix, "/") {
		prefix += "/"
	}

	rep := &Report{
		MergedKeySet: make(map[string]struct{}),
	}
	var err error
	err = storage.Walk(ctx, store, "/", func(e storage.Entry) error {
		if err := ctx.Err(); err != nil {
			return err
		}
		if !storage.IsKnowledgeFile(e.Path) {
			return nil
		}
		b, rerr := store.Read(ctx, e.Path)
		if rerr != nil {
			return rerr
		}
		return processFile(e.Path, b, prefix, rep)
	})
	if err != nil {
		return nil, err
	}
	return finishReport(rep), nil
}

func processFile(path string, b []byte, prefix string, rep *Report) error {
	fm, _ := markdown.Frontmatter(b)
	if fm == nil {
		fm = map[string]any{}
	}
	npath := filepath.ToSlash(path)
	mk, _ := fm["memory_kind"].(string)
	mk = strings.ToLower(strings.TrimSpace(mk))

	// Index merged-from from every file (any page may cite episodes).
	mergeList, w := extractMergedFrom(fm)
	rep.Warnings = append(rep.Warnings, w...)
	for _, e := range mergeList {
		rep.MergedFromRefs++
		typ, _ := e["type"].(string)
		typ = strings.TrimSpace(typ)
		if typ == "" {
			continue
		}
		id, _ := e["id"].(string)
		pth, _ := e["path"].(string)
		if strings.TrimSpace(id) == "" && strings.TrimSpace(pth) == "" {
			continue
		}
		ee := MergedFromEntry{Type: typ, ID: strings.TrimSpace(id), Path: strings.TrimSpace(pth)}
		rep.MergedKeySet[mergeKey(&ee)] = struct{}{}
		if pth != "" {
			pn := filepath.ToSlash(pth)
			rep.MergedKeySet["episode:path:"+pn] = struct{}{}
		}
	}

	if isEpisodic(npath, mk, prefix) {
		rep.EpisodicCount++
		ef, w := buildEpisodic(npath, fm)
		rep.Warnings = append(rep.Warnings, w...)
		rep.Episodes = append(rep.Episodes, ef)
	}
	return nil
}

func finishReport(r *Report) *Report {
	for _, e := range r.Episodes {
		if isEpisodicMerged(e, r.MergedKeySet) {
			continue
		}
		r.Unmerged = append(r.Unmerged, e)
	}
	return r
}

func isEpisodic(path, memoryKind, prefix string) bool {
	mk := strings.ToLower(strings.TrimSpace(memoryKind))
	if mk == KindSemantic || mk == KindConsolidation {
		return false
	}
	if mk == KindEpisodic {
		return true
	}
	if prefix != "" && strings.HasPrefix(path, prefix) {
		return true
	}
	return false
}

func buildEpisodic(path string, fm map[string]any) (EpisodicFile, []string) {
	var w []string
	id, _ := fm["episode_id"].(string)
	if strings.TrimSpace(id) == "" {
		if s, _ := fm["id"].(string); strings.TrimSpace(s) != "" {
			// only treat top-level `id` as the episode id when it looks intended
			id = strings.TrimSpace(s)
		}
	}
	if id == "" {
		w = append(w, "missing episode_id in "+path+" — set memory_kind: episodic and episode_id, or match merged-from by path")
	}
	mk, _ := fm["memory_kind"].(string)
	return EpisodicFile{Path: path, EpisodeID: id, MemoryKind: mk}, w
}

func isEpisodicMerged(e EpisodicFile, set map[string]struct{}) bool {
	id := strings.TrimSpace(e.EpisodeID)
	pn := filepath.ToSlash(strings.Trim(e.Path, "/"))
	if id != "" {
		if _, ok := set[mergeKey(&MergedFromEntry{Type: "episode", ID: id})]; ok {
			return true
		}
		// Producers may merge by run or session id instead of episode
		for _, typ := range []string{"run", "session", "trace", "event", "ingest"} {
			if _, ok := set[mergeKey(&MergedFromEntry{Type: typ, ID: id})]; ok {
				return true
			}
		}
	}
	if _, ok := set["episode:path:"+pn]; ok {
		return true
	}
	return false
}

func extractMergedFrom(fm map[string]any) ([]map[string]any, []string) {
	raw, ok := fm["merged-from"]
	if !ok {
		return nil, nil
	}
	out, err := normaliseMergedSequence(raw)
	if err != nil {
		return nil, []string{err.Error()}
	}
	return out, nil
}

func normaliseMergedSequence(raw any) ([]map[string]any, error) {
	if raw == nil {
		return nil, nil
	}
	switch t := raw.(type) {
	case []any:
		s := make([]map[string]any, 0, len(t))
		for _, e := range t {
			if m, ok := e.(map[string]any); ok {
				s = append(s, m)
			} else {
				// try yaml round-trip
				b, _ := yaml.Marshal(e)
				var m map[string]any
				if err := yaml.Unmarshal(b, &m); err == nil {
					s = append(s, m)
				}
			}
		}
		return s, nil
	case map[string]any:
		return []map[string]any{t}, nil
	default:
		return nil, fmt.Errorf("memory: merged-from has unsupported shape")
	}
}

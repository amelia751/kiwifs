package importer

import (
	"bytes"
	"context"
	"fmt"
	"os"
	"path/filepath"
	"regexp"
	"strings"

	"github.com/kiwifs/kiwifs/internal/markdown"
	"gopkg.in/yaml.v3"
)

var (
	obsEmbedRe   = regexp.MustCompile(`!\[\[([^\]]+)\]\]`)
	obsWikiRe    = regexp.MustCompile(`\[\[([^\]]+)\]\]`)
)

type ObsidianSource struct {
	vaultPath string
	files     []obsFile
	assets    []obsAsset
}

type obsFile struct {
	relPath string
	content []byte
	fm      map[string]any
}

type obsAsset struct {
	srcPath string
	relDest string
}

func NewObsidian(vaultPath string) (*ObsidianSource, error) {
	info, err := os.Stat(vaultPath)
	if err != nil {
		return nil, fmt.Errorf("vault path: %w", err)
	}
	if !info.IsDir() {
		return nil, fmt.Errorf("vault path is not a directory: %s", vaultPath)
	}

	s := &ObsidianSource{vaultPath: vaultPath}
	if err := s.walk(); err != nil {
		return nil, err
	}
	return s, nil
}

func (s *ObsidianSource) Name() string {
	return filepath.Base(s.vaultPath)
}

func (s *ObsidianSource) walk() error {
	attachDirs := map[string]bool{}
	// Common Obsidian attachment folder names.
	for _, d := range []string{"attachments", "assets", "images", "media", "files"} {
		attachDirs[d] = true
	}

	return filepath.Walk(s.vaultPath, func(path string, info os.FileInfo, err error) error {
		if err != nil {
			return err
		}

		rel, _ := filepath.Rel(s.vaultPath, path)

		if info.IsDir() {
			base := filepath.Base(path)
			if base == ".obsidian" || base == ".trash" {
				return filepath.SkipDir
			}
			return nil
		}

		if filepath.Ext(path) != ".md" {
			topDir := strings.SplitN(rel, string(filepath.Separator), 2)[0]
			if attachDirs[topDir] || isAttachmentFile(path) {
				s.assets = append(s.assets, obsAsset{
					srcPath: path,
					relDest: filepath.Join("assets", filepath.Base(path)),
				})
			}
			return nil
		}

		data, err := os.ReadFile(path)
		if err != nil {
			return fmt.Errorf("read %s: %w", rel, err)
		}

		fm, _ := markdown.Frontmatter(data)
		if fm == nil {
			fm = map[string]any{}
		}

		body := markdown.BodyAfterFrontmatter(data)
		body = rewriteObsidianSyntax(body)

		var buf bytes.Buffer
		if len(fm) > 0 {
			buf.WriteString("---\n")
			enc := yaml.NewEncoder(&buf)
			enc.SetIndent(2)
			enc.Encode(fm)
			enc.Close()
			buf.WriteString("---\n\n")
		}
		buf.WriteString(body)

		relMD := strings.TrimSuffix(rel, ".md")

		s.files = append(s.files, obsFile{
			relPath: relMD,
			content: buf.Bytes(),
			fm:      fm,
		})
		return nil
	})
}

func (s *ObsidianSource) Stream(ctx context.Context) (<-chan Record, <-chan error) {
	records := make(chan Record, 64)
	errs := make(chan error, 1)

	go func() {
		defer close(records)
		defer close(errs)

		name := s.Name()
		for i, f := range s.files {
			if ctx.Err() != nil {
				return
			}

			fields := make(map[string]any, len(f.fm)+1)
			for k, v := range f.fm {
				fields[k] = v
			}
			fields["_raw_content"] = string(f.content)

			pk := sanitizePath(f.relPath)

			rec := Record{
				SourceID:   fmt.Sprintf("obsidian:%s:%d", name, i),
				SourceDSN:  s.vaultPath,
				Table:      name,
				Fields:     fields,
				PrimaryKey: pk,
			}
			select {
			case records <- rec:
			case <-ctx.Done():
				return
			}
		}
	}()
	return records, errs
}

func (s *ObsidianSource) Close() error { return nil }

func rewriteObsidianSyntax(body string) string {
	// Rewrite embeds: ![[image.png]] → ![image](assets/image.png)
	body = obsEmbedRe.ReplaceAllStringFunc(body, func(match string) string {
		inner := obsEmbedRe.FindStringSubmatch(match)[1]
		parts := strings.SplitN(inner, "|", 2)
		file := strings.TrimSpace(parts[0])
		alt := strings.TrimSuffix(filepath.Base(file), filepath.Ext(file))
		if len(parts) > 1 {
			alt = strings.TrimSpace(parts[1])
		}
		return fmt.Sprintf("![%s](assets/%s)", alt, filepath.Base(file))
	})

	// Rewrite wiki-links: [[Page Name]] → [[page-name]], [[Page|Alias]] → [[page-name|Alias]]
	body = obsWikiRe.ReplaceAllStringFunc(body, func(match string) string {
		inner := obsWikiRe.FindStringSubmatch(match)[1]
		parts := strings.SplitN(inner, "|", 2)
		page := slugifyPage(strings.TrimSpace(parts[0]))
		if len(parts) > 1 {
			return fmt.Sprintf("[[%s|%s]]", page, strings.TrimSpace(parts[1]))
		}
		return fmt.Sprintf("[[%s]]", page)
	})

	return body
}

func slugifyPage(s string) string {
	s = strings.ToLower(s)
	s = strings.ReplaceAll(s, " ", "-")
	return s
}

func isAttachmentFile(path string) bool {
	ext := strings.ToLower(filepath.Ext(path))
	switch ext {
	case ".png", ".jpg", ".jpeg", ".gif", ".svg", ".webp", ".bmp",
		".pdf", ".mp3", ".mp4", ".wav", ".webm", ".ogg":
		return true
	}
	return false
}

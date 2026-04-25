package importer

import (
	"bytes"
	"context"
	"fmt"
	"os"
	"path/filepath"
	"strings"

	"golang.org/x/net/html"
)

type ConfluenceSource struct {
	exportPath string
	pages      []confluencePage
}

type confluencePage struct {
	relPath  string
	title    string
	markdown string
	meta     map[string]any
}

func NewConfluence(exportPath string) (*ConfluenceSource, error) {
	info, err := os.Stat(exportPath)
	if err != nil {
		return nil, fmt.Errorf("confluence export path: %w", err)
	}
	if !info.IsDir() {
		return nil, fmt.Errorf("confluence export path is not a directory: %s", exportPath)
	}

	s := &ConfluenceSource{exportPath: exportPath}
	if err := s.walk(); err != nil {
		return nil, err
	}
	return s, nil
}

func (s *ConfluenceSource) Name() string {
	return filepath.Base(s.exportPath)
}

func (s *ConfluenceSource) walk() error {
	return filepath.Walk(s.exportPath, func(path string, info os.FileInfo, err error) error {
		if err != nil {
			return err
		}
		if info.IsDir() {
			return nil
		}

		ext := strings.ToLower(filepath.Ext(path))
		if ext != ".html" && ext != ".htm" {
			return nil
		}

		data, readErr := os.ReadFile(path)
		if readErr != nil {
			return fmt.Errorf("read %s: %w", path, readErr)
		}

		doc, parseErr := html.Parse(bytes.NewReader(data))
		if parseErr != nil {
			return fmt.Errorf("parse %s: %w", path, parseErr)
		}

		meta := extractConfluenceMeta(doc)
		title := meta["title"]
		if t, ok := title.(string); ok && t == "" {
			title = strings.TrimSuffix(filepath.Base(path), ext)
			meta["title"] = title
		} else if title == nil {
			title = strings.TrimSuffix(filepath.Base(path), ext)
			meta["title"] = title
		}

		body := findBody(doc)
		md := htmlToMarkdown(body)

		rel, _ := filepath.Rel(s.exportPath, path)
		relPath := strings.TrimSuffix(rel, ext)

		s.pages = append(s.pages, confluencePage{
			relPath:  relPath,
			title:    fmt.Sprintf("%v", meta["title"]),
			markdown: md,
			meta:     meta,
		})
		return nil
	})
}

func (s *ConfluenceSource) Stream(ctx context.Context) (<-chan Record, <-chan error) {
	records := make(chan Record, 64)
	errs := make(chan error, 1)

	go func() {
		defer close(records)
		defer close(errs)

		name := s.Name()
		for i, p := range s.pages {
			if ctx.Err() != nil {
				return
			}

			fields := make(map[string]any, len(p.meta)+1)
			for k, v := range p.meta {
				fields[k] = v
			}
			fields["_raw_content"] = p.markdown

			pk := sanitizePath(p.relPath)

			rec := Record{
				SourceID:   fmt.Sprintf("confluence:%s:%d", name, i),
				SourceDSN:  s.exportPath,
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

func (s *ConfluenceSource) Close() error { return nil }

func extractConfluenceMeta(doc *html.Node) map[string]any {
	meta := map[string]any{}
	var walk func(*html.Node)
	walk = func(n *html.Node) {
		if n.Type == html.ElementNode && n.Data == "title" && n.FirstChild != nil {
			meta["title"] = n.FirstChild.Data
		}
		if n.Type == html.ElementNode && n.Data == "meta" {
			var name, content string
			for _, a := range n.Attr {
				switch a.Key {
				case "name":
					name = a.Val
				case "content":
					content = a.Val
				}
			}
			if name != "" && content != "" {
				meta[name] = content
			}
		}
		for c := n.FirstChild; c != nil; c = c.NextSibling {
			walk(c)
		}
	}
	walk(doc)
	return meta
}

func findBody(doc *html.Node) *html.Node {
	var body *html.Node
	var walk func(*html.Node)
	walk = func(n *html.Node) {
		if n.Type == html.ElementNode && n.Data == "body" {
			body = n
			return
		}
		for c := n.FirstChild; c != nil; c = c.NextSibling {
			walk(c)
		}
	}
	walk(doc)
	if body == nil {
		return doc
	}
	return body
}

func htmlToMarkdown(n *html.Node) string {
	if n == nil {
		return ""
	}
	var buf strings.Builder
	convertNode(&buf, n, 0)
	return strings.TrimSpace(buf.String())
}

func convertNode(buf *strings.Builder, n *html.Node, listDepth int) {
	if n.Type == html.TextNode {
		buf.WriteString(n.Data)
		return
	}

	if n.Type != html.ElementNode {
		for c := n.FirstChild; c != nil; c = c.NextSibling {
			convertNode(buf, c, listDepth)
		}
		return
	}

	switch n.Data {
	case "h1", "h2", "h3", "h4", "h5", "h6":
		level := int(n.Data[1] - '0')
		buf.WriteString("\n\n")
		buf.WriteString(strings.Repeat("#", level))
		buf.WriteByte(' ')
		for c := n.FirstChild; c != nil; c = c.NextSibling {
			convertNode(buf, c, listDepth)
		}
		buf.WriteString("\n\n")

	case "p", "div":
		buf.WriteString("\n\n")
		for c := n.FirstChild; c != nil; c = c.NextSibling {
			convertNode(buf, c, listDepth)
		}
		buf.WriteString("\n\n")

	case "br":
		buf.WriteByte('\n')

	case "strong", "b":
		buf.WriteString("**")
		for c := n.FirstChild; c != nil; c = c.NextSibling {
			convertNode(buf, c, listDepth)
		}
		buf.WriteString("**")

	case "em", "i":
		buf.WriteString("*")
		for c := n.FirstChild; c != nil; c = c.NextSibling {
			convertNode(buf, c, listDepth)
		}
		buf.WriteString("*")

	case "code":
		buf.WriteByte('`')
		for c := n.FirstChild; c != nil; c = c.NextSibling {
			convertNode(buf, c, listDepth)
		}
		buf.WriteByte('`')

	case "pre":
		buf.WriteString("\n\n```\n")
		for c := n.FirstChild; c != nil; c = c.NextSibling {
			convertNode(buf, c, listDepth)
		}
		buf.WriteString("\n```\n\n")

	case "a":
		href := getAttr(n, "href")
		buf.WriteByte('[')
		for c := n.FirstChild; c != nil; c = c.NextSibling {
			convertNode(buf, c, listDepth)
		}
		buf.WriteString("](")
		buf.WriteString(href)
		buf.WriteByte(')')

	case "img":
		alt := getAttr(n, "alt")
		src := getAttr(n, "src")
		buf.WriteString("![")
		buf.WriteString(alt)
		buf.WriteString("](")
		buf.WriteString(src)
		buf.WriteByte(')')

	case "ul":
		buf.WriteByte('\n')
		counter := 0
		for c := n.FirstChild; c != nil; c = c.NextSibling {
			if c.Type == html.ElementNode && c.Data == "li" {
				buf.WriteString(strings.Repeat("  ", listDepth))
				buf.WriteString("- ")
				for gc := c.FirstChild; gc != nil; gc = gc.NextSibling {
					convertNode(buf, gc, listDepth+1)
				}
				buf.WriteByte('\n')
				counter++
			}
		}

	case "ol":
		buf.WriteByte('\n')
		counter := 1
		for c := n.FirstChild; c != nil; c = c.NextSibling {
			if c.Type == html.ElementNode && c.Data == "li" {
				buf.WriteString(strings.Repeat("  ", listDepth))
				fmt.Fprintf(buf, "%d. ", counter)
				for gc := c.FirstChild; gc != nil; gc = gc.NextSibling {
					convertNode(buf, gc, listDepth+1)
				}
				buf.WriteByte('\n')
				counter++
			}
		}

	case "table":
		buf.WriteString("\n\n")
		convertTable(buf, n)
		buf.WriteString("\n\n")

	case "blockquote":
		buf.WriteString("\n\n> ")
		for c := n.FirstChild; c != nil; c = c.NextSibling {
			convertNode(buf, c, listDepth)
		}
		buf.WriteString("\n\n")

	case "hr":
		buf.WriteString("\n\n---\n\n")

	default:
		for c := n.FirstChild; c != nil; c = c.NextSibling {
			convertNode(buf, c, listDepth)
		}
	}
}

func convertTable(buf *strings.Builder, table *html.Node) {
	var rows [][]string
	var walk func(*html.Node)
	walk = func(n *html.Node) {
		if n.Type == html.ElementNode && (n.Data == "tr") {
			var cells []string
			for c := n.FirstChild; c != nil; c = c.NextSibling {
				if c.Type == html.ElementNode && (c.Data == "td" || c.Data == "th") {
					var cellBuf strings.Builder
					for gc := c.FirstChild; gc != nil; gc = gc.NextSibling {
						convertNode(&cellBuf, gc, 0)
					}
					cells = append(cells, strings.TrimSpace(cellBuf.String()))
				}
			}
			if len(cells) > 0 {
				rows = append(rows, cells)
			}
			return
		}
		for c := n.FirstChild; c != nil; c = c.NextSibling {
			walk(c)
		}
	}
	walk(table)

	if len(rows) == 0 {
		return
	}

	// Header row.
	buf.WriteString("| ")
	buf.WriteString(strings.Join(rows[0], " | "))
	buf.WriteString(" |\n")

	// Separator.
	buf.WriteString("|")
	for range rows[0] {
		buf.WriteString(" --- |")
	}
	buf.WriteByte('\n')

	// Data rows.
	for _, row := range rows[1:] {
		buf.WriteString("| ")
		// Pad to match header column count.
		padded := make([]string, len(rows[0]))
		copy(padded, row)
		buf.WriteString(strings.Join(padded, " | "))
		buf.WriteString(" |\n")
	}
}

func getAttr(n *html.Node, key string) string {
	for _, a := range n.Attr {
		if a.Key == key {
			return a.Val
		}
	}
	return ""
}

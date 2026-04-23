package vectorstore

import "strings"

// chunk splits text into overlapping windows of roughly `size` characters
// each, preferring paragraph boundaries. Overlap is the number of trailing
// chars from the previous chunk to prepend to the next — keeps context across
// splits so an embedding of the boundary doesn't lose the surrounding
// sentence.
//
// Deliberately simple: paragraph-aware char slicing, not a tokenizer. For the
// target use case (markdown notes, mostly <5KB each), that's enough.
func chunk(text string, size, overlap int) []string {
	text = strings.TrimSpace(text)
	if text == "" {
		return nil
	}
	if len(text) <= size {
		return []string{text}
	}

	paragraphs := splitParagraphs(text)
	out := make([]string, 0)
	var buf strings.Builder
	for _, p := range paragraphs {
		// Paragraph alone is larger than size → fall back to window split.
		if len(p) > size {
			if buf.Len() > 0 {
				out = append(out, buf.String())
				buf.Reset()
			}
			out = append(out, slidingWindow(p, size, overlap)...)
			continue
		}
		if buf.Len()+len(p)+2 > size && buf.Len() > 0 {
			out = append(out, buf.String())
			// Tail of previous chunk as overlap context for the next.
			tail := ""
			if overlap > 0 && buf.Len() > overlap {
				prev := buf.String()
				tail = prev[len(prev)-overlap:]
			}
			buf.Reset()
			if tail != "" {
				buf.WriteString(tail)
				buf.WriteString("\n\n")
			}
		}
		if buf.Len() > 0 {
			buf.WriteString("\n\n")
		}
		buf.WriteString(p)
	}
	if buf.Len() > 0 {
		out = append(out, buf.String())
	}
	return out
}

func splitParagraphs(text string) []string {
	text = strings.ReplaceAll(text, "\r\n", "\n")
	raw := strings.Split(text, "\n\n")
	out := make([]string, 0, len(raw))
	for _, p := range raw {
		p = strings.TrimSpace(p)
		if p != "" {
			out = append(out, p)
		}
	}
	return out
}

func slidingWindow(text string, size, overlap int) []string {
	if size <= 0 {
		return []string{text}
	}
	step := size - overlap
	if step <= 0 {
		step = size
	}
	runes := []rune(text)
	out := make([]string, 0)
	for start := 0; start < len(runes); start += step {
		end := start + size
		if end > len(runes) {
			end = len(runes)
		}
		out = append(out, strings.TrimSpace(string(runes[start:end])))
		if end == len(runes) {
			break
		}
	}
	return out
}

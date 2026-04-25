package importer

import (
	"bufio"
	"context"
	"encoding/json"
	"fmt"
	"os"
	"strings"
)

// JSONSource implements Source for JSON and JSONL files.
type JSONSource struct {
	filePath string
}

// NewJSON creates a JSON/JSONL source. Format is auto-detected: if the file
// starts with '[' it's a JSON array, otherwise JSONL (one object per line).
func NewJSON(filePath string) (*JSONSource, error) {
	if _, err := os.Stat(filePath); err != nil {
		return nil, fmt.Errorf("json file: %w", err)
	}
	return &JSONSource{filePath: filePath}, nil
}

func (s *JSONSource) Name() string {
	base := s.filePath
	if idx := strings.LastIndex(base, "/"); idx >= 0 {
		base = base[idx+1:]
	}
	base = strings.TrimSuffix(base, ".jsonl")
	base = strings.TrimSuffix(base, ".json")
	return base
}

func (s *JSONSource) Stream(ctx context.Context) (<-chan Record, <-chan error) {
	records := make(chan Record, 64)
	errs := make(chan error, 1)

	go func() {
		defer close(records)
		defer close(errs)

		f, err := os.Open(s.filePath)
		if err != nil {
			errs <- fmt.Errorf("open json: %w", err)
			return
		}
		defer f.Close()

		// Detect format by first non-whitespace byte.
		reader := bufio.NewReader(f)
		firstByte, err := peekFirstNonSpace(reader)
		if err != nil {
			errs <- fmt.Errorf("detect format: %w", err)
			return
		}

		name := s.Name()
		var objects []map[string]any

		if firstByte == '[' {
			// JSON array
			dec := json.NewDecoder(reader)
			if err := dec.Decode(&objects); err != nil {
				errs <- fmt.Errorf("decode json array: %w", err)
				return
			}
		} else {
			// JSONL: one object per line
			scanner := bufio.NewScanner(reader)
			scanner.Buffer(make([]byte, 0, 1024*1024), 10*1024*1024)
			for scanner.Scan() {
				line := strings.TrimSpace(scanner.Text())
				if line == "" {
					continue
				}
				var obj map[string]any
				if err := json.Unmarshal([]byte(line), &obj); err != nil {
					errs <- fmt.Errorf("decode jsonl line: %w", err)
					return
				}
				objects = append(objects, obj)
			}
			if err := scanner.Err(); err != nil {
				errs <- fmt.Errorf("read jsonl: %w", err)
				return
			}
		}

		for i, obj := range objects {
			if ctx.Err() != nil {
				return
			}

			pk := fmt.Sprintf("%d", i)
			if id, ok := obj["id"]; ok {
				pk = fmt.Sprintf("%v", id)
			} else if id, ok := obj["_id"]; ok {
				pk = fmt.Sprintf("%v", id)
			}

			rec := Record{
				SourceID:   fmt.Sprintf("json:%s:%d", name, i),
				SourceDSN:  s.filePath,
				Table:      name,
				Fields:     obj,
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

func (s *JSONSource) Close() error { return nil }

func peekFirstNonSpace(r *bufio.Reader) (byte, error) {
	for {
		b, err := r.ReadByte()
		if err != nil {
			return 0, err
		}
		if b == ' ' || b == '\t' || b == '\n' || b == '\r' {
			continue
		}
		if err := r.UnreadByte(); err != nil {
			return 0, err
		}
		return b, nil
	}
}

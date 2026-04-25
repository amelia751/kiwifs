package importer

import (
	"context"
	"fmt"
	"os"
	"strings"

	"gopkg.in/yaml.v3"
)

type YAMLSource struct {
	filePath string
}

func NewYAML(filePath string) (*YAMLSource, error) {
	if _, err := os.Stat(filePath); err != nil {
		return nil, fmt.Errorf("yaml file: %w", err)
	}
	return &YAMLSource{filePath: filePath}, nil
}

func (s *YAMLSource) Name() string {
	base := s.filePath
	if idx := strings.LastIndex(base, "/"); idx >= 0 {
		base = base[idx+1:]
	}
	base = strings.TrimSuffix(base, ".yaml")
	base = strings.TrimSuffix(base, ".yml")
	return base
}

func (s *YAMLSource) Stream(ctx context.Context) (<-chan Record, <-chan error) {
	records := make(chan Record, 64)
	errs := make(chan error, 1)

	go func() {
		defer close(records)
		defer close(errs)

		data, err := os.ReadFile(s.filePath)
		if err != nil {
			errs <- fmt.Errorf("read yaml: %w", err)
			return
		}

		objects, err := parseYAMLObjects(data)
		if err != nil {
			errs <- fmt.Errorf("parse yaml: %w", err)
			return
		}

		name := s.Name()
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
				SourceID:   fmt.Sprintf("yaml:%s:%d", name, i),
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

func (s *YAMLSource) Close() error { return nil }

func parseYAMLObjects(data []byte) ([]map[string]any, error) {
	// Try array of objects first.
	var arr []map[string]any
	if err := yaml.Unmarshal(data, &arr); err == nil && len(arr) > 0 {
		return arr, nil
	}

	// Try single object with array values (e.g. {students: [{...}, ...]}).
	var obj map[string]any
	if err := yaml.Unmarshal(data, &obj); err != nil {
		return nil, fmt.Errorf("unmarshal yaml: %w", err)
	}

	for _, v := range obj {
		if items, ok := v.([]any); ok {
			var result []map[string]any
			for _, item := range items {
				if m, ok := item.(map[string]any); ok {
					result = append(result, m)
				}
			}
			if len(result) > 0 {
				return result, nil
			}
		}
	}

	// Single object — return it as a one-element list.
	return []map[string]any{obj}, nil
}

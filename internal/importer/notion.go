package importer

import (
	"context"
	"fmt"
	"strings"

	"github.com/jomei/notionapi"
)

// NotionSource implements Source for Notion databases.
type NotionSource struct {
	client     *notionapi.Client
	databaseID string
}

// NewNotion creates a Notion source. apiKey is the integration token.
func NewNotion(apiKey, databaseID string) (*NotionSource, error) {
	if apiKey == "" {
		return nil, fmt.Errorf("notion API key is required (set NOTION_API_KEY)")
	}
	client := notionapi.NewClient(notionapi.Token(apiKey))
	return &NotionSource{client: client, databaseID: databaseID}, nil
}

func (s *NotionSource) Name() string { return "notion" }

func (s *NotionSource) Stream(ctx context.Context) (<-chan Record, <-chan error) {
	records := make(chan Record, 64)
	errs := make(chan error, 1)

	go func() {
		defer close(records)
		defer close(errs)

		var cursor notionapi.Cursor
		idx := 0
		for {
			req := &notionapi.DatabaseQueryRequest{
				PageSize: 100,
			}
			if cursor != "" {
				req.StartCursor = cursor
			}

			resp, err := s.client.Database.Query(ctx, notionapi.DatabaseID(s.databaseID), req)
			if err != nil {
				errs <- fmt.Errorf("notion query: %w", err)
				return
			}

			for _, page := range resp.Results {
				fields := mapNotionProperties(page.Properties)
				pk := string(page.ID)

				if title, ok := fields["title"].(string); ok && title != "" {
					pk = sanitizePath(title)
				} else if name, ok := fields["Name"].(string); ok && name != "" {
					pk = sanitizePath(name)
				}

				rec := Record{
					SourceID:   fmt.Sprintf("notion:%s:%s", s.databaseID, page.ID),
					SourceDSN:  "notion",
					Table:      s.databaseID,
					Fields:     fields,
					PrimaryKey: pk,
				}
				select {
				case records <- rec:
				case <-ctx.Done():
					return
				}
				idx++
			}

			if !resp.HasMore || resp.NextCursor == "" {
				break
			}
			cursor = notionapi.Cursor(resp.NextCursor)
		}
	}()
	return records, errs
}

func (s *NotionSource) Close() error { return nil }

func mapNotionProperties(props notionapi.Properties) map[string]any {
	fields := make(map[string]any, len(props))
	for name, prop := range props {
		fields[name] = mapNotionProperty(prop)
	}
	return fields
}

func mapNotionProperty(prop notionapi.Property) any {
	switch p := prop.(type) {
	case *notionapi.TitleProperty:
		var parts []string
		for _, t := range p.Title {
			parts = append(parts, t.PlainText)
		}
		return strings.Join(parts, "")
	case *notionapi.RichTextProperty:
		var parts []string
		for _, t := range p.RichText {
			parts = append(parts, t.PlainText)
		}
		return strings.Join(parts, "")
	case *notionapi.NumberProperty:
		return p.Number
	case *notionapi.SelectProperty:
		if p.Select.Name != "" {
			return p.Select.Name
		}
		return nil
	case *notionapi.MultiSelectProperty:
		out := make([]string, len(p.MultiSelect))
		for i, s := range p.MultiSelect {
			out[i] = s.Name
		}
		return out
	case *notionapi.DateProperty:
		if p.Date != nil && p.Date.Start != nil {
			return p.Date.Start.String()
		}
		return nil
	case *notionapi.CheckboxProperty:
		return p.Checkbox
	case *notionapi.URLProperty:
		return p.URL
	case *notionapi.EmailProperty:
		return p.Email
	case *notionapi.PhoneNumberProperty:
		return p.PhoneNumber
	case *notionapi.FormulaProperty:
		switch p.Formula.Type {
		case "string":
			return p.Formula.String
		case "number":
			return p.Formula.Number
		case "boolean":
			return p.Formula.Boolean
		case "date":
			if p.Formula.Date != nil && p.Formula.Date.Start != nil {
				return p.Formula.Date.Start.String()
			}
		}
		return nil
	case *notionapi.RelationProperty:
		out := make([]string, len(p.Relation))
		for i, r := range p.Relation {
			out[i] = string(r.ID)
		}
		return out
	default:
		return nil
	}
}

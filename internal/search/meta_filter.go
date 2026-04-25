package search

import (
	"fmt"
	"strings"
)

// ParseMetaFilter parses a single "field op value" expression into a MetaFilter.
func ParseMetaFilter(expr string) (MetaFilter, error) {
	for _, op := range []string{"!=", "<=", ">=", "<>", "=", "<", ">"} {
		if i := strings.Index(expr, op); i > 0 {
			return MetaFilter{
				Field: strings.TrimSpace(expr[:i]),
				Op:    op,
				Value: strings.TrimSpace(expr[i+len(op):]),
			}, nil
		}
	}
	lower := strings.ToLower(expr)
	if i := strings.Index(lower, " not like "); i > 0 {
		return MetaFilter{
			Field: strings.TrimSpace(expr[:i]),
			Op:    "NOT LIKE",
			Value: strings.TrimSpace(expr[i+len(" not like "):]),
		}, nil
	}
	if i := strings.Index(lower, " like "); i > 0 {
		return MetaFilter{
			Field: strings.TrimSpace(expr[:i]),
			Op:    "LIKE",
			Value: strings.TrimSpace(expr[i+len(" like "):]),
		}, nil
	}
	return MetaFilter{}, fmt.Errorf("invalid filter %q — expected <field><op><value>", expr)
}

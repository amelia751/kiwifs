package search

import (
	"fmt"
	"strings"

	"github.com/kiwifs/kiwifs/internal/dataview"
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

// CompileWhereClause parses a simple "field op value" expression into
// parameterized SQL safe for injection into a WHERE clause. Returns the SQL
// fragment and bound args. Only allows validated field names and whitelisted ops.
func CompileWhereClause(expr string) (string, []any, error) {
	mf, err := ParseMetaFilter(expr)
	if err != nil {
		return "", nil, err
	}
	if !dataview.ValidFieldName(mf.Field) {
		return "", nil, fmt.Errorf("invalid field name in where: %q", mf.Field)
	}
	if !validMetaOp[mf.Op] {
		return "", nil, fmt.Errorf("invalid operator in where: %q", mf.Op)
	}
	val := strings.Trim(mf.Value, `"'`)
	sql := fmt.Sprintf("json_extract(frontmatter, '$.%s') %s ?", mf.Field, mf.Op)
	return sql, []any{val}, nil
}

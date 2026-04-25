package cmd

import (
	"context"
	"fmt"
	"strings"
	"time"

	"github.com/kiwifs/kiwifs/internal/dataview"
	"github.com/kiwifs/kiwifs/internal/search"
	"github.com/kiwifs/kiwifs/internal/storage"
	"github.com/spf13/cobra"
)

var aggregateCmd = &cobra.Command{
	Use:   "aggregate",
	Short: "Aggregate files by a frontmatter field",
	Long: `Group files by a frontmatter field and compute aggregations.
Supports count (default), avg, sum, min, and max.`,
	Example: `  kiwifs aggregate --group status --calc count
  kiwifs aggregate --group grade --calc "avg:mastery"
  kiwifs aggregate --group status --calc "count,avg:mastery,max:score" --path students/`,
	RunE: runAggregate,
}

func init() {
	aggregateCmd.Flags().StringP("root", "r", "./knowledge", "knowledge root directory")
	aggregateCmd.Flags().String("group", "", "field to group by (required)")
	aggregateCmd.Flags().String("calc", "count", "aggregation: count, avg:field, sum:field, min:field, max:field (comma-separated)")
	aggregateCmd.Flags().String("path", "", "path prefix to scope results")
	aggregateCmd.Flags().String("where", "", "DQL WHERE filter expression")
	_ = aggregateCmd.MarkFlagRequired("group")
	rootCmd.AddCommand(aggregateCmd)
}

type aggCalcSpec struct {
	fn    string
	field string
}

func (cs aggCalcSpec) label() string {
	if cs.field == "" {
		return cs.fn
	}
	return cs.fn + ":" + cs.field
}

func parseAggCalcSpecs(raw string) ([]aggCalcSpec, error) {
	if raw == "" {
		return []aggCalcSpec{{fn: "count"}}, nil
	}
	parts := strings.Split(raw, ",")
	specs := make([]aggCalcSpec, 0, len(parts))
	for _, p := range parts {
		p = strings.TrimSpace(p)
		if p == "" {
			continue
		}
		if p == "count" {
			specs = append(specs, aggCalcSpec{fn: "count"})
			continue
		}
		fn, field, ok := strings.Cut(p, ":")
		if !ok || field == "" {
			return nil, fmt.Errorf("invalid calc %q: expected func:field", p)
		}
		if !dataview.ValidFieldName(field) {
			return nil, fmt.Errorf("invalid field name: %q", field)
		}
		specs = append(specs, aggCalcSpec{fn: fn, field: field})
	}
	if len(specs) == 0 {
		return []aggCalcSpec{{fn: "count"}}, nil
	}
	return specs, nil
}

func runAggregate(cmd *cobra.Command, args []string) error {
	root, _ := cmd.Flags().GetString("root")
	groupBy, _ := cmd.Flags().GetString("group")
	calcRaw, _ := cmd.Flags().GetString("calc")
	pathPrefix, _ := cmd.Flags().GetString("path")
	where, _ := cmd.Flags().GetString("where")

	if !dataview.ValidFieldName(groupBy) {
		return fmt.Errorf("invalid group field name: %q", groupBy)
	}

	calcs, err := parseAggCalcSpecs(calcRaw)
	if err != nil {
		return err
	}

	store, err := storage.NewLocal(root)
	if err != nil {
		return fmt.Errorf("open storage: %w", err)
	}
	sq, err := search.NewSQLite(root, store)
	if err != nil {
		return fmt.Errorf("open sqlite index: %w", err)
	}
	defer sq.Close()

	validFns := map[string]bool{"count": true, "avg": true, "sum": true, "min": true, "max": true}
	var sb strings.Builder
	sb.WriteString(fmt.Sprintf("SELECT json_extract(frontmatter, '$.%s') AS grp", groupBy))
	for _, cs := range calcs {
		if !validFns[cs.fn] {
			return fmt.Errorf("invalid aggregate function: %q (use count, avg, sum, min, max)", cs.fn)
		}
		switch cs.fn {
		case "count":
			sb.WriteString(", COUNT(*) AS agg_count")
		case "avg":
			sb.WriteString(fmt.Sprintf(", AVG(json_extract(frontmatter, '$.%s'))", cs.field))
		case "sum":
			sb.WriteString(fmt.Sprintf(", SUM(json_extract(frontmatter, '$.%s'))", cs.field))
		case "min":
			sb.WriteString(fmt.Sprintf(", MIN(json_extract(frontmatter, '$.%s'))", cs.field))
		case "max":
			sb.WriteString(fmt.Sprintf(", MAX(json_extract(frontmatter, '$.%s'))", cs.field))
		}
	}
	sb.WriteString(" FROM file_meta")

	var conditions []string
	var queryArgs []any
	if pathPrefix != "" {
		conditions = append(conditions, "path LIKE ? || '%'")
		queryArgs = append(queryArgs, pathPrefix)
	}
	if where != "" {
		compiled, whereArgs, compileErr := search.CompileWhereClause(where)
		if compileErr != nil {
			return fmt.Errorf("invalid where expression: %w", compileErr)
		}
		conditions = append(conditions, compiled)
		queryArgs = append(queryArgs, whereArgs...)
	}
	if len(conditions) > 0 {
		sb.WriteString(" WHERE " + strings.Join(conditions, " AND "))
	}
	sb.WriteString(fmt.Sprintf(" GROUP BY json_extract(frontmatter, '$.%s')", groupBy))
	sb.WriteString(" ORDER BY grp ASC")

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	rows, err := sq.ReadDB().QueryContext(ctx, sb.String(), queryArgs...)
	if err != nil {
		return fmt.Errorf("query: %w", err)
	}
	defer rows.Close()

	// Print header
	header := fmt.Sprintf("%-20s", groupBy)
	for _, cs := range calcs {
		header += fmt.Sprintf("  %-15s", cs.label())
	}
	fmt.Println(header)
	fmt.Println(strings.Repeat("-", len(header)))

	cols, _ := rows.Columns()
	for rows.Next() {
		vals := make([]any, len(cols))
		ptrs := make([]any, len(cols))
		for i := range vals {
			ptrs[i] = &vals[i]
		}
		if err := rows.Scan(ptrs...); err != nil {
			return err
		}
		key := fmt.Sprint(vals[0])
		if key == "<nil>" {
			key = "(none)"
		}

		keys := make([]string, 0, len(calcs))
		valMap := make(map[string]any)
		for i, cs := range calcs {
			keys = append(keys, cs.label())
			valMap[cs.label()] = vals[i+1]
		}

		line := fmt.Sprintf("%-20s", key)
		for _, k := range keys {
			v := valMap[k]
			switch n := v.(type) {
			case int64:
				line += fmt.Sprintf("  %-15d", n)
			case float64:
				line += fmt.Sprintf("  %-15.2f", n)
			default:
				line += fmt.Sprintf("  %-15v", v)
			}
		}
		fmt.Println(line)
	}

	return rows.Err()
}


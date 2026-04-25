package dataview

import (
	"fmt"
	"regexp"
	"strings"
)

// validFieldRe validates field paths used in json_extract.
var validFieldRe = regexp.MustCompile(`^[a-zA-Z0-9_\-.\[\]*]+$`)

// CompileSQL turns a QueryPlan into a SQLite query with bound parameters.
func CompileSQL(plan *QueryPlan) (string, []any, error) {
	c := &compiler{plan: plan}
	return c.compile()
}

// CompileSQLWithIndexer is like CompileSQL but uses the auto-indexer to
// resolve fields to generated columns when available.
func CompileSQLWithIndexer(plan *QueryPlan, indexer *AutoIndexer) (string, []any, error) {
	c := &compiler{plan: plan, indexer: indexer}
	return c.compile()
}

type compiler struct {
	plan    *QueryPlan
	params  []any
	indexer *AutoIndexer
}

func (c *compiler) compile() (string, []any, error) {
	switch c.plan.Type {
	case "count":
		return c.compileCount()
	case "distinct":
		return c.compileDistinct()
	case "task":
		return c.compileTask()
	default:
		return c.compileSelect()
	}
}

func (c *compiler) compileTask() (string, []any, error) {
	var sb strings.Builder
	sb.WriteString("SELECT file_meta.path, file_meta.tasks FROM file_meta")

	// Add base condition: only files with tasks
	var conditions []string
	conditions = append(conditions, "json_array_length(file_meta.tasks) > 0")

	if c.plan.From != "" {
		conditions = append(conditions, "file_meta.path LIKE ? || '%'")
		c.params = append(c.params, c.plan.From)
	}

	for _, tf := range c.plan.FromTags {
		if tf.Negate {
			conditions = append(conditions,
				"NOT EXISTS (SELECT 1 FROM json_each(file_meta.frontmatter, '$.tags') WHERE value = ?)")
		} else {
			conditions = append(conditions,
				"EXISTS (SELECT 1 FROM json_each(file_meta.frontmatter, '$.tags') WHERE value = ?)")
		}
		c.params = append(c.params, tf.Tag)
	}

	if len(conditions) > 0 {
		fmt.Fprintf(&sb, " WHERE %s", strings.Join(conditions, " AND "))
	}

	sb.WriteString(" ORDER BY file_meta.path ASC")
	c.writeLimitOffset(&sb)
	return sb.String(), c.params, nil
}

func (c *compiler) compileCount() (string, []any, error) {
	var sb strings.Builder
	sb.WriteString("SELECT COUNT(*) AS cnt FROM file_meta")

	if err := c.writeFromAndFlatten(&sb); err != nil {
		return "", nil, err
	}
	if err := c.writeWhere(&sb); err != nil {
		return "", nil, err
	}
	return sb.String(), c.params, nil
}

func (c *compiler) compileDistinct() (string, []any, error) {
	if len(c.plan.Fields) == 0 {
		return "", nil, fmt.Errorf("DISTINCT requires a field")
	}
	field := c.plan.Fields[0].Expr
	fieldSQL, err := c.fieldToSQL(field)
	if err != nil {
		return "", nil, err
	}

	var sb strings.Builder
	fmt.Fprintf(&sb, "SELECT DISTINCT %s AS val FROM file_meta", fieldSQL)

	if err := c.writeFromAndFlatten(&sb); err != nil {
		return "", nil, err
	}
	if err := c.writeWhere(&sb); err != nil {
		return "", nil, err
	}

	sb.WriteString(" ORDER BY val ASC")
	c.writeLimitOffset(&sb)
	return sb.String(), c.params, nil
}

func (c *compiler) compileSelect() (string, []any, error) {
	var sb strings.Builder

	if c.plan.GroupBy != "" {
		return c.compileGroupBy()
	}

	// SELECT clause
	if c.plan.WithoutID {
		first := true
		sb.WriteString("SELECT ")
		for _, fs := range c.plan.Fields {
			fieldSQL, fsParams, err := c.fieldSpecToSQL(fs)
			if err != nil {
				return "", nil, err
			}
			c.params = append(c.params, fsParams...)
			if !first {
				sb.WriteString(", ")
			}
			first = false
			fmt.Fprintf(&sb, "%s AS %s", fieldSQL, c.aliasFor(fs))
		}
		if first {
			sb.WriteString("file_meta.frontmatter")
		}
		sb.WriteString(", file_meta.frontmatter")
	} else {
		sb.WriteString("SELECT file_meta.path")
		for _, fs := range c.plan.Fields {
			fieldSQL, fsParams, err := c.fieldSpecToSQL(fs)
			if err != nil {
				return "", nil, err
			}
			c.params = append(c.params, fsParams...)
			fmt.Fprintf(&sb, ", %s AS %s", fieldSQL, c.aliasFor(fs))
		}
		sb.WriteString(", file_meta.frontmatter")
	}
	sb.WriteString(" FROM file_meta")

	if err := c.writeFromAndFlatten(&sb); err != nil {
		return "", nil, err
	}
	if err := c.writeWhere(&sb); err != nil {
		return "", nil, err
	}
	if err := c.writeOrderBy(&sb); err != nil {
		return "", nil, err
	}
	limit := c.plan.Limit + 1
	c.params = append(c.params, limit, c.plan.Offset)
	sb.WriteString(" LIMIT ? OFFSET ?")

	return sb.String(), c.params, nil
}

func (c *compiler) compileGroupBy() (string, []any, error) {
	var sb strings.Builder
	groupSQL, err := c.fieldToSQL(c.plan.GroupBy)
	if err != nil {
		return "", nil, err
	}

	// Select group key + all user fields + frontmatter for row building
	fmt.Fprintf(&sb, "SELECT %s AS grp", groupSQL)
	for _, fs := range c.plan.Fields {
		fieldSQL, fsParams, err := c.fieldSpecToSQL(fs)
		if err != nil {
			return "", nil, err
		}
		c.params = append(c.params, fsParams...)
		fmt.Fprintf(&sb, ", %s AS %s", fieldSQL, c.aliasFor(fs))
	}
	sb.WriteString(", file_meta.path, file_meta.frontmatter")
	sb.WriteString(" FROM file_meta")

	if err := c.writeFromAndFlatten(&sb); err != nil {
		return "", nil, err
	}
	if err := c.writeWhere(&sb); err != nil {
		return "", nil, err
	}
	if err := c.writeOrderBy(&sb); err != nil {
		return "", nil, err
	}
	c.writeLimitOffset(&sb)
	return sb.String(), c.params, nil
}

func (c *compiler) writeFromAndFlatten(sb *strings.Builder) error {
	if c.plan.Flatten != "" {
		if err := validateFieldPath(c.plan.Flatten); err != nil {
			return fmt.Errorf("FLATTEN field: %w", err)
		}
		fmt.Fprintf(sb, ", json_each(file_meta.frontmatter, '$.%s') AS _flat", c.plan.Flatten)
	}
	return nil
}

func (c *compiler) writeWhere(sb *strings.Builder) error {
	var conditions []string

	if c.plan.From != "" {
		conditions = append(conditions, "file_meta.path LIKE ? || '%'")
		c.params = append(c.params, c.plan.From)
	}

	for _, tf := range c.plan.FromTags {
		if tf.Negate {
			conditions = append(conditions,
				"NOT EXISTS (SELECT 1 FROM json_each(file_meta.frontmatter, '$.tags') WHERE value = ?)")
		} else {
			conditions = append(conditions,
				"EXISTS (SELECT 1 FROM json_each(file_meta.frontmatter, '$.tags') WHERE value = ?)")
		}
		c.params = append(c.params, tf.Tag)
	}

	if c.plan.Where != nil {
		whereSQL, whereParams, err := c.compileExpr(c.plan.Where)
		if err != nil {
			return fmt.Errorf("WHERE: %w", err)
		}
		conditions = append(conditions, whereSQL)
		c.params = append(c.params, whereParams...)
	}

	if len(conditions) > 0 {
		fmt.Fprintf(sb, " WHERE %s", strings.Join(conditions, " AND "))
	}
	return nil
}

func (c *compiler) writeOrderBy(sb *strings.Builder) error {
	if len(c.plan.Sorts) > 0 {
		var parts []string
		for _, s := range c.plan.Sorts {
			sortSQL, err := c.fieldToSQL(s.Field)
			if err != nil {
				return fmt.Errorf("SORT field: %w", err)
			}
			dir := "ASC"
			if strings.EqualFold(s.Order, "desc") {
				dir = "DESC"
			}
			parts = append(parts, fmt.Sprintf("%s %s", sortSQL, dir))
		}
		fmt.Fprintf(sb, " ORDER BY %s", strings.Join(parts, ", "))
	} else if c.plan.Sort != "" {
		sortSQL, err := c.fieldToSQL(c.plan.Sort)
		if err != nil {
			return fmt.Errorf("SORT field: %w", err)
		}
		dir := "ASC"
		if strings.EqualFold(c.plan.Order, "desc") {
			dir = "DESC"
		}
		fmt.Fprintf(sb, " ORDER BY %s %s", sortSQL, dir)
	} else {
		sb.WriteString(" ORDER BY file_meta.path ASC")
	}
	return nil
}

func (c *compiler) writeLimitOffset(sb *strings.Builder) {
	c.params = append(c.params, c.plan.Limit, c.plan.Offset)
	sb.WriteString(" LIMIT ? OFFSET ?")
}

func (c *compiler) fieldSpecToSQL(fs FieldSpec) (string, []any, error) {
	if fs.Parsed != nil {
		return c.compileExpr(fs.Parsed)
	}
	sql, err := c.fieldToSQL(fs.Expr)
	return sql, nil, err
}

func (c *compiler) fieldToSQL(field string) (string, error) {
	if sql, isImplicit := resolveField(field); isImplicit {
		return sql, nil
	}
	if c.plan.Flatten != "" && field == c.plan.Flatten {
		return "_flat.value", nil
	}
	if err := validateFieldPath(field); err != nil {
		return "", err
	}
	if c.indexer != nil {
		if col, ok := c.indexer.IndexedColumn(field); ok {
			return col, nil
		}
	}
	return fmt.Sprintf("json_extract(file_meta.frontmatter, '$.%s')", field), nil
}

func (c *compiler) aliasFor(fs FieldSpec) string {
	if fs.Alias != "" {
		safe := strings.NewReplacer(" ", "_", ".", "_", "-", "_").Replace(fs.Alias)
		if safe == "" {
			safe = "col"
		}
		return safe
	}
	safe := strings.NewReplacer(".", "_", "-", "_", "[", "", "]", "", "*", "").Replace(fs.Expr)
	if safe == "" {
		safe = "col"
	}
	return safe
}

func (c *compiler) compileExpr(expr Expr) (string, []any, error) {
	switch e := expr.(type) {
	case *BinaryExpr:
		return c.compileBinary(e)
	case *UnaryExpr:
		return c.compileUnary(e)
	case *FieldRef:
		return c.compileFieldRef(e)
	case *Literal:
		return c.compileLiteral(e)
	case *FuncCall:
		return c.compileFuncCall(e)
	case *ListExpr:
		return c.compileList(e)
	case *BetweenExpr:
		return c.compileBetween(e)
	case *IsNullExpr:
		return c.compileIsNull(e)
	default:
		return "", nil, fmt.Errorf("unknown expression type %T", expr)
	}
}

var binaryOpFmt = map[Operator]string{
	OpAnd:     "(%s AND %s)",
	OpOr:      "(%s OR %s)",
	OpIn:      "%s IN %s",
	OpNotIn:   "%s NOT IN %s",
	OpLike:    "%s LIKE %s",
	OpNotLike: "%s NOT LIKE %s",
}

func (c *compiler) compileBinary(e *BinaryExpr) (string, []any, error) {
	left, lp, err := c.compileExpr(e.Left)
	if err != nil {
		return "", nil, err
	}
	right, rp, err := c.compileExpr(e.Right)
	if err != nil {
		return "", nil, err
	}
	if tmpl, ok := binaryOpFmt[e.Op]; ok {
		return fmt.Sprintf(tmpl, left, right), append(lp, rp...), nil
	}
	return fmt.Sprintf("%s %s %s", left, e.Op.String(), right), append(lp, rp...), nil
}

func (c *compiler) compileUnary(e *UnaryExpr) (string, []any, error) {
	inner, params, err := c.compileExpr(e.Expr)
	if err != nil {
		return "", nil, err
	}
	return fmt.Sprintf("NOT (%s)", inner), params, nil
}

func (c *compiler) compileFieldRef(e *FieldRef) (string, []any, error) {
	if sql, isImplicit := resolveField(e.Path); isImplicit {
		return sql, nil, nil
	}
	if c.plan != nil && c.plan.Flatten != "" && e.Path == c.plan.Flatten {
		return "_flat.value", nil, nil
	}
	if err := validateFieldPath(e.Path); err != nil {
		return "", nil, err
	}
	return fmt.Sprintf("json_extract(file_meta.frontmatter, '$.%s')", e.Path), nil, nil
}

func (c *compiler) compileLiteral(e *Literal) (string, []any, error) {
	if e.Value == nil {
		return "NULL", nil, nil
	}
	return "?", []any{e.Value}, nil
}

func (c *compiler) compileFuncCall(e *FuncCall) (string, []any, error) {
	fn, ok := funcRegistry[strings.ToLower(e.Name)]
	if !ok {
		return "", nil, fmt.Errorf("unknown function %q", e.Name)
	}

	cArgs := make([]compiledArg, len(e.Args))
	nameLower := strings.ToLower(e.Name)
	for i, arg := range e.Args {
		if (nameLower == "contains" || nameLower == "length") && i == 0 {
			if fr, ok := arg.(*FieldRef); ok {
				if err := validateFieldPath(fr.Path); err != nil {
					return "", nil, err
				}
				cArgs[i] = compiledArg{SQL: fmt.Sprintf("'$.%s'", fr.Path)}
				continue
			}
		}

		sql, params, err := c.compileExpr(arg)
		if err != nil {
			return "", nil, err
		}
		cArgs[i] = compiledArg{SQL: sql, Params: params}
	}

	return fn(cArgs)
}

func (c *compiler) compileList(e *ListExpr) (string, []any, error) {
	if len(e.Items) == 0 {
		return "1=0", nil, nil
	}
	parts := make([]string, len(e.Items))
	var allParams []any
	for i, item := range e.Items {
		sql, params, err := c.compileExpr(item)
		if err != nil {
			return "", nil, err
		}
		parts[i] = sql
		allParams = append(allParams, params...)
	}
	return fmt.Sprintf("(%s)", strings.Join(parts, ", ")), allParams, nil
}

func (c *compiler) compileBetween(e *BetweenExpr) (string, []any, error) {
	expr, ep, err := c.compileExpr(e.Expr)
	if err != nil {
		return "", nil, err
	}
	low, lp, err := c.compileExpr(e.Low)
	if err != nil {
		return "", nil, err
	}
	high, hp, err := c.compileExpr(e.High)
	if err != nil {
		return "", nil, err
	}
	var params []any
	params = append(params, ep...)
	params = append(params, lp...)
	params = append(params, hp...)
	return fmt.Sprintf("%s BETWEEN %s AND %s", expr, low, high), params, nil
}

func (c *compiler) compileIsNull(e *IsNullExpr) (string, []any, error) {
	expr, params, err := c.compileExpr(e.Expr)
	if err != nil {
		return "", nil, err
	}
	op := "IS NULL"
	if e.Negate {
		op = "IS NOT NULL"
	}
	return fmt.Sprintf("%s %s", expr, op), params, nil
}

// ValidFieldName reports whether s is a safe field name for use in DQL.
func ValidFieldName(s string) bool {
	return s != "" && validFieldRe.MatchString(s)
}

func validateFieldPath(field string) error {
	if field == "" {
		return fmt.Errorf("empty field path")
	}
	if !validFieldRe.MatchString(field) {
		return fmt.Errorf("invalid field path %q", field)
	}
	return nil
}

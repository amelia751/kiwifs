package dataview

import "fmt"

// FuncCompiler emits SQL + bound args for a built-in function call.
type FuncCompiler func(args []compiledArg) (sql string, params []any, err error)

// compiledArg is the SQL fragment + bound params for one function argument.
type compiledArg struct {
	SQL    string
	Params []any
}

type simpleFuncDef struct {
	arity    int    // exact arity (-1 for variadic)
	minArity int
	maxArity int
	template string // e.g. "lower(%s)" — %s placeholders filled with arg SQL
}

var simpleFuncs = map[string]simpleFuncDef{
	"lower":      {arity: 1, template: "lower(%s)"},
	"upper":      {arity: 1, template: "upper(%s)"},
	"date":       {arity: 1, template: "date(%s)"},
	"typeof":     {arity: 1, template: "typeof(%s)"},
	"number":     {arity: 1, template: "CAST(%s AS REAL)"},
	"string":     {arity: 1, template: "CAST(%s AS TEXT)"},
	"sum":        {arity: 1, template: "SUM(%s)"},
	"average":    {arity: 1, template: "AVG(%s)"},
	"min":        {arity: 1, template: "MIN(%s)"},
	"max":        {arity: 1, template: "MAX(%s)"},
	"nonnull":    {arity: 1, template: "(%s IS NOT NULL)"},
	"striptime":  {arity: 1, template: "date(%s)"},
	"days_since": {arity: 1, template: "(julianday('now') - julianday(%s))"},
	"default":    {arity: 2, template: "COALESCE(%s, %s)"},
	"startswith": {arity: 2, template: "(%s LIKE %s || '%%')"},
	"endswith":   {arity: 2, template: "(%s LIKE '%%' || %s)"},
	"matches":    {arity: 2, template: "(%s LIKE %s)"},
	"join":       {arity: 2, template: "GROUP_CONCAT(%s, %s)"},
	"replace":    {arity: 3, template: "REPLACE(%s, %s, %s)"},
}

func compileSimpleFunc(name string, def simpleFuncDef) FuncCompiler {
	return func(args []compiledArg) (string, []any, error) {
		if def.arity > 0 && len(args) != def.arity {
			return "", nil, fmt.Errorf("%s() requires %d argument(s)", name, def.arity)
		}
		if def.arity == -1 && (len(args) < def.minArity || len(args) > def.maxArity) {
			return "", nil, fmt.Errorf("%s() requires %d-%d arguments", name, def.minArity, def.maxArity)
		}
		sqlArgs := make([]any, len(args))
		var params []any
		for i, a := range args {
			sqlArgs[i] = a.SQL
			params = append(params, a.Params...)
		}
		return fmt.Sprintf(def.template, sqlArgs...), params, nil
	}
}

var funcRegistry = map[string]FuncCompiler{
	"contains":   compileContains,
	"length":     compileLength,
	"now":        compileNow,
	"choice":     compileChoice,
	"substring":  compileSubstring,
	"regextest":  compileRegexTest,
	"regexreplace": compileRegexReplace,
	"dateformat": compileDateFormat,
	"round":      compileRound,
}

func init() {
	for name, def := range simpleFuncs {
		funcRegistry[name] = compileSimpleFunc(name, def)
	}
}

func compileContains(args []compiledArg) (string, []any, error) {
	if len(args) != 2 {
		return "", nil, fmt.Errorf("contains() requires 2 arguments")
	}
	sql := fmt.Sprintf("EXISTS (SELECT 1 FROM json_each(file_meta.frontmatter, %s) AS _je WHERE _je.value = %s)",
		args[0].SQL, args[1].SQL)
	var params []any
	params = append(params, args[0].Params...)
	params = append(params, args[1].Params...)
	return sql, params, nil
}

func compileLength(args []compiledArg) (string, []any, error) {
	if len(args) != 1 {
		return "", nil, fmt.Errorf("length() requires 1 argument")
	}
	// args[0].SQL is a JSON path like '$.name' for FieldRef args (due to
	// special-casing in compileFuncCall). We need json_extract for the ELSE
	// branch so length operates on the actual value, not the path literal.
	sql := fmt.Sprintf(
		"CASE json_type(file_meta.frontmatter, %s) WHEN 'array' THEN json_array_length(file_meta.frontmatter, %s) ELSE length(json_extract(file_meta.frontmatter, %s)) END",
		args[0].SQL, args[0].SQL, args[0].SQL)
	var params []any
	params = append(params, args[0].Params...)
	params = append(params, args[0].Params...)
	params = append(params, args[0].Params...)
	return sql, params, nil
}

func compileNow(args []compiledArg) (string, []any, error) {
	if len(args) != 0 {
		return "", nil, fmt.Errorf("now() takes no arguments")
	}
	return "datetime('now')", nil, nil
}

func compileChoice(args []compiledArg) (string, []any, error) {
	if len(args) != 3 {
		return "", nil, fmt.Errorf("choice() requires 3 arguments (condition, ifTrue, ifFalse)")
	}
	sql := fmt.Sprintf("CASE WHEN %s THEN %s ELSE %s END", args[0].SQL, args[1].SQL, args[2].SQL)
	var params []any
	params = append(params, args[0].Params...)
	params = append(params, args[1].Params...)
	params = append(params, args[2].Params...)
	return sql, params, nil
}

func compileSubstring(args []compiledArg) (string, []any, error) {
	if len(args) < 2 || len(args) > 3 {
		return "", nil, fmt.Errorf("substring() requires 2 or 3 arguments (str, start[, len])")
	}
	var params []any
	params = append(params, args[0].Params...)
	params = append(params, args[1].Params...)
	if len(args) == 3 {
		params = append(params, args[2].Params...)
		return fmt.Sprintf("SUBSTR(%s, %s, %s)", args[0].SQL, args[1].SQL, args[2].SQL), params, nil
	}
	return fmt.Sprintf("SUBSTR(%s, %s)", args[0].SQL, args[1].SQL), params, nil
}

func compileRegexTest(args []compiledArg) (string, []any, error) {
	if len(args) != 2 {
		return "", nil, fmt.Errorf("regextest() requires 2 arguments (pattern, str)")
	}
	sql := fmt.Sprintf("(%s REGEXP %s)", args[1].SQL, args[0].SQL)
	var params []any
	params = append(params, args[1].Params...)
	params = append(params, args[0].Params...)
	return sql, params, nil
}

func compileRegexReplace(args []compiledArg) (string, []any, error) {
	if len(args) != 3 {
		return "", nil, fmt.Errorf("regexreplace() requires 3 arguments (str, pattern, replacement)")
	}
	sql := fmt.Sprintf("regex_replace(%s, %s, %s)", args[0].SQL, args[1].SQL, args[2].SQL)
	var params []any
	params = append(params, args[0].Params...)
	params = append(params, args[1].Params...)
	params = append(params, args[2].Params...)
	return sql, params, nil
}

func compileDateFormat(args []compiledArg) (string, []any, error) {
	if len(args) != 2 {
		return "", nil, fmt.Errorf("dateformat() requires 2 arguments (date, format)")
	}
	sql := fmt.Sprintf("strftime(%s, %s)", args[1].SQL, args[0].SQL)
	var params []any
	params = append(params, args[1].Params...)
	params = append(params, args[0].Params...)
	return sql, params, nil
}

func compileRound(args []compiledArg) (string, []any, error) {
	if len(args) < 1 || len(args) > 2 {
		return "", nil, fmt.Errorf("round() requires 1 or 2 arguments (num[, digits])")
	}
	var params []any
	params = append(params, args[0].Params...)
	if len(args) == 2 {
		params = append(params, args[1].Params...)
		return fmt.Sprintf("ROUND(%s, %s)", args[0].SQL, args[1].SQL), params, nil
	}
	return fmt.Sprintf("ROUND(%s)", args[0].SQL), params, nil
}

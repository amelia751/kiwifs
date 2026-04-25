package dataview

import (
	"database/sql/driver"
	"regexp"
	"sync"

	sqlite "modernc.org/sqlite"
)

var regexpCache sync.Map

func getRegexp(pattern string) (*regexp.Regexp, error) {
	if cached, ok := regexpCache.Load(pattern); ok {
		return cached.(*regexp.Regexp), nil
	}
	re, err := regexp.Compile(pattern)
	if err != nil {
		return nil, err
	}
	regexpCache.Store(pattern, re)
	return re, nil
}

func init() {
	sqlite.MustRegisterDeterministicScalarFunction("regex_replace", 3,
		func(ctx *sqlite.FunctionContext, args []driver.Value) (driver.Value, error) {
			str, _ := args[0].(string)
			pattern, _ := args[1].(string)
			replacement, _ := args[2].(string)
			re, err := getRegexp(pattern)
			if err != nil {
				return str, nil
			}
			return re.ReplaceAllString(str, replacement), nil
		},
	)

	sqlite.MustRegisterDeterministicScalarFunction("regexp", 2,
		func(ctx *sqlite.FunctionContext, args []driver.Value) (driver.Value, error) {
			pattern, _ := args[0].(string)
			str, _ := args[1].(string)
			re, err := getRegexp(pattern)
			if err != nil {
				return int64(0), nil
			}
			if re.MatchString(str) {
				return int64(1), nil
			}
			return int64(0), nil
		},
	)
}

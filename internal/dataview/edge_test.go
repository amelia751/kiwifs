package dataview

import (
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"testing"

	_ "modernc.org/sqlite"
)

func edgeDB(t *testing.T) *sql.DB {
	t.Helper()
	db, err := sql.Open("sqlite", ":memory:?_pragma=journal_mode(WAL)")
	if err != nil { t.Fatal(err) }
	_, err = db.Exec(`CREATE TABLE IF NOT EXISTS file_meta (
		path TEXT PRIMARY KEY,
		frontmatter TEXT NOT NULL DEFAULT '{}',
		tasks TEXT NOT NULL DEFAULT '[]',
		updated_at TEXT NOT NULL
	)`)
	if err != nil { t.Fatal(err) }
	return db
}

func seed(t *testing.T, db *sql.DB, path string, fm map[string]any, tasks string) {
	t.Helper()
	fmb, _ := json.Marshal(fm)
	if tasks == "" { tasks = "[]" }
	_, err := db.Exec(`INSERT INTO file_meta(path, frontmatter, tasks, updated_at) VALUES (?, ?, ?, ?)`,
		path, string(fmb), tasks, "2026-04-25T00:00:00Z")
	if err != nil { t.Fatal(err) }
}

// Edge: empty table, all query types should return 0 rows, not crash
func TestEdge_EmptyDB(t *testing.T) {
	db := edgeDB(t)
	defer db.Close()
	exec := NewExecutor(db)
	for _, q := range []string{
		`TABLE name`, `LIST name`, `COUNT`, `DISTINCT name`,
		`TABLE name GROUP BY status`, `TASK`,
		`CALENDAR last_active`,
	} {
		result, err := exec.Query(context.Background(), q, 0, 0)
		if err != nil { t.Errorf("query %q: %v", q, err) }
		if result == nil { t.Errorf("query %q: nil result", q) }
	}
}

// Edge: NULL frontmatter values
func TestEdge_NullFrontmatter(t *testing.T) {
	db := edgeDB(t)
	defer db.Close()
	seed(t, db, "test.md", map[string]any{"name": nil, "status": "ok"}, "")
	exec := NewExecutor(db)
	r, err := exec.Query(context.Background(), `TABLE name, status`, 0, 0)
	if err != nil { t.Fatal(err) }
	if len(r.Rows) != 1 { t.Fatalf("got %d rows, want 1", len(r.Rows)) }
}

// Edge: deeply nested frontmatter
func TestEdge_DeepNested(t *testing.T) {
	db := edgeDB(t)
	defer db.Close()
	seed(t, db, "deep.md", map[string]any{
		"a": map[string]any{"b": map[string]any{"c": 42}},
	}, "")
	exec := NewExecutor(db)
	r, err := exec.Query(context.Background(), `TABLE a.b.c WHERE a.b.c = 42`, 0, 0)
	if err != nil { t.Fatal(err) }
	if len(r.Rows) != 1 { t.Fatalf("got %d rows, want 1", len(r.Rows)) }
}

// Edge: special chars in field values (quotes, backslashes)
func TestEdge_SpecialCharsInValues(t *testing.T) {
	db := edgeDB(t)
	defer db.Close()
	seed(t, db, "special.md", map[string]any{
		"name": `O'Brien "Bob"`, "status": "active",
	}, "")
	exec := NewExecutor(db)
	r, err := exec.Query(context.Background(), `TABLE name WHERE name = "O'Brien \"Bob\""`, 0, 0)
	if err != nil { t.Fatal(err) }
	if len(r.Rows) != 1 { t.Fatalf("got %d rows, want 1", len(r.Rows)) }
}

// Edge: REGEXP function works
func TestEdge_RegexpFunction(t *testing.T) {
	db := edgeDB(t)
	defer db.Close()
	seed(t, db, "a.md", map[string]any{"name": "Alice"}, "")
	seed(t, db, "b.md", map[string]any{"name": "Bob"}, "")
	seed(t, db, "c.md", map[string]any{"name": "Charlie"}, "")
	exec := NewExecutor(db)
	r, err := exec.Query(context.Background(), `TABLE name WHERE regextest("^[AB]", name)`, 0, 0)
	if err != nil { t.Fatal(err) }
	if len(r.Rows) != 2 { t.Fatalf("got %d rows, want 2 (Alice, Bob)", len(r.Rows)) }
}

// Edge: TASK BETWEEN
func TestEdge_TaskBetween(t *testing.T) {
	db := edgeDB(t)
	defer db.Close()
	tasks := `[{"text":"A","completed":false,"line":1},{"text":"B","completed":false,"line":3},{"text":"C","completed":false,"line":5}]`
	seed(t, db, "p.md", map[string]any{"name": "P"}, tasks)
	exec := NewExecutor(db)
	r, err := exec.Query(context.Background(), `TASK WHERE line BETWEEN 2 AND 4`, 0, 0)
	if err != nil { t.Fatal(err) }
	if len(r.Rows) != 1 { t.Fatalf("got %d rows, want 1 (line 3)", len(r.Rows)) }
	if r.Rows[0]["text"] != "B" { t.Errorf("text = %v, want B", r.Rows[0]["text"]) }
}

// Edge: TASK IS NULL / IS NOT NULL
// Note: Go JSON unmarshalling sets missing string fields to "" (zero value),
// not nil. So "due" for a task without it is "" not nil. IS NULL checks for
// Go nil, which only occurs for fields not in the taskRow struct (e.g. meta.*).
func TestEdge_TaskIsNull(t *testing.T) {
	db := edgeDB(t)
	defer db.Close()
	tasks := `[{"text":"A","completed":false,"line":1,"meta":{"priority":1}},{"text":"B","completed":false,"line":2}]`
	seed(t, db, "p.md", map[string]any{"name": "P"}, tasks)
	exec := NewExecutor(db)
	// meta.priority IS NOT NULL — only task A has it
	r, err := exec.Query(context.Background(), `TASK WHERE meta.priority IS NOT NULL`, 0, 0)
	if err != nil { t.Fatal(err) }
	if len(r.Rows) != 1 { t.Fatalf("IS NOT NULL: got %d rows, want 1", len(r.Rows)) }
	if r.Rows[0]["text"] != "A" { t.Errorf("text = %v, want A", r.Rows[0]["text"]) }
	// meta.priority IS NULL — only task B (no meta)
	r2, err := exec.Query(context.Background(), `TASK WHERE meta.priority IS NULL`, 0, 0)
	if err != nil { t.Fatal(err) }
	if len(r2.Rows) != 1 { t.Fatalf("IS NULL: got %d rows, want 1", len(r2.Rows)) }
	if r2.Rows[0]["text"] != "B" { t.Errorf("text = %v, want B", r2.Rows[0]["text"]) }
}

// Edge: WITHOUT ID + GROUP BY
func TestEdge_WithoutIDGroupBy(t *testing.T) {
	db := edgeDB(t)
	defer db.Close()
	seed(t, db, "a.md", map[string]any{"name": "A", "status": "active"}, "")
	seed(t, db, "b.md", map[string]any{"name": "B", "status": "active"}, "")
	seed(t, db, "c.md", map[string]any{"name": "C", "status": "draft"}, "")
	exec := NewExecutor(db)
	r, err := exec.Query(context.Background(), `TABLE WITHOUT ID name GROUP BY status`, 0, 0)
	if err != nil { t.Fatal(err) }
	if len(r.Groups) < 2 { t.Fatalf("got %d groups, want >=2", len(r.Groups)) }
}

// Edge: multiple chained WHERE + FROM
func TestEdge_ChainedWhereFrom(t *testing.T) {
	db := edgeDB(t)
	defer db.Close()
	seed(t, db, "docs/a.md", map[string]any{"name": "A", "priority": 1, "status": "active"}, "")
	seed(t, db, "docs/b.md", map[string]any{"name": "B", "priority": 5, "status": "active"}, "")
	seed(t, db, "docs/c.md", map[string]any{"name": "C", "priority": 3, "status": "draft"}, "")
	seed(t, db, "other/d.md", map[string]any{"name": "D", "priority": 1, "status": "active"}, "")
	exec := NewExecutor(db)
	r, err := exec.Query(context.Background(),
		`TABLE name FROM "docs/" WHERE status = "active" WHERE priority > 2`, 0, 0)
	if err != nil { t.Fatal(err) }
	if len(r.Rows) != 1 { t.Fatalf("got %d rows, want 1 (B)", len(r.Rows)) }
	if r.Rows[0]["name"] != "B" { t.Errorf("name = %v, want B", r.Rows[0]["name"]) }
}

// Edge: computed column with alias in WHERE (should work on raw field)
func TestEdge_AliasDoesNotAffectWhere(t *testing.T) {
	db := edgeDB(t)
	defer db.Close()
	seed(t, db, "a.md", map[string]any{"name": "Alpha", "score": 10}, "")
	seed(t, db, "b.md", map[string]any{"name": "Beta", "score": 90}, "")
	exec := NewExecutor(db)
	r, err := exec.Query(context.Background(),
		`TABLE name AS "Title", score AS "Points" WHERE score > 50`, 0, 0)
	if err != nil { t.Fatal(err) }
	if len(r.Rows) != 1 { t.Fatalf("got %d rows, want 1", len(r.Rows)) }
	if r.Rows[0]["Title"] != "Beta" { t.Errorf("Title = %v, want Beta", r.Rows[0]["Title"]) }
	if r.Rows[0]["Points"] == nil { t.Error("Points is nil") }
}

// Edge: SQL injection via field value (should be parameterized)
func TestEdge_InjectionViaValue(t *testing.T) {
	db := edgeDB(t)
	defer db.Close()
	seed(t, db, "a.md", map[string]any{"name": "safe"}, "")
	exec := NewExecutor(db)
	_, err := exec.Query(context.Background(),
		`TABLE name WHERE name = "'; DROP TABLE file_meta; --"`, 0, 0)
	if err != nil { t.Fatal(err) }
	// Verify table still exists
	var cnt int
	if err := db.QueryRow("SELECT COUNT(*) FROM file_meta").Scan(&cnt); err != nil {
		t.Fatalf("table dropped! %v", err)
	}
	if cnt != 1 { t.Errorf("expected 1 row, got %d", cnt) }
}

// Edge: LIMIT 0 should use default
func TestEdge_LimitZero(t *testing.T) {
	db := edgeDB(t)
	defer db.Close()
	for i := 0; i < 60; i++ {
		seed(t, db, fmt.Sprintf("f%d.md", i), map[string]any{"name": fmt.Sprintf("F%d", i)}, "")
	}
	exec := NewExecutor(db)
	r, err := exec.Query(context.Background(), `TABLE name`, 0, 0)
	if err != nil { t.Fatal(err) }
	// Default limit is 50, so should get 50 rows
	if len(r.Rows) != 50 { t.Errorf("got %d rows, want 50 (default limit)", len(r.Rows)) }
	if !r.HasMore { t.Error("expected HasMore=true") }
}


// Edge: regex_replace function
func TestEdge_RegexReplace(t *testing.T) {
	db := edgeDB(t)
	defer db.Close()
	seed(t, db, "a.md", map[string]any{"name": "hello-world-123"}, "")
	exec := NewExecutor(db)
	r, err := exec.Query(context.Background(),
		`TABLE regexreplace(name, "[0-9]+", "NUM") AS cleaned`, 0, 0)
	if err != nil { t.Fatal(err) }
	if len(r.Rows) != 1 { t.Fatalf("got %d rows, want 1", len(r.Rows)) }
	if r.Rows[0]["cleaned"] != "hello-world-NUM" {
		t.Errorf("cleaned = %v, want hello-world-NUM", r.Rows[0]["cleaned"])
	}
}

// Edge: implicit meta fields (_path, _name, _folder, _ext, _updated)
func TestEdge_ImplicitMetaFields(t *testing.T) {
	db := edgeDB(t)
	defer db.Close()
	seed(t, db, "docs/notes/hello.md", map[string]any{"title": "Hi"}, "")
	exec := NewExecutor(db)
	r, err := exec.Query(context.Background(),
		`TABLE _path, _name, _folder, _ext`, 0, 0)
	if err != nil { t.Fatal(err) }
	if len(r.Rows) != 1 { t.Fatal("expected 1 row") }
	if r.Rows[0]["_path"] != "docs/notes/hello.md" {
		t.Errorf("_path = %v", r.Rows[0]["_path"])
	}
	if r.Rows[0]["_name"] != "hello.md" {
		t.Errorf("_name = %v, want hello.md", r.Rows[0]["_name"])
	}
	if r.Rows[0]["_ext"] != ".md" {
		t.Errorf("_ext = %v, want .md", r.Rows[0]["_ext"])
	}
}

// Edge: DISTINCT with no matching rows
func TestEdge_DistinctEmpty(t *testing.T) {
	db := edgeDB(t)
	defer db.Close()
	exec := NewExecutor(db)
	r, err := exec.Query(context.Background(), `DISTINCT status`, 0, 0)
	if err != nil { t.Fatal(err) }
	if len(r.Rows) != 0 { t.Errorf("got %d rows, want 0", len(r.Rows)) }
}

// Edge: DISTINCT with multiple values
func TestEdge_DistinctValues(t *testing.T) {
	db := edgeDB(t)
	defer db.Close()
	seed(t, db, "a.md", map[string]any{"status": "active"}, "")
	seed(t, db, "b.md", map[string]any{"status": "draft"}, "")
	seed(t, db, "c.md", map[string]any{"status": "active"}, "")
	seed(t, db, "d.md", map[string]any{"status": "archived"}, "")
	exec := NewExecutor(db)
	r, err := exec.Query(context.Background(), `DISTINCT status`, 0, 0)
	if err != nil { t.Fatal(err) }
	if len(r.Rows) != 3 { t.Errorf("got %d distinct, want 3", len(r.Rows)) }
}

// Edge: LIST format
func TestEdge_ListFormat(t *testing.T) {
	db := edgeDB(t)
	defer db.Close()
	seed(t, db, "a.md", map[string]any{"name": "Alpha"}, "")
	seed(t, db, "b.md", map[string]any{"name": "Beta"}, "")
	exec := NewExecutor(db)
	r, err := exec.Query(context.Background(), `LIST name`, 0, 0)
	if err != nil { t.Fatal(err) }
	if len(r.Rows) != 2 { t.Fatalf("got %d rows, want 2", len(r.Rows)) }
}

// Edge: COUNT format
func TestEdge_CountFormat(t *testing.T) {
	db := edgeDB(t)
	defer db.Close()
	seed(t, db, "a.md", map[string]any{"status": "ok"}, "")
	seed(t, db, "b.md", map[string]any{"status": "ok"}, "")
	seed(t, db, "c.md", map[string]any{"status": "bad"}, "")
	exec := NewExecutor(db)
	r, err := exec.Query(context.Background(), `COUNT`, 0, 0)
	if err != nil { t.Fatal(err) }
	if r.Total != 3 { t.Errorf("total = %d, want 3", r.Total) }
}

// Edge: multiple SORTs
func TestEdge_MultipleSorts(t *testing.T) {
	db := edgeDB(t)
	defer db.Close()
	seed(t, db, "a.md", map[string]any{"priority": 1, "name": "Zeta"}, "")
	seed(t, db, "b.md", map[string]any{"priority": 1, "name": "Alpha"}, "")
	seed(t, db, "c.md", map[string]any{"priority": 2, "name": "Beta"}, "")
	exec := NewExecutor(db)
	r, err := exec.Query(context.Background(),
		`TABLE name SORT priority ASC SORT name ASC`, 0, 0)
	if err != nil { t.Fatal(err) }
	if len(r.Rows) != 3 { t.Fatal("expected 3 rows") }
	if r.Rows[0]["name"] != "Alpha" { t.Errorf("r[0] = %v, want Alpha", r.Rows[0]["name"]) }
	if r.Rows[1]["name"] != "Zeta" { t.Errorf("r[1] = %v, want Zeta", r.Rows[1]["name"]) }
	if r.Rows[2]["name"] != "Beta" { t.Errorf("r[2] = %v, want Beta", r.Rows[2]["name"]) }
}

// Edge: function in WHERE — length()
func TestEdge_FunctionInWhere(t *testing.T) {
	db := edgeDB(t)
	defer db.Close()
	seed(t, db, "a.md", map[string]any{"name": "Hi"}, "")
	seed(t, db, "b.md", map[string]any{"name": "Hello World"}, "")
	exec := NewExecutor(db)
	r, err := exec.Query(context.Background(),
		`TABLE name WHERE length(name) > 5`, 0, 0)
	if err != nil { t.Fatal(err) }
	if len(r.Rows) != 1 { t.Fatalf("got %d rows, want 1", len(r.Rows)) }
	if r.Rows[0]["name"] != "Hello World" { t.Errorf("name = %v", r.Rows[0]["name"]) }
}

// Edge: GROUP BY basic
func TestEdge_GroupByBasic(t *testing.T) {
	db := edgeDB(t)
	defer db.Close()
	seed(t, db, "a.md", map[string]any{"name": "A", "status": "ok"}, "")
	seed(t, db, "b.md", map[string]any{"name": "B", "status": "ok"}, "")
	seed(t, db, "c.md", map[string]any{"name": "C", "status": "bad"}, "")
	exec := NewExecutor(db)
	r, err := exec.Query(context.Background(), `TABLE name GROUP BY status`, 0, 0)
	if err != nil { t.Fatalf("group by: %v", err) }
	if len(r.Groups) < 2 { t.Fatalf("got %d groups, want >=2", len(r.Groups)) }
}


package dataview

import (
	"context"
	"database/sql"
	"encoding/json"
	"os"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/kiwifs/kiwifs/internal/storage"
	_ "modernc.org/sqlite"
)

type memStore struct {
	mu    sync.RWMutex
	files map[string][]byte
}

func newMemStore() *memStore {
	return &memStore{files: make(map[string][]byte)}
}

func (m *memStore) Read(_ context.Context, path string) ([]byte, error) {
	m.mu.RLock()
	defer m.mu.RUnlock()
	b, ok := m.files[path]
	if !ok {
		return nil, os.ErrNotExist
	}
	return b, nil
}

func (m *memStore) Write(_ context.Context, path string, content []byte) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.files[path] = content
	return nil
}

func (m *memStore) Delete(context.Context, string) error                           { return nil }
func (m *memStore) List(context.Context, string) ([]storage.Entry, error)          { return nil, nil }
func (m *memStore) Stat(context.Context, string) (*storage.Entry, error)           { return nil, nil }
func (m *memStore) Exists(_ context.Context, _ string) bool                        { return false }
func (m *memStore) AbsPath(path string) string                                     { return path }

func setupTestDB(t *testing.T) *sql.DB {
	t.Helper()
	db, err := sql.Open("sqlite", ":memory:?_pragma=journal_mode(WAL)&_pragma=foreign_keys(1)")
	if err != nil {
		t.Fatal(err)
	}
	_, err = db.Exec(`CREATE TABLE IF NOT EXISTS file_meta (
		path TEXT PRIMARY KEY,
		frontmatter TEXT NOT NULL DEFAULT '{}',
		tasks TEXT NOT NULL DEFAULT '[]',
		updated_at TEXT NOT NULL
	)`)
	if err != nil {
		t.Fatal(err)
	}

	students := []struct {
		path string
		fm   map[string]any
	}{
		{"students/priya.md", map[string]any{
			"name": "Priya Sharma", "status": "active", "phone": "919999999999",
			"mastery": map[string]any{"derivatives": 0.1},
			"tags": []any{"math", "calculus"}, "last_active": "2026-04-24",
		}},
		{"students/amit.md", map[string]any{
			"name": "Amit Patel", "status": "active", "phone": "918888888888",
			"mastery": map[string]any{"derivatives": 0.7},
			"tags": []any{"physics", "math"}, "last_active": "2026-04-20",
		}},
		{"students/chen.md", map[string]any{
			"name": "Chen Wei", "status": "inactive", "phone": "8612345678",
			"mastery": map[string]any{"derivatives": 0.9},
			"tags": []any{"physics"}, "last_active": "2026-03-15",
		}},
		{"concepts/derivatives.md", map[string]any{
			"name": "Derivatives", "status": "published", "difficulty": 3,
			"tags": []any{"math", "calculus"},
		}},
		{"concepts/integration.md", map[string]any{
			"name": "Integration", "status": "draft",
			"tags": []any{"math"},
		}},
	}
	for _, s := range students {
		fm, _ := json.Marshal(s.fm)
		_, err := db.Exec(`INSERT INTO file_meta(path, frontmatter, tasks, updated_at) VALUES (?, ?, ?, ?)`,
			s.path, string(fm), "[]", "2026-04-24T12:00:00Z")
		if err != nil {
			t.Fatal(err)
		}
	}

	// Add a file with tasks for TASK query tests
	taskFM, _ := json.Marshal(map[string]any{"name": "Project Plan", "status": "active"})
	taskJSON := `[{"text":"Buy groceries","completed":true,"line":3,"tags":["shopping"],"meta":{"priority":1}},{"text":"Send email","completed":false,"line":4,"due":"2026-05-01","meta":{"priority":2}},{"text":"Read chapter 3","completed":false,"line":5,"tags":["study"],"meta":{"priority":1}}]`
	_, err = db.Exec(`INSERT INTO file_meta(path, frontmatter, tasks, updated_at) VALUES (?, ?, ?, ?)`,
		"projects/plan.md", string(taskFM), taskJSON, "2026-04-24T12:00:00Z")
	if err != nil {
		t.Fatal(err)
	}

	return db
}

func TestIntegration_TableQuery(t *testing.T) {
	db := setupTestDB(t)
	defer db.Close()
	exec := NewExecutor(db)

	result, err := exec.Query(context.Background(),
		`TABLE name, status FROM "students/" WHERE status = "active" SORT name ASC`, 0, 0)
	if err != nil {
		t.Fatal(err)
	}
	if len(result.Rows) != 2 {
		t.Fatalf("got %d rows, want 2", len(result.Rows))
	}
	// Amit comes before Priya alphabetically
	if result.Rows[0]["name"] != "Amit Patel" {
		t.Errorf("row[0].name = %v, want Amit Patel", result.Rows[0]["name"])
	}
	if result.Rows[1]["name"] != "Priya Sharma" {
		t.Errorf("row[1].name = %v, want Priya Sharma", result.Rows[1]["name"])
	}
}

func TestIntegration_CountQuery(t *testing.T) {
	db := setupTestDB(t)
	defer db.Close()
	exec := NewExecutor(db)

	result, err := exec.Query(context.Background(),
		`COUNT FROM "students/" WHERE status = "active"`, 0, 0)
	if err != nil {
		t.Fatal(err)
	}
	if result.Total != 2 {
		t.Errorf("count = %d, want 2", result.Total)
	}
}

func TestIntegration_DistinctQuery(t *testing.T) {
	db := setupTestDB(t)
	defer db.Close()
	exec := NewExecutor(db)

	result, err := exec.Query(context.Background(),
		`DISTINCT status`, 0, 0)
	if err != nil {
		t.Fatal(err)
	}
	if len(result.Rows) < 3 {
		t.Errorf("got %d distinct values, want at least 3", len(result.Rows))
	}
}

func TestIntegration_ListQuery(t *testing.T) {
	db := setupTestDB(t)
	defer db.Close()
	exec := NewExecutor(db)

	result, err := exec.Query(context.Background(),
		`LIST FROM "concepts/"`, 0, 0)
	if err != nil {
		t.Fatal(err)
	}
	if len(result.Rows) != 2 {
		t.Fatalf("got %d rows, want 2", len(result.Rows))
	}
}

func TestIntegration_GroupBy(t *testing.T) {
	db := setupTestDB(t)
	defer db.Close()
	exec := NewExecutor(db)

	result, err := exec.Query(context.Background(),
		`TABLE name GROUP BY status`, 0, 0)
	if err != nil {
		t.Fatal(err)
	}
	if len(result.Groups) < 2 {
		t.Errorf("got %d groups, want at least 2", len(result.Groups))
	}
}

func TestIntegration_NestedField(t *testing.T) {
	db := setupTestDB(t)
	defer db.Close()
	exec := NewExecutor(db)

	result, err := exec.Query(context.Background(),
		`TABLE name, mastery.derivatives FROM "students/" WHERE mastery.derivatives < 0.5 SORT mastery.derivatives ASC`, 0, 0)
	if err != nil {
		t.Fatal(err)
	}
	if len(result.Rows) != 1 {
		t.Fatalf("got %d rows, want 1 (only Priya has derivatives < 0.5)", len(result.Rows))
	}
}

func TestIntegration_ImplicitFields(t *testing.T) {
	db := setupTestDB(t)
	defer db.Close()
	exec := NewExecutor(db)

	result, err := exec.Query(context.Background(),
		`TABLE _path, _updated SORT _updated DESC LIMIT 3`, 0, 0)
	if err != nil {
		t.Fatal(err)
	}
	if len(result.Rows) == 0 {
		t.Fatal("no rows")
	}
	if result.Rows[0]["_path"] == nil {
		t.Error("_path is nil")
	}
}

func TestIntegration_InOperator(t *testing.T) {
	db := setupTestDB(t)
	defer db.Close()
	exec := NewExecutor(db)

	result, err := exec.Query(context.Background(),
		`TABLE name WHERE status IN ("active", "draft")`, 0, 0)
	if err != nil {
		t.Fatal(err)
	}
	// 2 active students + 1 draft concept + 1 active project = 4
	if len(result.Rows) != 4 {
		t.Errorf("got %d rows, want 4", len(result.Rows))
	}
}

func TestIntegration_IsNull(t *testing.T) {
	db := setupTestDB(t)
	defer db.Close()
	exec := NewExecutor(db)

	result, err := exec.Query(context.Background(),
		`TABLE name WHERE difficulty IS NOT NULL`, 0, 0)
	if err != nil {
		t.Fatal(err)
	}
	if len(result.Rows) != 1 {
		t.Errorf("got %d rows, want 1 (only derivatives.md has difficulty)", len(result.Rows))
	}
}

func TestIntegration_Pagination(t *testing.T) {
	db := setupTestDB(t)
	defer db.Close()
	exec := NewExecutor(db)

	result, err := exec.Query(context.Background(),
		`TABLE name SORT name ASC LIMIT 2 OFFSET 0`, 0, 0)
	if err != nil {
		t.Fatal(err)
	}
	if len(result.Rows) != 2 {
		t.Fatalf("got %d rows, want 2", len(result.Rows))
	}
	if !result.HasMore {
		t.Error("expected has_more = true")
	}
}

func TestIntegration_Render(t *testing.T) {
	db := setupTestDB(t)
	defer db.Close()
	exec := NewExecutor(db)

	result, err := exec.Query(context.Background(),
		`TABLE name, status FROM "students/" WHERE status = "active" SORT name ASC`, 0, 0)
	if err != nil {
		t.Fatal(err)
	}

	// Table format
	table := Render(result, "table")
	if !contains(table, "| Name |") {
		t.Errorf("table output missing header: %s", table)
	}
	if !contains(table, "Amit Patel") {
		t.Errorf("table output missing Amit: %s", table)
	}

	// List format
	list := Render(result, "list")
	if !contains(list, "- [") {
		t.Errorf("list output missing bullets: %s", list)
	}

	// JSON format
	js := Render(result, "json")
	if !contains(js, `"columns"`) {
		t.Errorf("json output missing columns: %s", js)
	}
}

func TestIntegration_RenderCount(t *testing.T) {
	db := setupTestDB(t)
	defer db.Close()
	exec := NewExecutor(db)

	result, err := exec.Query(context.Background(),
		`COUNT WHERE status = "active"`, 0, 0)
	if err != nil {
		t.Fatal(err)
	}
	s := Render(result, "count")
	// 2 active students + 1 active project = 3
	if !contains(s, "**3**") {
		t.Errorf("count render = %q, want **3**", s)
	}
}

func TestIntegration_EmptyResult(t *testing.T) {
	db := setupTestDB(t)
	defer db.Close()
	exec := NewExecutor(db)

	result, err := exec.Query(context.Background(),
		`TABLE name WHERE status = "nonexistent"`, 0, 0)
	if err != nil {
		t.Fatal(err)
	}
	if len(result.Rows) != 0 {
		t.Errorf("got %d rows, want 0", len(result.Rows))
	}
	s := Render(result, "table")
	if !contains(s, "*No results*") {
		t.Errorf("empty table render = %q, want *No results*", s)
	}
}

func TestRenderer_FormatColumnHeader(t *testing.T) {
	tests := []struct {
		input string
		want  string
	}{
		{"mastery.derivatives", "Mastery Derivatives"},
		{"_word_count", "Word Count"},
		{"$.status", "Status"},
		{"name", "Name"},
	}
	for _, tt := range tests {
		got := formatColumnHeader(tt.input)
		if got != tt.want {
			t.Errorf("formatColumnHeader(%q) = %q, want %q", tt.input, got, tt.want)
		}
	}
}

func TestRenderer_FormatValue(t *testing.T) {
	tests := []struct {
		input any
		want  string
	}{
		{nil, "—"},
		{"hello", "hello"},
		{3.14, "3.14"},
		{float64(42), "42"},
		{int64(7), "7"},
		{true, "true"},
		{false, "false"},
		{"2026-04-24", "Apr 24, 2026"},
	}
	for _, tt := range tests {
		got := formatValue(tt.input)
		if got != tt.want {
			t.Errorf("formatValue(%v) = %q, want %q", tt.input, got, tt.want)
		}
	}
}

func TestIntegration_LegacyFormat(t *testing.T) {
	db := setupTestDB(t)
	defer db.Close()
	exec := NewExecutor(db)

	result, err := exec.Query(context.Background(), `type: table
columns: name, status
where: status = "active"
sort: name asc
from: students/`, 0, 0)
	if err != nil {
		t.Fatal(err)
	}
	if len(result.Rows) != 2 {
		t.Errorf("got %d rows, want 2", len(result.Rows))
	}
}

func TestIntegration_Flatten(t *testing.T) {
	db := setupTestDB(t)
	defer db.Close()
	exec := NewExecutor(db)

	result, err := exec.Query(context.Background(),
		`TABLE name FLATTEN tags`, 0, 0)
	if err != nil {
		t.Fatal(err)
	}
	// Priya: [math, calculus] → 2, Amit: [physics, math] → 2, Chen: [physics] → 1
	// Derivatives: [math, calculus] → 2, Integration: [math] → 1
	// Total: 8 rows
	if len(result.Rows) != 8 {
		t.Fatalf("got %d rows, want 8 (one per tag)", len(result.Rows))
	}
}

func TestIntegration_FlattenFromFolder(t *testing.T) {
	db := setupTestDB(t)
	defer db.Close()
	exec := NewExecutor(db)

	result, err := exec.Query(context.Background(),
		`TABLE name FLATTEN tags FROM "students/"`, 0, 0)
	if err != nil {
		t.Fatal(err)
	}
	// Priya: 2, Amit: 2, Chen: 1 → 5 rows
	if len(result.Rows) != 5 {
		t.Fatalf("got %d rows, want 5", len(result.Rows))
	}
}

func TestImplicitField_Ext(t *testing.T) {
	db := setupTestDB(t)
	defer db.Close()
	exec := NewExecutor(db)

	result, err := exec.Query(context.Background(),
		`TABLE _ext LIMIT 1`, 0, 0)
	if err != nil {
		t.Fatal(err)
	}
	if len(result.Rows) == 0 {
		t.Fatal("no rows")
	}
	ext := result.Rows[0]["_ext"]
	if ext != ".md" {
		t.Errorf("_ext = %v, want .md", ext)
	}
}

func TestExecutor_MaxScanRows(t *testing.T) {
	db := setupTestDB(t)
	defer db.Close()
	exec := NewExecutor(db)
	exec.SetLimits(2, 0)

	result, err := exec.Query(context.Background(), `LIST`, 0, 0)
	if err != nil {
		t.Fatal(err)
	}
	if len(result.Rows) > 2 {
		t.Errorf("got %d rows, want ≤2 with maxScanRows=2", len(result.Rows))
	}
}

func TestParseExpr_EmptyInList(t *testing.T) {
	_, err := ParseExpr(`status IN ()`)
	if err == nil {
		t.Error("expected error for empty IN list")
	}
}

func TestCompileSQL_InvalidSortField(t *testing.T) {
	plan := &QueryPlan{
		Type:  "table",
		Sort:  "'; DROP TABLE",
		Limit: 50,
	}
	_, _, err := CompileSQL(plan)
	if err == nil {
		t.Error("expected error for invalid sort field")
	}
}

func TestParseQuery_BacktickFieldNames(t *testing.T) {
	plan, err := ParseQuery("TABLE `sort`, `from`, `limit` WHERE status = \"active\"")
	if err != nil {
		t.Fatal(err)
	}
	want := []string{"sort", "from", "limit"}
	if len(plan.Fields) != len(want) {
		t.Fatalf("got %d fields, want %d", len(plan.Fields), len(want))
	}
	for i, f := range plan.Fields {
		if f.Expr != want[i] {
			t.Errorf("field[%d] = %q, want %q", i, f.Expr, want[i])
		}
	}
}

func TestRenderer_ISODate_NotTooAggressive(t *testing.T) {
	input := "2026-04-24 is my birthday"
	got := formatValue(input)
	if got != input {
		t.Errorf("formatValue(%q) = %q, want unchanged string", input, got)
	}
	// Pure date should still be formatted
	got = formatValue("2026-04-24")
	if got != "Apr 24, 2026" {
		t.Errorf("formatValue(\"2026-04-24\") = %q, want \"Apr 24, 2026\"", got)
	}
}

func TestRenderer_HasMore_Honest(t *testing.T) {
	result := &QueryResult{
		Columns: []string{"_path"},
		Rows: []map[string]any{
			{"_path": "a.md"},
			{"_path": "b.md"},
		},
		HasMore: true,
		Total:   -1,
	}
	rendered := Render(result, "table")
	if !contains(rendered, "2+ results") {
		t.Errorf("expected '2+ results' in output, got: %s", rendered)
	}
}

func contains(s, sub string) bool {
	return len(s) >= len(sub) && containsStr(s, sub)
}

func containsStr(s, sub string) bool {
	for i := 0; i <= len(s)-len(sub); i++ {
		if s[i:i+len(sub)] == sub {
			return true
		}
	}
	return false
}

func TestViewReg_RegenerateRespectsLimits(t *testing.T) {
	db := setupTestDB(t)
	defer db.Close()

	exec := NewExecutor(db)
	exec.SetLimits(1, 5*time.Second)

	store := newMemStore()
	viewFile := "views/test.md"
	_ = store.Write(context.Background(), viewFile, []byte(
		"---\nkiwi-view: true\nkiwi-query: TABLE name\n---\n<!-- kiwi:auto -->\n",
	))

	// Insert the view into file_meta so Scan picks it up.
	fm, _ := json.Marshal(map[string]any{"kiwi-view": true, "kiwi-query": "TABLE name"})
	_, _ = db.Exec(`INSERT INTO file_meta(path, frontmatter, tasks, updated_at) VALUES (?, ?, ?, ?)`,
		viewFile, string(fm), "[]", "2026-04-24T12:00:00Z")

	reg := NewRegistry(exec, store)
	if err := reg.Scan(context.Background()); err != nil {
		t.Fatal(err)
	}
	reg.OnWrite("students/priya.md")

	changed, err := reg.RegenerateIfStale(context.Background(), viewFile)
	if err != nil {
		t.Fatal(err)
	}
	if !changed {
		t.Fatal("expected view to be regenerated")
	}

	content, _ := store.Read(context.Background(), viewFile)
	body := string(content)

	// maxScanRows=1 → only 1 data row in output (the over-fetch row is
	// stripped and HasMore is set, so we see "1+ results").
	lines := strings.Split(strings.TrimSpace(body), "\n")
	dataRows := 0
	for _, l := range lines {
		if strings.HasPrefix(l, "| ") && !strings.Contains(l, "---") {
			dataRows++
		}
	}
	// header row + 1 data row = 2 table rows
	if dataRows > 3 {
		t.Errorf("expected at most 3 table lines (header+sep+1 row) with maxScanRows=1, got %d data rows", dataRows)
	}
}

func TestRegenerateView_ListFormat(t *testing.T) {
	db := setupTestDB(t)
	defer db.Close()

	exec := NewExecutor(db)
	exec.SetLimits(10000, 5*time.Second)
	store := newMemStore()

	viewFile := "views/list.md"
	_ = store.Write(context.Background(), viewFile, []byte(
		"---\nkiwi-view: true\nkiwi-query: TABLE name\nkiwi-format: list\n---\n<!-- kiwi:auto -->\n",
	))

	changed, err := RegenerateView(context.Background(), store, exec, viewFile)
	if err != nil {
		t.Fatal(err)
	}
	if !changed {
		t.Fatal("expected view to change")
	}

	content, _ := store.Read(context.Background(), viewFile)
	body := string(content)
	if !strings.Contains(body, "- ") {
		t.Errorf("expected list format (lines starting with '- '), got:\n%s", body)
	}
	if strings.Contains(body, "| ") {
		t.Errorf("expected list format, but got table format with '| ':\n%s", body)
	}
}

func TestRegenerateView_DefaultsToTable(t *testing.T) {
	db := setupTestDB(t)
	defer db.Close()

	exec := NewExecutor(db)
	exec.SetLimits(10000, 5*time.Second)
	store := newMemStore()

	viewFile := "views/default.md"
	_ = store.Write(context.Background(), viewFile, []byte(
		"---\nkiwi-view: true\nkiwi-query: TABLE name\n---\n<!-- kiwi:auto -->\n",
	))

	changed, err := RegenerateView(context.Background(), store, exec, viewFile)
	if err != nil {
		t.Fatal(err)
	}
	if !changed {
		t.Fatal("expected view to change")
	}

	content, _ := store.Read(context.Background(), viewFile)
	body := string(content)
	if !strings.Contains(body, "| ") {
		t.Errorf("expected table format with '| ', got:\n%s", body)
	}
}

func TestIntegration_ColumnAlias(t *testing.T) {
	db := setupTestDB(t)
	defer db.Close()
	exec := NewExecutor(db)

	result, err := exec.Query(context.Background(),
		`TABLE name AS "Full Name", status AS State FROM "students/" SORT name ASC`, 0, 0)
	if err != nil {
		t.Fatal(err)
	}
	// Columns should use aliases
	found := false
	for _, col := range result.Columns {
		if col == "Full Name" {
			found = true
		}
	}
	if !found {
		t.Errorf("columns = %v, want 'Full Name' alias present", result.Columns)
	}
	// Rows should be keyed by alias
	if len(result.Rows) == 0 {
		t.Fatal("no rows")
	}
	if result.Rows[0]["Full Name"] == nil {
		t.Error("expected row keyed by alias 'Full Name'")
	}
}

func TestIntegration_WithoutID(t *testing.T) {
	db := setupTestDB(t)
	defer db.Close()
	exec := NewExecutor(db)

	result, err := exec.Query(context.Background(),
		`TABLE WITHOUT ID name, status FROM "students/" SORT name ASC`, 0, 0)
	if err != nil {
		t.Fatal(err)
	}
	// Columns should NOT include _path
	for _, col := range result.Columns {
		if col == "_path" {
			t.Error("expected no _path column with WITHOUT ID")
		}
	}
	// Rows should not have _path
	if len(result.Rows) == 0 {
		t.Fatal("no rows")
	}
	if result.Rows[0]["_path"] != nil {
		t.Error("expected no _path in rows with WITHOUT ID")
	}
}

func TestIntegration_MultipleWhere(t *testing.T) {
	db := setupTestDB(t)
	defer db.Close()
	exec := NewExecutor(db)

	result, err := exec.Query(context.Background(),
		`TABLE name WHERE status = "active" WHERE mastery.derivatives > 0.5`, 0, 0)
	if err != nil {
		t.Fatal(err)
	}
	// Only Amit has status=active AND derivatives > 0.5
	if len(result.Rows) != 1 {
		t.Fatalf("got %d rows, want 1", len(result.Rows))
	}
}

func TestIntegration_MultipleSort(t *testing.T) {
	db := setupTestDB(t)
	defer db.Close()
	exec := NewExecutor(db)

	result, err := exec.Query(context.Background(),
		`TABLE name, status FROM "students/" SORT status ASC SORT name ASC`, 0, 0)
	if err != nil {
		t.Fatal(err)
	}
	if len(result.Rows) != 3 {
		t.Fatalf("got %d rows, want 3", len(result.Rows))
	}
	// status ASC, then name ASC within same status:
	// active: Amit, Priya; inactive: Chen
	if result.Rows[0]["name"] != "Amit Patel" {
		t.Errorf("row[0].name = %v, want Amit Patel", result.Rows[0]["name"])
	}
	if result.Rows[1]["name"] != "Priya Sharma" {
		t.Errorf("row[1].name = %v, want Priya Sharma", result.Rows[1]["name"])
	}
	if result.Rows[2]["name"] != "Chen Wei" {
		t.Errorf("row[2].name = %v, want Chen Wei", result.Rows[2]["name"])
	}
}

func TestIntegration_FromTag(t *testing.T) {
	db := setupTestDB(t)
	defer db.Close()
	exec := NewExecutor(db)

	result, err := exec.Query(context.Background(),
		`TABLE name FROM #physics`, 0, 0)
	if err != nil {
		t.Fatal(err)
	}
	// Amit has [physics, math], Chen has [physics] → 2 files
	if len(result.Rows) != 2 {
		t.Fatalf("got %d rows, want 2 (files with #physics tag)", len(result.Rows))
	}
}

func TestIntegration_FromTagNegate(t *testing.T) {
	db := setupTestDB(t)
	defer db.Close()
	exec := NewExecutor(db)

	result, err := exec.Query(context.Background(),
		`TABLE name FROM #math AND -#physics`, 0, 0)
	if err != nil {
		t.Fatal(err)
	}
	// Priya has [math, calculus] (no physics), Derivatives has [math, calculus],
	// Integration has [math]. Amit has [physics, math] (excluded). → 3 files
	if len(result.Rows) != 3 {
		t.Fatalf("got %d rows, want 3 (math but not physics)", len(result.Rows))
	}
}

func TestIntegration_GroupByWithRows(t *testing.T) {
	db := setupTestDB(t)
	defer db.Close()
	exec := NewExecutor(db)

	result, err := exec.Query(context.Background(),
		`TABLE name GROUP BY status`, 0, 0)
	if err != nil {
		t.Fatal(err)
	}
	if len(result.Groups) < 2 {
		t.Fatalf("got %d groups, want at least 2", len(result.Groups))
	}
	// Each group should have rows
	for _, g := range result.Groups {
		if len(g.Rows) == 0 {
			t.Errorf("group %q has no rows", g.Key)
		}
		if g.Count != len(g.Rows) {
			t.Errorf("group %q: count=%d but rows=%d", g.Key, g.Count, len(g.Rows))
		}
	}
}

func TestIntegration_Calendar(t *testing.T) {
	db := setupTestDB(t)
	defer db.Close()
	exec := NewExecutor(db)

	result, err := exec.Query(context.Background(),
		`CALENDAR last_active FROM "students/"`, 0, 0)
	if err != nil {
		t.Fatal(err)
	}
	if len(result.Rows) != 3 {
		t.Fatalf("got %d rows, want 3", len(result.Rows))
	}
	found := false
	for _, col := range result.Columns {
		if col == "last_active" {
			found = true
		}
	}
	if !found {
		t.Errorf("columns = %v, want 'last_active' present", result.Columns)
	}
	val, ok := result.Rows[0]["last_active"].(string)
	if !ok || val == "" {
		t.Errorf("expected non-empty last_active string, got %v", result.Rows[0]["last_active"])
	}
}

func TestParseQuery_ComputedColumn(t *testing.T) {
	plan, err := ParseQuery(`TABLE days_since(last_active) AS "Idle", name`)
	if err != nil {
		t.Fatal(err)
	}
	if len(plan.Fields) != 2 {
		t.Fatalf("got %d fields, want 2", len(plan.Fields))
	}
	if plan.Fields[0].Parsed == nil {
		t.Error("expected parsed expression for computed column")
	}
	if plan.Fields[0].Alias != "Idle" {
		t.Errorf("alias = %q, want Idle", plan.Fields[0].Alias)
	}
	if plan.Fields[1].Parsed != nil {
		t.Error("expected nil parsed for simple field 'name'")
	}
}

func TestIntegration_ComputedColumn(t *testing.T) {
	db := setupTestDB(t)
	defer db.Close()
	exec := NewExecutor(db)

	result, err := exec.Query(context.Background(),
		`TABLE round(mastery.derivatives, 1) AS "Score", name FROM "students/" SORT name ASC`, 0, 0)
	if err != nil {
		t.Fatal(err)
	}
	if len(result.Rows) != 3 {
		t.Fatalf("got %d rows, want 3", len(result.Rows))
	}
	// Check that the computed column uses the alias
	found := false
	for _, col := range result.Columns {
		if col == "Score" {
			found = true
		}
	}
	if !found {
		t.Errorf("columns = %v, want 'Score' present", result.Columns)
	}
	// SORT name ASC → [Amit(0.7), Chen(0.9), Priya(0.1)]
	amit := result.Rows[0]["Score"]
	if amit != 0.7 {
		t.Errorf("Amit Score = %v, want 0.7", amit)
	}
	priya := result.Rows[2]["Score"]
	if priya != 0.1 {
		t.Errorf("Priya Score = %v, want 0.1", priya)
	}
}

func TestIntegration_FuncChoice(t *testing.T) {
	db := setupTestDB(t)
	defer db.Close()
	exec := NewExecutor(db)

	result, err := exec.Query(context.Background(),
		`TABLE name WHERE choice(status = "active", true, false) FROM "students/"`, 0, 0)
	if err != nil {
		t.Fatal(err)
	}
	if len(result.Rows) != 2 {
		t.Fatalf("got %d rows, want 2", len(result.Rows))
	}
}

func TestIntegration_FuncReplace(t *testing.T) {
	db := setupTestDB(t)
	defer db.Close()
	exec := NewExecutor(db)

	result, err := exec.Query(context.Background(),
		`TABLE name WHERE replace(name, "Priya", "X") = "X Sharma"`, 0, 0)
	if err != nil {
		t.Fatal(err)
	}
	if len(result.Rows) != 1 {
		t.Fatalf("got %d rows, want 1", len(result.Rows))
	}
}

func TestIntegration_FuncRound(t *testing.T) {
	db := setupTestDB(t)
	defer db.Close()
	exec := NewExecutor(db)

	result, err := exec.Query(context.Background(),
		`TABLE name WHERE round(mastery.derivatives, 0) = 1 FROM "students/"`, 0, 0)
	if err != nil {
		t.Fatal(err)
	}
	// Amit has 0.7→1, Chen has 0.9→1
	if len(result.Rows) != 2 {
		t.Fatalf("got %d rows, want 2", len(result.Rows))
	}
}

func TestIntegration_FuncNumber(t *testing.T) {
	db := setupTestDB(t)
	defer db.Close()
	exec := NewExecutor(db)

	result, err := exec.Query(context.Background(),
		`TABLE name WHERE number(mastery.derivatives) > 0.5 FROM "students/"`, 0, 0)
	if err != nil {
		t.Fatal(err)
	}
	// Amit 0.7 + Chen 0.9 = 2
	if len(result.Rows) != 2 {
		t.Fatalf("got %d rows, want 2", len(result.Rows))
	}
}

func TestIntegration_FuncDateFormat(t *testing.T) {
	db := setupTestDB(t)
	defer db.Close()
	exec := NewExecutor(db)

	result, err := exec.Query(context.Background(),
		`TABLE name WHERE dateformat(last_active, "%Y") = "2026" FROM "students/"`, 0, 0)
	if err != nil {
		t.Fatal(err)
	}
	if len(result.Rows) != 3 {
		t.Fatalf("got %d rows, want 3", len(result.Rows))
	}
}

func TestIntegration_FuncStripTime(t *testing.T) {
	db := setupTestDB(t)
	defer db.Close()
	exec := NewExecutor(db)

	result, err := exec.Query(context.Background(),
		`TABLE name WHERE striptime(last_active) = "2026-04-24" FROM "students/"`, 0, 0)
	if err != nil {
		t.Fatal(err)
	}
	if len(result.Rows) != 1 {
		t.Fatalf("got %d rows, want 1 (Priya)", len(result.Rows))
	}
}

func TestIntegration_FuncSubstring(t *testing.T) {
	db := setupTestDB(t)
	defer db.Close()
	exec := NewExecutor(db)

	result, err := exec.Query(context.Background(),
		`TABLE name WHERE substring(name, 1, 4) = "Priy" FROM "students/"`, 0, 0)
	if err != nil {
		t.Fatal(err)
	}
	if len(result.Rows) != 1 {
		t.Fatalf("got %d rows, want 1", len(result.Rows))
	}
}

func TestParseQuery_Task(t *testing.T) {
	plan, err := ParseQuery(`TASK FROM "projects/"`)
	if err != nil {
		t.Fatal(err)
	}
	if plan.Type != "task" {
		t.Errorf("type = %q, want task", plan.Type)
	}
	if plan.From != "projects/" {
		t.Errorf("from = %q, want projects/", plan.From)
	}
}

func TestIntegration_TaskQuery(t *testing.T) {
	db := setupTestDB(t)
	defer db.Close()
	exec := NewExecutor(db)

	result, err := exec.Query(context.Background(), `TASK`, 0, 0)
	if err != nil {
		t.Fatal(err)
	}
	if len(result.Rows) != 3 {
		t.Fatalf("got %d tasks, want 3", len(result.Rows))
	}
	// Verify task fields
	first := result.Rows[0]
	if first["text"] == nil {
		t.Error("expected text field in task row")
	}
	if first["_path"] == nil {
		t.Error("expected _path field in task row")
	}
}

func TestIntegration_TaskWhereCompleted(t *testing.T) {
	db := setupTestDB(t)
	defer db.Close()
	exec := NewExecutor(db)

	result, err := exec.Query(context.Background(),
		`TASK WHERE completed = false`, 0, 0)
	if err != nil {
		t.Fatal(err)
	}
	// 2 uncompleted tasks: "Send email" and "Read chapter 3"
	if len(result.Rows) != 2 {
		t.Fatalf("got %d rows, want 2 uncompleted tasks", len(result.Rows))
	}
}

func TestIntegration_TaskRender(t *testing.T) {
	db := setupTestDB(t)
	defer db.Close()
	exec := NewExecutor(db)

	result, err := exec.Query(context.Background(), `TASK`, 0, 0)
	if err != nil {
		t.Fatal(err)
	}
	rendered := Render(result, "task")
	if !strings.Contains(rendered, "[x]") {
		t.Errorf("expected [x] in task render, got:\n%s", rendered)
	}
	if !strings.Contains(rendered, "[ ]") {
		t.Errorf("expected [ ] in task render, got:\n%s", rendered)
	}
}

func TestIntegration_FuncNonNull(t *testing.T) {
	db := setupTestDB(t)
	defer db.Close()
	exec := NewExecutor(db)

	result, err := exec.Query(context.Background(),
		`TABLE name WHERE nonnull(difficulty)`, 0, 0)
	if err != nil {
		t.Fatal(err)
	}
	if len(result.Rows) != 1 {
		t.Fatalf("got %d rows, want 1 (derivatives.md)", len(result.Rows))
	}
}

func TestIntegration_TaskWhereLike(t *testing.T) {
	db := setupTestDB(t)
	defer db.Close()
	exec := NewExecutor(db)

	result, err := exec.Query(context.Background(),
		`TASK WHERE text LIKE "%groceries%"`, 0, 0)
	if err != nil {
		t.Fatal(err)
	}
	if len(result.Rows) != 1 {
		t.Fatalf("got %d rows, want 1", len(result.Rows))
	}
	if result.Rows[0]["text"] != "Buy groceries" {
		t.Errorf("text = %v, want Buy groceries", result.Rows[0]["text"])
	}
}

func TestIntegration_TaskWhereComparison(t *testing.T) {
	db := setupTestDB(t)
	defer db.Close()
	exec := NewExecutor(db)

	result, err := exec.Query(context.Background(),
		`TASK WHERE line > 3`, 0, 0)
	if err != nil {
		t.Fatal(err)
	}
	// line 4 ("Send email") and line 5 ("Read chapter 3")
	if len(result.Rows) != 2 {
		t.Fatalf("got %d rows, want 2 (tasks on lines 4+)", len(result.Rows))
	}
}

func TestIntegration_TaskWhereTag(t *testing.T) {
	db := setupTestDB(t)
	defer db.Close()
	exec := NewExecutor(db)

	result, err := exec.Query(context.Background(),
		`TASK WHERE contains(tags, "shopping")`, 0, 0)
	if err != nil {
		t.Fatal(err)
	}
	if len(result.Rows) != 1 {
		t.Fatalf("got %d rows, want 1", len(result.Rows))
	}
	if result.Rows[0]["text"] != "Buy groceries" {
		t.Errorf("text = %v, want Buy groceries", result.Rows[0]["text"])
	}
}

func TestIntegration_RegexReplace(t *testing.T) {
	db := setupTestDB(t)
	defer db.Close()
	exec := NewExecutor(db)

	result, err := exec.Query(context.Background(),
		`TABLE regexreplace(name, "\\s+.*", "") AS "First" FROM "students/" SORT name ASC`, 0, 0)
	if err != nil {
		t.Fatal(err)
	}
	if len(result.Rows) != 3 {
		t.Fatalf("got %d rows, want 3", len(result.Rows))
	}
	first := result.Rows[0]["First"]
	if first != "Amit" {
		t.Errorf("First = %v, want Amit", first)
	}
}

func TestIntegration_ExtField(t *testing.T) {
	db := setupTestDB(t)
	defer db.Close()

	// Insert a .json file
	fm, _ := json.Marshal(map[string]any{"name": "Config"})
	_, err := db.Exec(`INSERT INTO file_meta(path, frontmatter, tasks, updated_at) VALUES (?, ?, ?, ?)`,
		"config/settings.json", string(fm), "[]", "2026-04-24T12:00:00Z")
	if err != nil {
		t.Fatal(err)
	}

	exec := NewExecutor(db)
	result, err := exec.Query(context.Background(),
		`TABLE _ext WHERE _path = "config/settings.json"`, 0, 0)
	if err != nil {
		t.Fatal(err)
	}
	if len(result.Rows) != 1 {
		t.Fatalf("got %d rows, want 1", len(result.Rows))
	}
	ext := result.Rows[0]["_ext"]
	if ext != ".json" {
		t.Errorf("_ext = %v, want .json", ext)
	}

	// Also verify .md still works
	result2, err := exec.Query(context.Background(),
		`TABLE _ext WHERE _path = "students/priya.md"`, 0, 0)
	if err != nil {
		t.Fatal(err)
	}
	if len(result2.Rows) != 1 {
		t.Fatalf("got %d rows, want 1", len(result2.Rows))
	}
	if result2.Rows[0]["_ext"] != ".md" {
		t.Errorf("_ext = %v, want .md", result2.Rows[0]["_ext"])
	}
}

func TestRegistry_TagViewInvalidation(t *testing.T) {
	db := setupTestDB(t)
	defer db.Close()

	exec := NewExecutor(db)
	store := newMemStore()
	reg := NewRegistry(exec, store)

	plan := &QueryPlan{
		Type:     "table",
		FromTags: []TagFilter{{Tag: "physics"}},
		Fields:   []FieldSpec{{Expr: "name"}},
		Limit:    50,
	}
	reg.Register("views/physics.md", plan)
	reg.MarkFresh("views/physics.md")

	if reg.IsStale("views/physics.md") {
		t.Fatal("view should start fresh")
	}

	// Write a file in an unrelated folder — tag views should still invalidate
	reg.OnWrite("notes/random.md")

	if !reg.IsStale("views/physics.md") {
		t.Error("tag-scoped view should be stale after any write")
	}
}

func TestRegenerateView_PreservesContentAfterMarker(t *testing.T) {
	db := setupTestDB(t)
	defer db.Close()

	exec := NewExecutor(db)
	exec.SetLimits(10000, 5*time.Second)
	store := newMemStore()

	viewFile := "views/preserve.md"
	_ = store.Write(context.Background(), viewFile, []byte(
		"---\nkiwi-view: true\nkiwi-query: TABLE name FROM \"students/\"\n---\n"+
			"<!-- kiwi:auto -->\nold content\n<!-- /kiwi-view -->\n\n## Notes\n\nThis should survive regeneration.\n",
	))

	changed, err := RegenerateView(context.Background(), store, exec, viewFile)
	if err != nil {
		t.Fatal(err)
	}
	if !changed {
		t.Fatal("expected view to change")
	}

	content, _ := store.Read(context.Background(), viewFile)
	body := string(content)
	if !strings.Contains(body, "## Notes") {
		t.Errorf("content after end marker was lost:\n%s", body)
	}
	if !strings.Contains(body, "This should survive regeneration.") {
		t.Errorf("prose below end marker was lost:\n%s", body)
	}
	if strings.Contains(body, "old content") {
		t.Errorf("old content between markers was not replaced:\n%s", body)
	}
}

func TestRegenerateView_BackwardCompat_NoEndMarker(t *testing.T) {
	db := setupTestDB(t)
	defer db.Close()

	exec := NewExecutor(db)
	exec.SetLimits(10000, 5*time.Second)
	store := newMemStore()

	viewFile := "views/compat.md"
	_ = store.Write(context.Background(), viewFile, []byte(
		"---\nkiwi-view: true\nkiwi-query: TABLE name FROM \"students/\"\n---\n"+
			"<!-- kiwi:auto -->\nold stuff\n",
	))

	changed, err := RegenerateView(context.Background(), store, exec, viewFile)
	if err != nil {
		t.Fatal(err)
	}
	if !changed {
		t.Fatal("expected view to change")
	}

	content, _ := store.Read(context.Background(), viewFile)
	body := string(content)
	if !strings.Contains(body, "<!-- /kiwi-view -->") {
		t.Error("expected end marker to be added")
	}
	if strings.Contains(body, "old stuff") {
		t.Error("old content should be replaced")
	}
}

func TestCollectFields_MultiSort(t *testing.T) {
	plan := &QueryPlan{
		Type: "table",
		Sorts: []SortSpec{
			{Field: "status", Order: "asc"},
			{Field: "name", Order: "asc"},
		},
		Limit: 50,
	}
	fields := CollectFields(plan)
	want := map[string]bool{"status": true, "name": true}
	for _, f := range fields {
		delete(want, f)
	}
	if len(want) > 0 {
		t.Errorf("missing fields from CollectFields: %v", want)
	}
}

func TestIntegration_TaskWhereMeta(t *testing.T) {
	db := setupTestDB(t)
	defer db.Close()
	exec := NewExecutor(db)

	result, err := exec.Query(context.Background(),
		`TASK WHERE meta.priority = 1`, 0, 0)
	if err != nil {
		t.Fatal(err)
	}
	// "Buy groceries" (priority 1) and "Read chapter 3" (priority 1)
	if len(result.Rows) != 2 {
		t.Fatalf("got %d rows, want 2", len(result.Rows))
	}
}

func TestIntegration_RegexTest(t *testing.T) {
	db := setupTestDB(t)
	defer db.Close()
	exec := NewExecutor(db)

	result, err := exec.Query(context.Background(),
		`TABLE name WHERE regextest("^A.*", name) FROM "students/"`, 0, 0)
	if err != nil {
		t.Fatal(err)
	}
	if len(result.Rows) != 1 {
		t.Fatalf("got %d rows, want 1 (only Amit)", len(result.Rows))
	}
	if result.Rows[0]["name"] != "Amit Patel" {
		t.Errorf("name = %v, want Amit Patel", result.Rows[0]["name"])
	}
}

func TestIntegration_TaskWhereBetween(t *testing.T) {
	db := setupTestDB(t)
	defer db.Close()
	exec := NewExecutor(db)

	result, err := exec.Query(context.Background(),
		`TASK WHERE line BETWEEN 3 AND 4`, 0, 0)
	if err != nil {
		t.Fatal(err)
	}
	// line 3 ("Buy groceries") and line 4 ("Send email")
	if len(result.Rows) != 2 {
		t.Fatalf("got %d rows, want 2", len(result.Rows))
	}
}

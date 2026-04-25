package importer

import (
	"context"
	"database/sql"
	"encoding/json"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/kiwifs/kiwifs/internal/events"
	"github.com/kiwifs/kiwifs/internal/pipeline"
	"github.com/kiwifs/kiwifs/internal/search"
	"github.com/kiwifs/kiwifs/internal/storage"
	"github.com/kiwifs/kiwifs/internal/versioning"
	"github.com/xuri/excelize/v2"
)

func testPipeline(t *testing.T) (*pipeline.Pipeline, storage.Storage) {
	t.Helper()
	dir := t.TempDir()
	store, err := storage.NewLocal(dir)
	if err != nil {
		t.Fatalf("storage: %v", err)
	}
	ver := versioning.NewNoop()
	searcher := search.NewGrep(dir)
	hub := events.NewHub()
	pipe := pipeline.New(store, ver, searcher, nil, hub, nil, "")
	return pipe, store
}

func TestCSVImport(t *testing.T) {
	csvData := "name,age,grade\nAlice,18,A\nBob,19,B\nCharlie,20,A\n"
	csvFile := filepath.Join(t.TempDir(), "students.csv")
	if err := os.WriteFile(csvFile, []byte(csvData), 0644); err != nil {
		t.Fatalf("write csv: %v", err)
	}

	src, err := NewCSV(csvFile, true)
	if err != nil {
		t.Fatalf("new csv: %v", err)
	}
	defer src.Close()

	pipe, store := testPipeline(t)
	ctx := context.Background()
	stats, err := Run(ctx, src, pipe, Options{Actor: "test"})
	if err != nil {
		t.Fatalf("run: %v", err)
	}
	if stats.Imported != 3 {
		t.Fatalf("imported=%d, want 3", stats.Imported)
	}

	// Verify files exist with correct frontmatter.
	content, err := store.Read(ctx, "students/0.md")
	if err != nil {
		t.Fatalf("read: %v", err)
	}
	s := string(content)
	if !strings.Contains(s, "name: Alice") {
		t.Fatalf("missing Alice in frontmatter: %s", s)
	}
	if !strings.Contains(s, "_source: students") {
		t.Fatalf("missing _source: %s", s)
	}
	if !strings.Contains(s, "Auto-imported from students") {
		t.Fatalf("missing auto-imported line: %s", s)
	}
}

func TestJSONImport(t *testing.T) {
	data := `[
		{"id": "s1", "name": "Alice", "score": 95},
		{"id": "s2", "name": "Bob", "score": 88}
	]`
	jsonFile := filepath.Join(t.TempDir(), "data.json")
	if err := os.WriteFile(jsonFile, []byte(data), 0644); err != nil {
		t.Fatalf("write json: %v", err)
	}

	src, err := NewJSON(jsonFile)
	if err != nil {
		t.Fatalf("new json: %v", err)
	}
	defer src.Close()

	pipe, store := testPipeline(t)
	ctx := context.Background()
	stats, err := Run(ctx, src, pipe, Options{Actor: "test"})
	if err != nil {
		t.Fatalf("run: %v", err)
	}
	if stats.Imported != 2 {
		t.Fatalf("imported=%d, want 2", stats.Imported)
	}

	content, err := store.Read(ctx, "data/s1.md")
	if err != nil {
		t.Fatalf("read: %v", err)
	}
	if !strings.Contains(string(content), "name: Alice") {
		t.Fatalf("missing Alice: %s", content)
	}
}

func TestJSONLImport(t *testing.T) {
	data := `{"id": "a", "name": "X"}
{"id": "b", "name": "Y"}`
	jsonlFile := filepath.Join(t.TempDir(), "data.jsonl")
	if err := os.WriteFile(jsonlFile, []byte(data), 0644); err != nil {
		t.Fatalf("write jsonl: %v", err)
	}

	src, err := NewJSON(jsonlFile)
	if err != nil {
		t.Fatalf("new json: %v", err)
	}
	defer src.Close()

	pipe, _ := testPipeline(t)
	ctx := context.Background()
	stats, err := Run(ctx, src, pipe, Options{Actor: "test"})
	if err != nil {
		t.Fatalf("run: %v", err)
	}
	if stats.Imported != 2 {
		t.Fatalf("imported=%d, want 2", stats.Imported)
	}
}

func TestSQLiteImport(t *testing.T) {
	dbPath := filepath.Join(t.TempDir(), "test.db")
	db, err := sql.Open("sqlite", dbPath)
	if err != nil {
		t.Fatalf("open db: %v", err)
	}
	defer db.Close()

	_, err = db.Exec(`CREATE TABLE students (id INTEGER PRIMARY KEY, name TEXT, grade TEXT)`)
	if err != nil {
		t.Fatalf("create table: %v", err)
	}
	_, err = db.Exec(`INSERT INTO students VALUES (1, 'Alice', 'A'), (2, 'Bob', 'B'), (3, 'Charlie', 'C')`)
	if err != nil {
		t.Fatalf("insert: %v", err)
	}
	db.Close()

	src, err := NewSQLiteSource(dbPath, "students", "")
	if err != nil {
		t.Fatalf("new sqlite: %v", err)
	}
	defer src.Close()

	pipe, store := testPipeline(t)
	ctx := context.Background()
	stats, err := Run(ctx, src, pipe, Options{Actor: "test"})
	if err != nil {
		t.Fatalf("run: %v", err)
	}
	if stats.Imported != 3 {
		t.Fatalf("imported=%d, want 3", stats.Imported)
	}

	content, err := store.Read(ctx, "students/1.md")
	if err != nil {
		t.Fatalf("read: %v", err)
	}
	if !strings.Contains(string(content), "name: Alice") {
		t.Fatalf("missing Alice: %s", content)
	}
}

func TestIdempotentReImport(t *testing.T) {
	data := `[{"id": "x", "name": "Test"}]`
	jsonFile := filepath.Join(t.TempDir(), "data.json")
	if err := os.WriteFile(jsonFile, []byte(data), 0644); err != nil {
		t.Fatalf("write json: %v", err)
	}

	pipe, _ := testPipeline(t)
	ctx := context.Background()

	// First import.
	src1, _ := NewJSON(jsonFile)
	stats1, err := Run(ctx, src1, pipe, Options{Actor: "test"})
	src1.Close()
	if err != nil {
		t.Fatalf("first run: %v", err)
	}
	if stats1.Imported != 1 {
		t.Fatalf("first imported=%d, want 1", stats1.Imported)
	}

	// Second import — same data, should be skipped.
	src2, _ := NewJSON(jsonFile)
	stats2, err := Run(ctx, src2, pipe, Options{Actor: "test"})
	src2.Close()
	if err != nil {
		t.Fatalf("second run: %v", err)
	}
	if stats2.Skipped != 1 {
		t.Fatalf("second skipped=%d, want 1", stats2.Skipped)
	}
	if stats2.Imported != 0 {
		t.Fatalf("second imported=%d, want 0", stats2.Imported)
	}
}

func TestDryRun(t *testing.T) {
	data := `[{"id": "1", "name": "A"}, {"id": "2", "name": "B"}]`
	jsonFile := filepath.Join(t.TempDir(), "data.json")
	if err := os.WriteFile(jsonFile, []byte(data), 0644); err != nil {
		t.Fatalf("write: %v", err)
	}

	pipe, store := testPipeline(t)
	ctx := context.Background()

	src, _ := NewJSON(jsonFile)
	stats, err := Run(ctx, src, pipe, Options{Actor: "test", DryRun: true})
	src.Close()
	if err != nil {
		t.Fatalf("run: %v", err)
	}
	if stats.Imported != 2 {
		t.Fatalf("imported=%d, want 2", stats.Imported)
	}

	// No files should have been written.
	if store.Exists(ctx, "data/1.md") {
		t.Fatalf("file exists in dry-run mode")
	}
}

func TestColumnsFilter(t *testing.T) {
	data := `[{"id": "1", "name": "Alice", "secret": "hidden", "grade": "A"}]`
	jsonFile := filepath.Join(t.TempDir(), "data.json")
	if err := os.WriteFile(jsonFile, []byte(data), 0644); err != nil {
		t.Fatalf("write: %v", err)
	}

	pipe, store := testPipeline(t)
	ctx := context.Background()

	src, _ := NewJSON(jsonFile)
	stats, err := Run(ctx, src, pipe, Options{
		Actor:   "test",
		Columns: []string{"name", "grade"},
	})
	src.Close()
	if err != nil {
		t.Fatalf("run: %v", err)
	}
	if stats.Imported != 1 {
		t.Fatalf("imported=%d, want 1", stats.Imported)
	}

	content, err := store.Read(ctx, "data/1.md")
	if err != nil {
		t.Fatalf("read: %v", err)
	}
	s := string(content)
	if strings.Contains(s, "secret") {
		t.Fatalf("secret field should be filtered out: %s", s)
	}
	if !strings.Contains(s, "name: Alice") {
		t.Fatalf("name should be present: %s", s)
	}
}

func TestLimit(t *testing.T) {
	data := `[{"id":"1"},{"id":"2"},{"id":"3"},{"id":"4"},{"id":"5"}]`
	jsonFile := filepath.Join(t.TempDir(), "data.json")
	if err := os.WriteFile(jsonFile, []byte(data), 0644); err != nil {
		t.Fatalf("write: %v", err)
	}

	pipe, _ := testPipeline(t)
	ctx := context.Background()

	src, _ := NewJSON(jsonFile)
	stats, err := Run(ctx, src, pipe, Options{Actor: "test", Limit: 2})
	src.Close()
	if err != nil {
		t.Fatalf("run: %v", err)
	}
	if stats.Imported != 2 {
		t.Fatalf("imported=%d, want 2", stats.Imported)
	}
}

func TestPostgresSkipWithoutEnv(t *testing.T) {
	if os.Getenv("KIWI_TEST_POSTGRES_DSN") == "" {
		t.Skip("skipping postgres integration test (set KIWI_TEST_POSTGRES_DSN)")
	}
}

func TestMySQLSkipWithoutEnv(t *testing.T) {
	if os.Getenv("KIWI_TEST_MYSQL_DSN") == "" {
		t.Skip("skipping mysql integration test (set KIWI_TEST_MYSQL_DSN)")
	}
}

func TestFirestoreSkipWithoutEnv(t *testing.T) {
	if os.Getenv("GOOGLE_APPLICATION_CREDENTIALS") == "" {
		t.Skip("skipping firestore integration test (set GOOGLE_APPLICATION_CREDENTIALS)")
	}
}

func TestMongoDBSkipWithoutEnv(t *testing.T) {
	if os.Getenv("KIWI_TEST_MONGODB_URI") == "" {
		t.Skip("skipping mongodb integration test (set KIWI_TEST_MONGODB_URI)")
	}
}

// Verify JSON source auto-detects array vs JSONL format.
func TestJSONAutoDetect(t *testing.T) {
	// Array format
	arrayFile := filepath.Join(t.TempDir(), "array.json")
	os.WriteFile(arrayFile, []byte(`[{"id":"1"}]`), 0644)
	src1, _ := NewJSON(arrayFile)
	ch1, _ := src1.Stream(context.Background())
	count := 0
	for range ch1 {
		count++
	}
	if count != 1 {
		t.Fatalf("array format: got %d, want 1", count)
	}

	// JSONL format
	jsonlFile := filepath.Join(t.TempDir(), "data.jsonl")
	os.WriteFile(jsonlFile, []byte("{\"id\":\"1\"}\n{\"id\":\"2\"}\n"), 0644)
	src2, _ := NewJSON(jsonlFile)
	ch2, _ := src2.Stream(context.Background())
	count = 0
	for range ch2 {
		count++
	}
	if count != 2 {
		t.Fatalf("jsonl format: got %d, want 2", count)
	}
}

// Ensure CSVSource auto-detects numeric columns.
func TestCSVNumericDetection(t *testing.T) {
	csvData := "name,score,grade\nAlice,95,A\nBob,88,B\n"
	csvFile := filepath.Join(t.TempDir(), "scores.csv")
	os.WriteFile(csvFile, []byte(csvData), 0644)

	src, _ := NewCSV(csvFile, true)
	ch, _ := src.Stream(context.Background())

	rec := <-ch
	if _, ok := rec.Fields["score"].(int64); !ok {
		t.Fatalf("score should be int64, got %T: %v", rec.Fields["score"], rec.Fields["score"])
	}
	if _, ok := rec.Fields["name"].(string); !ok {
		t.Fatalf("name should be string, got %T", rec.Fields["name"])
	}
}

func TestExcelImport(t *testing.T) {
	f := excelize.NewFile()
	defer f.Close()

	sheet := "Sheet1"
	f.SetCellValue(sheet, "A1", "name")
	f.SetCellValue(sheet, "B1", "score")
	f.SetCellValue(sheet, "C1", "grade")
	f.SetCellValue(sheet, "A2", "Alice")
	f.SetCellValue(sheet, "B2", 95)
	f.SetCellValue(sheet, "C2", "A")
	f.SetCellValue(sheet, "A3", "Bob")
	f.SetCellValue(sheet, "B3", 88)
	f.SetCellValue(sheet, "C3", "B")

	xlsxPath := filepath.Join(t.TempDir(), "students.xlsx")
	if err := f.SaveAs(xlsxPath); err != nil {
		t.Fatalf("save xlsx: %v", err)
	}

	src, err := NewExcel(xlsxPath, "")
	if err != nil {
		t.Fatalf("new excel: %v", err)
	}
	defer src.Close()

	pipe, store := testPipeline(t)
	ctx := context.Background()
	stats, err := Run(ctx, src, pipe, Options{Actor: "test"})
	if err != nil {
		t.Fatalf("run: %v", err)
	}
	if stats.Imported != 2 {
		t.Fatalf("imported=%d, want 2", stats.Imported)
	}

	content, err := store.Read(ctx, "students/0.md")
	if err != nil {
		t.Fatalf("read: %v", err)
	}
	s := string(content)
	if !strings.Contains(s, "name: Alice") {
		t.Fatalf("missing Alice: %s", s)
	}
	if !strings.Contains(s, "_source: students") {
		t.Fatalf("missing _source: %s", s)
	}
}

func TestExcelWithSheetName(t *testing.T) {
	f := excelize.NewFile()
	defer f.Close()

	f.NewSheet("Data")
	f.SetCellValue("Data", "A1", "id")
	f.SetCellValue("Data", "B1", "value")
	f.SetCellValue("Data", "A2", "1")
	f.SetCellValue("Data", "B2", "hello")

	xlsxPath := filepath.Join(t.TempDir(), "test.xlsx")
	f.SaveAs(xlsxPath)

	src, _ := NewExcel(xlsxPath, "Data")
	ch, _ := src.Stream(context.Background())
	count := 0
	for range ch {
		count++
	}
	if count != 1 {
		t.Fatalf("got %d records, want 1", count)
	}
}

func TestYAMLArrayImport(t *testing.T) {
	data := `- name: Alice
  grade: 10
- name: Bob
  grade: 11
- name: Charlie
  grade: 12
`
	yamlFile := filepath.Join(t.TempDir(), "data.yaml")
	os.WriteFile(yamlFile, []byte(data), 0644)

	src, err := NewYAML(yamlFile)
	if err != nil {
		t.Fatalf("new yaml: %v", err)
	}
	defer src.Close()

	pipe, store := testPipeline(t)
	ctx := context.Background()
	stats, err := Run(ctx, src, pipe, Options{Actor: "test"})
	if err != nil {
		t.Fatalf("run: %v", err)
	}
	if stats.Imported != 3 {
		t.Fatalf("imported=%d, want 3", stats.Imported)
	}

	content, err := store.Read(ctx, "data/0.md")
	if err != nil {
		t.Fatalf("read: %v", err)
	}
	if !strings.Contains(string(content), "name: Alice") {
		t.Fatalf("missing Alice: %s", content)
	}
}

func TestYAMLNestedImport(t *testing.T) {
	data := `students:
  - name: Priya
    grade: 10
  - name: Raj
    grade: 11
`
	yamlFile := filepath.Join(t.TempDir(), "nested.yaml")
	os.WriteFile(yamlFile, []byte(data), 0644)

	src, _ := NewYAML(yamlFile)
	ch, _ := src.Stream(context.Background())
	var recs []Record
	for r := range ch {
		recs = append(recs, r)
	}
	if len(recs) != 2 {
		t.Fatalf("got %d records, want 2", len(recs))
	}
	if recs[0].Fields["name"] != "Priya" {
		t.Fatalf("first record name=%v, want Priya", recs[0].Fields["name"])
	}
}

func TestObsidianImport(t *testing.T) {
	vault := t.TempDir()

	// Create a note with wiki-links and embeds.
	os.MkdirAll(filepath.Join(vault, "notes"), 0755)
	note := `---
title: Test Note
tags: [test, demo]
---

# Hello

This links to [[Other Page]] and [[Page Name|alias]].

Here is an embed: ![[image.png]]
`
	os.WriteFile(filepath.Join(vault, "notes", "test.md"), []byte(note), 0644)

	// Create .obsidian dir that should be skipped.
	os.MkdirAll(filepath.Join(vault, ".obsidian"), 0755)
	os.WriteFile(filepath.Join(vault, ".obsidian", "config.json"), []byte("{}"), 0644)

	// Create an attachment.
	os.MkdirAll(filepath.Join(vault, "attachments"), 0755)
	os.WriteFile(filepath.Join(vault, "attachments", "image.png"), []byte("fakepng"), 0644)

	src, err := NewObsidian(vault)
	if err != nil {
		t.Fatalf("new obsidian: %v", err)
	}
	defer src.Close()

	ch, errs := src.Stream(context.Background())
	var recs []Record
	for r := range ch {
		recs = append(recs, r)
	}
	for err := range errs {
		if err != nil {
			t.Fatalf("stream error: %v", err)
		}
	}

	if len(recs) != 1 {
		t.Fatalf("got %d records, want 1", len(recs))
	}

	raw, ok := recs[0].Fields["_raw_content"].(string)
	if !ok {
		t.Fatalf("no _raw_content field")
	}

	if !strings.Contains(raw, "[[other-page]]") {
		t.Fatalf("wiki-link not rewritten: %s", raw)
	}
	if !strings.Contains(raw, "[[page-name|alias]]") {
		t.Fatalf("aliased wiki-link not rewritten: %s", raw)
	}
	if !strings.Contains(raw, "![image](assets/image.png)") {
		t.Fatalf("embed not rewritten: %s", raw)
	}
}

func TestObsidianSkipsDotObsidian(t *testing.T) {
	vault := t.TempDir()
	os.MkdirAll(filepath.Join(vault, ".obsidian"), 0755)
	os.WriteFile(filepath.Join(vault, ".obsidian", "app.json"), []byte("{}"), 0644)
	os.WriteFile(filepath.Join(vault, "note.md"), []byte("# Hi\n"), 0644)

	src, _ := NewObsidian(vault)
	ch, _ := src.Stream(context.Background())
	count := 0
	for range ch {
		count++
	}
	if count != 1 {
		t.Fatalf("got %d records (should skip .obsidian), want 1", count)
	}
}

func TestConfluenceImport(t *testing.T) {
	exportDir := t.TempDir()

	htmlContent := `<!DOCTYPE html>
<html>
<head>
<title>Test Page</title>
<meta name="author" content="John">
</head>
<body>
<h1>Welcome</h1>
<p>This is a <strong>test</strong> page.</p>
<ul>
<li>Item 1</li>
<li>Item 2</li>
</ul>
<table>
<tr><th>Name</th><th>Score</th></tr>
<tr><td>Alice</td><td>95</td></tr>
</table>
<pre><code>fmt.Println("hello")</code></pre>
<p>Visit <a href="https://example.com">here</a>.</p>
</body>
</html>`
	os.WriteFile(filepath.Join(exportDir, "page.html"), []byte(htmlContent), 0644)

	src, err := NewConfluence(exportDir)
	if err != nil {
		t.Fatalf("new confluence: %v", err)
	}
	defer src.Close()

	ch, errs := src.Stream(context.Background())
	var recs []Record
	for r := range ch {
		recs = append(recs, r)
	}
	for err := range errs {
		if err != nil {
			t.Fatalf("stream error: %v", err)
		}
	}

	if len(recs) != 1 {
		t.Fatalf("got %d records, want 1", len(recs))
	}

	raw, ok := recs[0].Fields["_raw_content"].(string)
	if !ok {
		t.Fatalf("no _raw_content field")
	}

	if !strings.Contains(raw, "# Welcome") {
		t.Fatalf("missing h1: %s", raw)
	}
	if !strings.Contains(raw, "**test**") {
		t.Fatalf("missing bold: %s", raw)
	}
	if !strings.Contains(raw, "- Item 1") {
		t.Fatalf("missing list: %s", raw)
	}
	if !strings.Contains(raw, "| Name | Score |") {
		t.Fatalf("missing table: %s", raw)
	}
	if !strings.Contains(raw, "[here](https://example.com)") {
		t.Fatalf("missing link: %s", raw)
	}

	if recs[0].Fields["title"] != "Test Page" {
		t.Fatalf("title=%v, want Test Page", recs[0].Fields["title"])
	}
	if recs[0].Fields["author"] != "John" {
		t.Fatalf("author=%v, want John", recs[0].Fields["author"])
	}
}

func TestConfluenceNestedPages(t *testing.T) {
	exportDir := t.TempDir()
	os.MkdirAll(filepath.Join(exportDir, "child"), 0755)

	os.WriteFile(filepath.Join(exportDir, "parent.html"), []byte("<html><body><p>Parent</p></body></html>"), 0644)
	os.WriteFile(filepath.Join(exportDir, "child", "page.html"), []byte("<html><body><p>Child</p></body></html>"), 0644)

	src, _ := NewConfluence(exportDir)
	ch, _ := src.Stream(context.Background())
	count := 0
	for range ch {
		count++
	}
	if count != 2 {
		t.Fatalf("got %d pages, want 2", count)
	}
}

func TestGoogleSheetsSkipWithoutEnv(t *testing.T) {
	if os.Getenv("GOOGLE_SHEETS_TEST_ID") == "" {
		t.Skip("skipping google sheets integration test (set GOOGLE_SHEETS_TEST_ID)")
	}
}

func TestDynamoDBSkipWithoutEnv(t *testing.T) {
	if os.Getenv("AWS_DYNAMODB_TEST_TABLE") == "" {
		t.Skip("skipping dynamodb integration test (set AWS_DYNAMODB_TEST_TABLE)")
	}
}

func TestRedisSkipWithoutEnv(t *testing.T) {
	if os.Getenv("REDIS_TEST_ADDR") == "" {
		t.Skip("skipping redis integration test (set REDIS_TEST_ADDR)")
	}
}

func TestElasticsearchSkipWithoutEnv(t *testing.T) {
	if os.Getenv("ES_TEST_URL") == "" {
		t.Skip("skipping elasticsearch integration test (set ES_TEST_URL)")
	}
}

// Suppress lint for unused json import.
var _ = json.Marshal

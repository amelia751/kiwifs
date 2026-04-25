package search

import (
	"context"
	"encoding/json"
	"testing"

	"github.com/kiwifs/kiwifs/internal/storage"
)

func TestEvalComputedField_Arithmetic(t *testing.T) {
	ctx := map[string]any{
		"_word_count": 150,
		"_link_count": 3,
	}
	expr := "(word_count > 100) * 0.3 + (link_count > 0) * 0.3"
	got := EvalComputedField(expr, ctx)
	f, ok := got.(float64)
	if !ok {
		t.Fatalf("expected float64, got %T (%v)", got, got)
	}
	if f != 0.6 {
		t.Fatalf("expected 0.6, got %v", f)
	}
}

func TestEvalComputedField_DaysSince(t *testing.T) {
	ctx := map[string]any{
		"reviewed": "2020-01-01",
	}
	got := EvalComputedField("days_since(reviewed)", ctx)
	f, ok := got.(float64)
	if !ok {
		t.Fatalf("expected float64, got %T", got)
	}
	if f < 365 {
		t.Fatalf("days_since(2020-01-01) should be > 365, got %v", f)
	}
}

func TestEvalComputedField_FieldReference(t *testing.T) {
	ctx := map[string]any{
		"mastery": 0.7,
		"weight":  2.0,
	}
	got := EvalComputedField("mastery * weight", ctx)
	f, ok := got.(float64)
	if !ok {
		t.Fatalf("expected float64, got %T", got)
	}
	if f != 1.4 {
		t.Fatalf("expected 1.4, got %v", f)
	}
}

func TestEvalComputedField_Comparisons(t *testing.T) {
	ctx := map[string]any{"score": 85.0}

	tests := []struct {
		expr string
		want float64
	}{
		{"score > 80", 1},
		{"score < 80", 0},
		{"score >= 85", 1},
		{"score <= 85", 1},
		{"score = 85", 1},
		{"score != 85", 0},
	}
	for _, tt := range tests {
		got := toFloat(EvalComputedField(tt.expr, ctx))
		if got != tt.want {
			t.Errorf("%s: want %v, got %v", tt.expr, tt.want, got)
		}
	}
}

func TestEvalComputedField_Len(t *testing.T) {
	ctx := map[string]any{
		"tags": []any{"math", "science", "history"},
	}
	got := EvalComputedField("len(tags)", ctx)
	f := toFloat(got)
	if f != 3 {
		t.Fatalf("expected 3, got %v", f)
	}
}

func TestEvalComputedField_InvalidExpr(t *testing.T) {
	got := EvalComputedField("", map[string]any{})
	f := toFloat(got)
	if f != 0 {
		t.Fatalf("empty expr should return 0, got %v", f)
	}
}

func newTestSQLiteWithComputed(t *testing.T, fields map[string]string) *SQLite {
	t.Helper()
	dir := t.TempDir()
	store, err := storage.NewLocal(dir)
	if err != nil {
		t.Fatalf("storage: %v", err)
	}
	s, err := NewSQLite(dir, store, fields)
	if err != nil {
		t.Fatalf("open sqlite: %v", err)
	}
	t.Cleanup(func() { _ = s.Close() })
	return s
}

func TestCustomComputedFieldInIndexMeta(t *testing.T) {
	fields := map[string]string{
		"quality_score": "(word_count > 100) * 0.3 + (link_count > 0) * 0.3",
	}
	s := newTestSQLiteWithComputed(t, fields)

	content := []byte("---\nstatus: published\n---\n# Hello\n\nThis is a test page with enough words to be over one hundred. " +
		"We need to write a lot of content here to ensure that the word count crosses the threshold. " +
		"Adding more sentences to pad this out so we have enough body text for the computed field to fire. " +
		"The quick brown fox jumps over the lazy dog. Pack my box with five dozen liquor jugs. " +
		"How vexingly quick daft zebras jump. The five boxing wizards jump quickly. " +
		"Sphinx of black quartz judge my vow. Two driven jocks help fax my big quiz. " +
		"More content to reach 100+ words easily. And yet more content to be really sure. " +
		"Final sentences to make absolutely certain we pass the word count threshold for testing. " +
		"Another few lines should do it. Yes, this should definitely be enough now. " +
		"One more line for good measure. Let's check [[other-page]] for links too.\n")

	if err := s.IndexMeta(context.Background(), "docs/test.md", content); err != nil {
		t.Fatalf("IndexMeta: %v", err)
	}

	var fm string
	if err := s.readDB.QueryRow(`SELECT frontmatter FROM file_meta WHERE path = ?`, "docs/test.md").Scan(&fm); err != nil {
		t.Fatalf("query: %v", err)
	}
	var parsed map[string]any
	if err := json.Unmarshal([]byte(fm), &parsed); err != nil {
		t.Fatalf("unmarshal: %v", err)
	}

	qs, ok := parsed["quality_score"]
	if !ok {
		t.Fatalf("quality_score not found in frontmatter: %v", parsed)
	}
	f, ok := qs.(float64)
	if !ok {
		t.Fatalf("quality_score is %T, want float64", qs)
	}
	// word_count > 100 → 1, link_count > 0 → 1, so 0.3 + 0.3 = 0.6
	if f != 0.6 {
		t.Fatalf("quality_score = %v, want 0.6", f)
	}
}

func TestCustomComputedFieldQueryable(t *testing.T) {
	fields := map[string]string{
		"completeness": "word_count * 0.01",
	}
	s := newTestSQLiteWithComputed(t, fields)

	content := []byte("---\nstatus: draft\n---\n# Page\n\n" +
		"word word word word word word word word word word " +
		"word word word word word word word word word word " +
		"word word word word word word word word word word " +
		"word word word word word word word word word word " +
		"word word word word word word word word word word\n")

	if err := s.IndexMeta(context.Background(), "page.md", content); err != nil {
		t.Fatalf("IndexMeta: %v", err)
	}

	// Verify via direct SQL that the computed field is queryable.
	var val float64
	err := s.readDB.QueryRow(
		`SELECT json_extract(frontmatter, '$.completeness') FROM file_meta WHERE path = ?`,
		"page.md",
	).Scan(&val)
	if err != nil {
		t.Fatalf("query completeness: %v", err)
	}
	if val <= 0 {
		t.Fatalf("completeness should be > 0, got %v", val)
	}

	// Verify filtering works with a SQL numeric comparison.
	var count int
	err = s.readDB.QueryRow(
		`SELECT COUNT(*) FROM file_meta WHERE json_extract(frontmatter, '$.completeness') > 0`,
	).Scan(&count)
	if err != nil {
		t.Fatalf("count query: %v", err)
	}
	if count != 1 {
		t.Fatalf("expected 1 page with completeness > 0, got %d", count)
	}
}

package links

import (
	"reflect"
	"testing"
)

func TestExtractAndUnique(t *testing.T) {
	body := []byte("see [[foo]] and [[bar|label]] and [[foo]] again\n")
	got := Extract(body)
	want := []string{"foo", "bar", "foo"}
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("extract: %v", got)
	}
	uniq := Unique(got)
	if len(uniq) != 2 || uniq[0] != "foo" || uniq[1] != "bar" {
		t.Fatalf("unique: %v", uniq)
	}
}

func TestTargetForms(t *testing.T) {
	forms := TargetForms("concepts/authentication.md")
	// concepts/authentication.md, concepts/authentication, authentication.md, authentication
	wantContains := []string{
		"concepts/authentication.md",
		"concepts/authentication",
		"authentication.md",
		"authentication",
	}
	m := map[string]bool{}
	for _, f := range forms {
		m[f] = true
	}
	for _, w := range wantContains {
		if !m[w] {
			t.Fatalf("missing form %q in %v", w, forms)
		}
	}
}

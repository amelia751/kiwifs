package links

import (
	"context"
	"fmt"
	"reflect"
	"testing"
	"time"
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

func TestResolveWikiLinksToMarkdown(t *testing.T) {
	resolver := func(target string) string {
		m := map[string]string{
			"auth":    "concepts/auth.md",
			"billing": "concepts/billing.md",
			"notes":   "my notes/auth flow.md",
			"日本語":     "日本語/ノート.md",
			"sharp":   "file#2.md",
		}
		return m[target]
	}
	cases := []struct {
		name, input, want string
	}{
		{
			"bare link",
			"See [[auth]] for details.",
			"See [auth](https://wiki.co/page/concepts/auth.md) for details.",
		},
		{
			"labeled link",
			"Check [[auth|authentication docs]] here.",
			"Check [authentication docs](https://wiki.co/page/concepts/auth.md) here.",
		},
		{
			"unresolved link stays",
			"See [[unknown]] page.",
			"See [[unknown]] page.",
		},
		{
			"multiple links",
			"See [[auth]] and [[billing]].",
			"See [auth](https://wiki.co/page/concepts/auth.md) and [billing](https://wiki.co/page/concepts/billing.md).",
		},
		{
			"empty publicURL returns unchanged",
			"See [[auth]].",
			"See [[auth]].",
		},
		{
			"path with spaces",
			"See [[notes]].",
			"See [notes](https://wiki.co/page/my%20notes/auth%20flow.md).",
		},
		{
			"unicode path",
			"See [[日本語]].",
			"See [日本語](https://wiki.co/page/%E6%97%A5%E6%9C%AC%E8%AA%9E/%E3%83%8E%E3%83%BC%E3%83%88.md).",
		},
		{
			"path with hash",
			"See [[sharp]].",
			"See [sharp](https://wiki.co/page/file%232.md).",
		},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			pub := "https://wiki.co"
			if tc.name == "empty publicURL returns unchanged" {
				pub = ""
			}
			got := ResolveWikiLinksToMarkdown(tc.input, pub, resolver)
			if got != tc.want {
				t.Fatalf("got %q, want %q", got, tc.want)
			}
		})
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

func TestTargetFormsSpecialChars(t *testing.T) {
	cases := []struct {
		path string
		want []string
	}{
		{
			"my notes/auth flow.md",
			[]string{"my notes/auth flow.md", "my notes/auth flow", "auth flow.md", "auth flow"},
		},
		{
			"日本語/ノート.md",
			[]string{"日本語/ノート.md", "日本語/ノート", "ノート.md", "ノート"},
		},
		{
			"file#2.md",
			[]string{"file#2.md", "file#2"},
		},
	}
	for _, tc := range cases {
		m := map[string]bool{}
		for _, f := range TargetForms(tc.path) {
			m[f] = true
		}
		for _, w := range tc.want {
			if !m[w] {
				t.Errorf("TargetForms(%q): missing %q, got %v", tc.path, w, TargetForms(tc.path))
			}
		}
	}
}

func TestResolverCaching(t *testing.T) {
	const fileCount = 1000
	paths := make([]string, fileCount)
	for i := range paths {
		paths[i] = fmt.Sprintf("dir%d/file%d.md", i%10, i)
	}

	walker := func(_ context.Context, fn func(string)) error {
		for _, p := range paths {
			fn(p)
		}
		return nil
	}
	r := NewResolver(walker)

	ctx := context.Background()
	content := "See [[file500]] for details."

	got := r.Resolve(ctx, content, "https://wiki.co")
	if got == content {
		t.Fatalf("first resolve returned unmodified content")
	}

	start := time.Now()
	got2 := r.Resolve(ctx, content, "https://wiki.co")
	elapsed := time.Since(start)

	if got2 != got {
		t.Fatalf("second resolve returned different result: %q vs %q", got2, got)
	}
	if elapsed > time.Millisecond {
		t.Fatalf("cached resolve took %v, expected <1ms", elapsed)
	}
}

func BenchmarkResolverResolve(b *testing.B) {
	const fileCount = 1000
	paths := make([]string, fileCount)
	for i := range paths {
		paths[i] = fmt.Sprintf("dir%d/file%d.md", i%10, i)
	}
	walker := func(_ context.Context, fn func(string)) error {
		for _, p := range paths {
			fn(p)
		}
		return nil
	}
	r := NewResolver(walker)
	ctx := context.Background()
	content := "See [[file500]] and [[file100]] and [[file999]]."

	r.Resolve(ctx, content, "https://wiki.co")

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		r.Resolve(ctx, content, "https://wiki.co")
	}
}

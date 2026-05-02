package links

import (
	"testing"
)

func TestRewriteLinks_Simple(t *testing.T) {
	content := "See [[auth]] for details."
	got, changed := RewriteLinks(content, "auth.md", "authentication")
	if !changed {
		t.Fatal("expected changed=true")
	}
	want := "See [[authentication]] for details."
	if got != want {
		t.Fatalf("got %q, want %q", got, want)
	}
}

func TestRewriteLinks_WithDisplayText(t *testing.T) {
	content := "See [[auth|Login docs]] for details."
	got, changed := RewriteLinks(content, "auth.md", "authentication")
	if !changed {
		t.Fatal("expected changed=true")
	}
	want := "See [[authentication|Login docs]] for details."
	if got != want {
		t.Fatalf("got %q, want %q", got, want)
	}
}

func TestRewriteLinks_WithPath(t *testing.T) {
	content := "See [[concepts/auth]] for more."
	got, changed := RewriteLinks(content, "concepts/auth.md", "authentication")
	if !changed {
		t.Fatal("expected changed=true")
	}
	want := "See [[authentication]] for more."
	if got != want {
		t.Fatalf("got %q, want %q", got, want)
	}
}

func TestRewriteLinks_CaseInsensitive(t *testing.T) {
	content := "See [[Auth]] for info."
	got, changed := RewriteLinks(content, "auth.md", "authentication")
	if !changed {
		t.Fatal("expected changed=true")
	}
	want := "See [[authentication]] for info."
	if got != want {
		t.Fatalf("got %q, want %q", got, want)
	}
}

func TestRewriteLinks_InCodeBlock(t *testing.T) {
	content := "Normal [[auth]] text.\n```\n[[auth]] in code\n```\nMore [[auth]] text."
	got, changed := RewriteLinks(content, "auth.md", "authentication")
	if !changed {
		t.Fatal("expected changed=true")
	}
	want := "Normal [[authentication]] text.\n```\n[[auth]] in code\n```\nMore [[authentication]] text."
	if got != want {
		t.Fatalf("got:\n%s\nwant:\n%s", got, want)
	}
}

func TestRewriteLinks_NoMatch(t *testing.T) {
	content := "See [[other]] for details."
	got, changed := RewriteLinks(content, "auth.md", "authentication")
	if changed {
		t.Fatal("expected changed=false")
	}
	if got != content {
		t.Fatalf("got %q, want %q", got, content)
	}
}

func TestRewriteLinks_MultipleOccurrences(t *testing.T) {
	content := "Link [[auth]] and [[auth|docs]] here."
	got, changed := RewriteLinks(content, "auth.md", "authentication")
	if !changed {
		t.Fatal("expected changed=true")
	}
	want := "Link [[authentication]] and [[authentication|docs]] here."
	if got != want {
		t.Fatalf("got %q, want %q", got, want)
	}
}

func TestRewriteLinks_EmptyTargets(t *testing.T) {
	content := "See [[auth]] here."
	got, changed := RewriteLinks(content, "", "authentication")
	if changed {
		t.Fatal("expected changed=false for empty oldTarget")
	}
	if got != content {
		t.Fatalf("content should be unchanged")
	}

	got, changed = RewriteLinks(content, "auth.md", "")
	if changed {
		t.Fatal("expected changed=false for empty newTarget")
	}
}

func TestRewriteLinks_InInlineCode(t *testing.T) {
	content := "Use `[[auth]]` syntax for links, but [[auth]] is real."
	got, changed := RewriteLinks(content, "auth.md", "authentication")
	if !changed {
		t.Fatal("expected changed=true")
	}
	want := "Use `[[auth]]` syntax for links, but [[authentication]] is real."
	if got != want {
		t.Fatalf("got %q, want %q", got, want)
	}
}

func TestRewriteLinks_AllInInlineCode(t *testing.T) {
	content := "Use `[[auth]]` for raw link syntax."
	got, changed := RewriteLinks(content, "auth.md", "authentication")
	if changed {
		t.Fatal("expected changed=false when all links in inline code")
	}
	if got != content {
		t.Fatalf("content should be unchanged")
	}
}

func TestRewriteLinks_NewTargetWithSpecialChars(t *testing.T) {
	content := "See [[auth]] for details."
	got, changed := RewriteLinks(content, "auth.md", "auth (v2)")
	if !changed {
		t.Fatal("expected changed=true")
	}
	want := "See [[auth (v2)]] for details."
	if got != want {
		t.Fatalf("got %q, want %q", got, want)
	}
}

func TestRewriteLinks_ConsecutiveLinksOnSameLine(t *testing.T) {
	content := "Compare [[auth]] with [[session]] and [[auth|login]]."
	got, changed := RewriteLinks(content, "auth.md", "authentication")
	if !changed {
		t.Fatal("expected changed=true")
	}
	want := "Compare [[authentication]] with [[session]] and [[authentication|login]]."
	if got != want {
		t.Fatalf("got %q, want %q", got, want)
	}
}

func TestRewriteLinks_NestedCodeBlocksAndInlineCode(t *testing.T) {
	content := "Real [[auth]] link.\n```\ncode [[auth]] block\n```\nAnother `[[auth]]` inline.\nFinal [[auth]] real."
	got, changed := RewriteLinks(content, "auth.md", "authentication")
	if !changed {
		t.Fatal("expected changed=true")
	}
	want := "Real [[authentication]] link.\n```\ncode [[auth]] block\n```\nAnother `[[auth]]` inline.\nFinal [[authentication]] real."
	if got != want {
		t.Fatalf("got:\n%s\nwant:\n%s", got, want)
	}
}

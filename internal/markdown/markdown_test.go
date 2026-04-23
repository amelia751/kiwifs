package markdown

import "testing"

func TestParseFrontmatterAndHeadings(t *testing.T) {
	input := []byte("---\ntitle: Hello\ntags: [a, b]\n---\n\n# Top\n\n## Section Two\n\nbody\n\n### Deep\n")
	p, err := Parse(input)
	if err != nil {
		t.Fatalf("parse: %v", err)
	}
	if p.Frontmatter["title"] != "Hello" {
		t.Fatalf("frontmatter: %v", p.Frontmatter)
	}
	if len(p.Headings) != 3 {
		t.Fatalf("want 3 headings, got %d: %v", len(p.Headings), p.Headings)
	}
	if p.Headings[0].Text != "Top" || p.Headings[0].Level != 1 {
		t.Fatalf("heading 0: %+v", p.Headings[0])
	}
	if p.Headings[1].Slug != "section-two" {
		t.Fatalf("slug: %s", p.Headings[1].Slug)
	}
}

func TestSlugify(t *testing.T) {
	cases := map[string]string{
		"Hello World":         "hello-world",
		"  A/B  c ":           "ab-c",
		"---dash":             "dash",
		"Already-OK_thing":    "already-ok-thing",
	}
	for in, want := range cases {
		if got := Slugify(in); got != want {
			t.Fatalf("Slugify(%q)=%q, want %q", in, got, want)
		}
	}
}

package comments

import (
	"sync"
	"testing"
)

func TestAddListDelete(t *testing.T) {
	s, err := New(t.TempDir())
	if err != nil {
		t.Fatalf("new: %v", err)
	}
	added, err := s.Add("note.md", Comment{
		Anchor: Anchor{Quote: "hello"},
		Body:   "comment 1",
		Author: "tester",
	})
	if err != nil {
		t.Fatalf("add: %v", err)
	}
	got, err := s.List("note.md")
	if err != nil || len(got) != 1 {
		t.Fatalf("list: %v %d", err, len(got))
	}
	if got[0].ID != added.ID {
		t.Fatalf("id mismatch")
	}
	if err := s.Delete("note.md", added.ID); err != nil {
		t.Fatalf("delete: %v", err)
	}
	got, _ = s.List("note.md")
	if len(got) != 0 {
		t.Fatalf("want empty, got %v", got)
	}
}

// Per-file locks mean concurrent Adds to *different* paths should not serialise
// on a single mutex. This test would deadlock if the old global mutex were in
// place and one Add held the lock while another waited.
func TestConcurrentAddsDifferentPaths(t *testing.T) {
	s, _ := New(t.TempDir())
	var wg sync.WaitGroup
	for i := 0; i < 10; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			_, _ = s.Add("p"+string(rune('a'+i))+".md", Comment{
				Anchor: Anchor{Quote: "q"},
				Body:   "b",
				Author: "t",
			})
		}(i)
	}
	wg.Wait()
}

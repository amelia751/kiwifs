// Package memory provides conventions and helpers for episodic vs semantic
// knowledge, and for consolidation provenance (merged-from).
package memory

// Well-known values for the memory_kind frontmatter key.
const (
	KindEpisodic       = "episodic"
	KindSemantic       = "semantic"
	KindConsolidation  = "consolidation" // intermediate / pending merge
	KindWorkingScratch = "working"      // high-churn scratch, optional
)

// DefaultEpisodesPathPrefix is used when [memory] episodes_path_prefix is unset
// in config. Files under this path are treated as episodic when frontmatter
// is ambiguous and memory_kind is not explicitly semantic.
const DefaultEpisodesPathPrefix = "episodes/"

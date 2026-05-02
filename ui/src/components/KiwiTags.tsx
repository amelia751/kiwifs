import { useEffect, useState } from "react";
import { Tag } from "lucide-react";
import { api } from "@/lib/api";
import { titleize } from "@/lib/paths";
import { Badge } from "@/components/ui/badge";

type TagInfo = { tag: string; count: number; paths: string[] };

type Props = {
  onTagClick?: (tag: string) => void;
  onNavigate?: (path: string) => void;
};

export function KiwiTags({ onTagClick, onNavigate }: Props) {
  const [tags, setTags] = useState<TagInfo[]>([]);
  const [loading, setLoading] = useState(true);
  const [selectedTag, setSelectedTag] = useState<string | null>(null);
  const [pages, setPages] = useState<string[]>([]);

  useEffect(() => {
    setLoading(true);
    api
      .meta({ where: [{ field: "$.tags", op: "!=", value: "" }], limit: 1000 })
      .then((r) => {
        const tagMap = new Map<string, string[]>();
        for (const result of r.results) {
          const rawTags = result.frontmatter?.tags;
          const tagList = Array.isArray(rawTags) ? rawTags : typeof rawTags === "string" ? [rawTags] : [];
          for (const t of tagList) {
            const tag = String(t).trim();
            if (!tag) continue;
            const existing = tagMap.get(tag) || [];
            existing.push(result.path);
            tagMap.set(tag, existing);
          }
        }
        const sorted = Array.from(tagMap.entries())
          .map(([tag, paths]) => ({ tag, count: paths.length, paths }))
          .sort((a, b) => b.count - a.count);
        setTags(sorted);
      })
      .catch(() => setTags([]))
      .finally(() => setLoading(false));
  }, []);

  function handleTagSelect(tag: string) {
    if (selectedTag === tag) {
      setSelectedTag(null);
      setPages([]);
    } else {
      setSelectedTag(tag);
      const info = tags.find((t) => t.tag === tag);
      setPages(info?.paths || []);
    }
    onTagClick?.(tag);
  }

  if (loading) {
    return <div className="p-4 text-sm text-muted-foreground">Loading tags...</div>;
  }
  if (tags.length === 0) {
    return <div className="p-4 text-sm text-muted-foreground">No tags found. Add tags to your pages via frontmatter.</div>;
  }

  const maxCount = tags[0]?.count || 1;

  return (
    <div className="p-4 space-y-4">
      <div className="flex items-center gap-2 text-sm font-semibold text-foreground">
        <Tag className="h-4 w-4" />
        Tags ({tags.length})
      </div>
      <div className="flex flex-wrap gap-2">
        {tags.map(({ tag, count }) => {
          const scale = 0.75 + (count / maxCount) * 0.5;
          return (
            <Badge
              key={tag}
              variant={selectedTag === tag ? "default" : "secondary"}
              className="cursor-pointer hover:bg-primary/20 transition-colors gap-1"
              style={{ fontSize: `${scale}rem` }}
              onClick={() => handleTagSelect(tag)}
            >
              <Tag className="h-3 w-3" />
              {tag}
              <span className="text-[0.7em] opacity-70">({count})</span>
            </Badge>
          );
        })}
      </div>
      {selectedTag && pages.length > 0 && (
        <div className="border-t border-border pt-3 space-y-1">
          <div className="text-xs font-medium text-muted-foreground mb-2">
            Pages tagged "{selectedTag}"
          </div>
          {pages.map((path) => (
            <button
              key={path}
              type="button"
              onClick={() => onNavigate?.(path)}
              className="w-full text-left px-2 py-1.5 rounded-md text-sm hover:bg-accent hover:text-accent-foreground transition-colors truncate"
            >
              {titleize(path)}
            </button>
          ))}
        </div>
      )}
    </div>
  );
}

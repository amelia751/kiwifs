export function TreeSkeleton() {
  return (
    <div className="animate-pulse p-3 space-y-2">
      {[120, 96, 80, 140, 72, 104, 88, 64, 112, 96].map((w, i) => (
        <div
          key={i}
          className="flex items-center gap-2"
          style={{ paddingLeft: (i % 3) * 12 + 8 }}
        >
          <div className="h-3.5 w-3.5 bg-muted rounded shrink-0" />
          <div className="h-3.5 bg-muted rounded" style={{ width: w }} />
        </div>
      ))}
    </div>
  );
}

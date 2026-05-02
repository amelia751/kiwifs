export function PageSkeleton() {
  return (
    <div className="animate-pulse space-y-4 p-8 max-w-3xl">
      <div className="h-4 w-48 bg-muted rounded" />
      <div className="h-8 w-96 bg-muted rounded" />
      <div className="h-3 w-64 bg-muted rounded" />
      <div className="flex gap-2">
        <div className="h-6 w-16 bg-muted rounded-full" />
        <div className="h-6 w-20 bg-muted rounded-full" />
      </div>
      <div className="space-y-3 mt-8">
        <div className="h-4 w-full bg-muted rounded" />
        <div className="h-4 w-5/6 bg-muted rounded" />
        <div className="h-4 w-4/6 bg-muted rounded" />
        <div className="h-4 w-full bg-muted rounded" />
        <div className="h-4 w-3/6 bg-muted rounded" />
      </div>
    </div>
  );
}

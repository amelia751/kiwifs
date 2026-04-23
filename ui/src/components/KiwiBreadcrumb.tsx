import { Home } from "lucide-react";
import {
  Breadcrumb,
  BreadcrumbItem,
  BreadcrumbLink,
  BreadcrumbList,
  BreadcrumbPage,
  BreadcrumbSeparator,
} from "@/components/ui/breadcrumb";
import { breadcrumbs } from "@/lib/paths";

type Props = {
  path: string;
  onNavigate: (path: string) => void;
};

export function KiwiBreadcrumb({ path, onNavigate }: Props) {
  const segments = breadcrumbs(path);
  return (
    <Breadcrumb>
      <BreadcrumbList>
        <BreadcrumbItem>
          <BreadcrumbLink
            href="#"
            onClick={(e) => {
              e.preventDefault();
              onNavigate("");
            }}
            className="flex items-center gap-1"
          >
            <Home className="h-3.5 w-3.5" />
            <span>Knowledge</span>
          </BreadcrumbLink>
        </BreadcrumbItem>
        {segments.map((seg, i) => {
          const isLast = i === segments.length - 1;
          return (
            <BreadcrumbItem key={seg.path}>
              <BreadcrumbSeparator />
              {isLast ? (
                <BreadcrumbPage className="truncate max-w-[220px]">
                  {seg.label}
                </BreadcrumbPage>
              ) : (
                <BreadcrumbLink
                  href="#"
                  onClick={(e) => {
                    e.preventDefault();
                    onNavigate(seg.path);
                  }}
                  className="truncate max-w-[220px]"
                >
                  {seg.label}
                </BreadcrumbLink>
              )}
            </BreadcrumbItem>
          );
        })}
      </BreadcrumbList>
    </Breadcrumb>
  );
}

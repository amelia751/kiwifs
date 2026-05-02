import { Component, type ReactNode } from "react";

type Props = { children: ReactNode; fallback?: ReactNode };
type State = { error: Error | null };

export class ErrorBoundary extends Component<Props, State> {
  state: State = { error: null };

  static getDerivedStateFromError(error: Error) {
    return { error };
  }

  render() {
    if (this.state.error) {
      return this.props.fallback ?? (
        <div className="p-6 rounded-lg border border-destructive/50 bg-destructive/5 text-sm">
          <p className="font-semibold text-destructive">Something went wrong rendering this content.</p>
          <pre className="mt-2 text-xs text-muted-foreground overflow-auto">
            {this.state.error.message}
          </pre>
          <button
            onClick={() => this.setState({ error: null })}
            className="mt-3 text-xs underline text-primary"
          >
            Try again
          </button>
        </div>
      );
    }
    return this.props.children;
  }
}

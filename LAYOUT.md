# KiwiFS — Layout Contract

KiwiFS uses an opinionated, fixed layout structure. Users can customize
**appearance** (colors, fonts, spacing) via the theme system, but the
**structure** — the zones described below — is not configurable.

## Zones

```
┌────────────────────────────────────────────────────┐
│  Header (h-12)                                     │
│  [sidebar toggle] [logo] [search bar] [actions]    │
├────────────┬───────────────────────────────────────┤
│  Sidebar   │  Main content                         │
│  (272px)   │                                       │
│            │  ┌─────────────────────────────────┐  │
│  Space     │  │ Breadcrumb (sticky)             │  │
│  Starred   │  ├─────────────────────────────────┤  │
│  Recent    │  │ Page header (title, meta, tags) │  │
│  Pages     │  │                                 │  │
│  (tree)    │  │ Content + ToC sidebar           │  │
│            │  │                                 │  │
│            │  │ Footer (comments, backlinks)    │  │
│            │  │                                 │  │
│            │  │ File info                       │  │
│            │  └─────────────────────────────────┘  │
└────────────┴───────────────────────────────────────┘
```

### Header

Fixed-height bar (`h-12`, 48px). Three zones:

| Zone   | Contents                                        |
|--------|-------------------------------------------------|
| Left   | Sidebar toggle, logo, space name                |
| Center | Search bar (opens command palette, `⌘K`)        |
| Right  | New page, graph, history, theme toggle           |

### Sidebar

Fixed width of **272px** (CSS `--sidebar-width`). Collapses to 0px via the
toggle button. Sections from top to bottom:

1. **Space selector** — switch between registered spaces.
2. **Starred** — pages the user has starred (localStorage).
3. **Recent** — last 5 visited pages (localStorage).
4. **Pages** — full file tree with drag-and-drop, rename, and context menu.

Each section is collapsible (state saved to localStorage).

### Main content

Fills the remaining width (`flex-1`). Renders one of:

- **Page view** (`KiwiPage`) — markdown render with ToC, backlinks, comments.
- **Editor** (`KiwiEditor`) — BlockNote rich-text editor.
- **Graph** (`KiwiGraph`) — Sigma.js force-directed knowledge graph.
- **History** (`KiwiHistory`) — version diff viewer.
- **Theme editor** (`KiwiThemeEditor`) — live color/font customization.
- **Welcome screen** — shown when no page is selected.

### Breadcrumb

Sticky bar at the top of the main content area. Shows the full path as
clickable segments. Present in both page view and editor.

### Footer (page view only)

Below the rendered markdown, two collapsible sections in fixed order:

1. **Comments** — text-anchored comment threads.
2. **Backlinks** — pages that link to the current page.

## Rules

1. **Structure is fixed.** Components always appear in the positions shown
   above. No user or API can rearrange zones.
2. **Appearance is customizable.** Colors, fonts, spacing, and density are
   controlled by CSS custom properties (see theme presets). Users change
   appearance, not structure.
3. **Responsive behavior.** The sidebar collapses on toggle; no breakpoint-based
   layout changes. The header is always visible.
4. **Overflow.** The sidebar and main content area scroll independently.
   The header and breadcrumb are sticky.

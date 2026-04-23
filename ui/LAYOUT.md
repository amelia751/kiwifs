# KiwiFS Layout Contract

The KiwiFS UI uses a **fixed layout structure**. Users and integrators may
customise appearance (theme, colours, fonts) but cannot rearrange the zones.

## Zones

```
┌─────────────────────────────────────────────────┐
│  HEADER  (h-12, full width)                     │
│  logo · search bar · actions · theme toggle     │
├──────────┬──────────────────────────────────────┤
│ SIDEBAR  │  BREADCRUMB  (sticky top-0)          │
│ (w-272)  ├──────────────────────────────────────┤
│          │                                      │
│ Space    │  CONTENT                             │
│ Starred  │  (scrollable, max-w-3xl/6xl)         │
│ Recent   │                                      │
│ Pinned   │  Page view / Editor / Graph /        │
│ Pages    │  History / Theme editor              │
│          │                                      │
│          ├──────────────────────────────────────┤
│          │  FOOTER sections (in-content)        │
│          │  comments · backlinks                │
└──────────┴──────────────────────────────────────┘
```

### Header
- Fixed height (48px / h-12), always visible.
- Left: sidebar toggle, logo, space name.
- Center: search bar (opens Cmd+K modal).
- Right: new page, graph, history, theme toggle.

### Sidebar
- Default width 272px, collapsible via toggle or drag-to-resize (200–480px range).
- Sections collapse/expand independently; state persisted in localStorage.
- Section order is fixed: Space selector → Starred → Recent → Pinned → Pages (tree).

### Breadcrumb
- Sticky at content top, shows path segments as clickable links.
- Present in both read and edit modes.

### Content
- Fills remaining space, scrollable.
- Max width constrained by `--content-max-width` token (default 48rem for prose,
  6xl for editor).
- Exactly one view active: Page, Editor, Graph, History, or Theme Editor.

### Footer (in-content)
- Collapsible sections at bottom of page view: comments, backlinks.
- Not a separate fixed zone — scrolls with content.

## Rules

1. **Structure is fixed.** Theme tokens change appearance; layout zones don't move.
2. **One content view at a time.** Switching views (edit, graph, history) replaces
   the content area — there are no split panes or simultaneous views.
3. **Sidebar sections have a fixed order.** Custom sections (Pinned) slot into the
   predefined order, not arbitrary positions.
4. **Responsive collapse.** On narrow viewports the sidebar auto-collapses; the
   header search bar truncates. The zone structure stays the same.

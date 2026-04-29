---
title: How wikilinks work
tags: [onboarding, concepts]
---

# How wikilinks work

Inside any page, write `[[target-page]]` to link to another
page. KiwiFS resolves the target by:

1. exact path (`[[concepts/wikilinks]]` → `concepts/wikilinks.md`)
2. basename match across the tree (`[[wikilinks]]` works too)
3. title match from the page's frontmatter

You can give the link a different label with a pipe:

    [[concepts/wikilinks|how linking works]]

→ [[concepts/wikilinks|how linking works]]

Broken links render with a dashed underline so they're easy to spot.
Fixing a link is as simple as creating the target page or renaming
along the way.

See also: [[concepts/frontmatter]], [[SCHEMA]].

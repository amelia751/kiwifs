# Schema — Runbook

Operational knowledge for on-call and platform teams.

## Conventions
- Every incident gets a file `incidents/YYYY-MM-DD-<slug>.md` copied from
  `incidents/template.md`.
- Every procedure lives in `procedures/` and is linkable from
  on-call playbooks via `[[procedure-name]]`.
- Postmortems live in `postmortems/` and link back to the incident.

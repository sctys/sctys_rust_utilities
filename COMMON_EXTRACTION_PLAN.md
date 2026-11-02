# Shared framework boundaries

This library owns reusable scraping, parsing, storage, and workflow mechanisms.
Callers supply identities, schemas, domain transformations, service configuration,
and scheduling policies.

## Design rules

- Keep concrete application enums, module names, URLs, challenge protocols,
  schedules, and migration inventories outside this repository.
- Define focused traits and configured adapters instead of importing application
  crates or selecting behavior by application identity.
- Share execution loops, batching, ordering, and error handling. Callbacks supply
  domain work rather than duplicate those mechanisms.
- Keep buffers and mutable sessions scoped to one execution. Borrow service
  contexts explicitly and avoid self-referential service owners.
- Preserve serialized field order, path conventions, date boundaries, and partial
  failure behavior when changing reusable mechanisms.
- Return structured errors from library workflows. Callers select reporting and
  termination boundaries.
- Keep test data synthetic. Live integration fixtures should use explicit test
  configuration rather than application endpoints or project data paths.

See the [framework guide](docs/common-extraction/README.md) for API ownership,
contracts, and local validation commands.

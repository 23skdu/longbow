# Schema Evolution and Versioning

Longbow supports basic schema evolution to allow applications to evolve without
downtime.

## Versioning

Each dataset tracks a `Version` number. Note that automated version incrementing on schema
changes is planned but currently requires manual coordination if explicit tracking is needed.

## Evolution Rules

Currently, the following evolution is supported:

- **Additive Changes**: New columns can be added to the end of the schema.
- **Compatibility**: The existing columns must match exactly (name and type).

If a schema mismatch is detected that does not follow these rules, the write is
rejected.

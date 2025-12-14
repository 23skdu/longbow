# Schema Evolution and Versioning

Longbow supports basic schema evolution to allow applications to evolve without
downtime.

## Versioning

Each dataset now tracks a `Version` number, which increments whenever a schema
change is detected and accepted.

## Evolution Rules

Currently, the following evolution is supported:

- **Additive Changes**: New columns can be added to the end of the schema.
- **Compatibility**: The existing columns must match exactly (name and type).

If a schema mismatch is detected that does not follow these rules, the write is
rejected.

# Namespace Management

Longbow supports multi-tenancy via Namespaces. Each namespace isolates datasets.

## Actions

The Meta Server exposes the following actions via `DoAction`.

### CreateNamespace

Body:

```json
{"name": "my-tenant"}
```

Returns: `{"status": "created"}` or error.

### DeleteNamespace

Body:

```json
{"name": "my-tenant"}
```

Returns: `{"status": "deleted"}` or error.

### ListNamespaces

Body: `{}`
Returns:

```json
{
  "namespaces": ["default", "my-tenant"],
  "count": 2
}
```

### GetTotalNamespaceCount

Body: `{}`
Returns:

```json
{"count": 2}
```

### GetNamespaceDatasetCount

Body:

```json
{"name": "my-tenant"}
```

Returns:

```json
{"count": 5}
```

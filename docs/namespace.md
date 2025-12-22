# Namespaces

Longbow supports multi-tenancy through **Namespaces**. Each namespace is an isolated logical unit that contains its own set of datasets.

## Concept

A namespace acts as a container for datasets, allowing for:

- Logical isolation of data (e.g., per-tenant or per-application).
- Simplified management (deleting a namespace deletes all its datasets).
- Independent configuration (future roadmap).

The default namespace is named `default`. If no namespace is specified in an operation, the `default` namespace is used.

## Usage

### Creating a Namespace

Namespaces are created explicitly via the API.

```go
err := store.CreateNamespace("tenant-abc")
if err != nil {
    // Handle error (e.g., already exists)
}
```

### Deleting a Namespace

Deleting a namespace removes it and all associated datasets permanently.

```go
err := store.DeleteNamespace("tenant-abc")
// Note: You cannot delete the "default" namespace.
```

### Addressing Datasets

Datasets within a namespace are addressed using a path format: methods `ParseNamespacedPath` and `BuildNamespacedPath` exist to help with this.

Format: `{namespace}/{dataset_name}`

Examples:

- `tenant-abc/users` -> Namespace: `tenant-abc`, Dataset: `users`
- `users` -> Namespace: `default`, Dataset: `users`
- `/users` -> Namespace: `default`, Dataset: `users`
- `org/project/data` -> Namespace: `org`, Dataset: `project/data`

## API Reference

The `VectorStore` exposes the following methods for namespace management:

- `CreateNamespace(name string) error`
- `DeleteNamespace(name string) error`
- `NamespaceExists(name string) bool`
- `ListNamespaces() []string`
- `GetNamespaceDatasetCount(name string) int`
- `GetTotalNamespaceCount() int`

## Metrics

- `longbow_namespaces_total`: Gauge tracking the active number of namespaces.

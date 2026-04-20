# Advanced SQL Features

Longbow extends the Arrow Flight protocol with advanced SQL-like capabilities, including **Common Table Expressions (CTEs)** and **Subqueries**. These features allow for complex, multi-stage filtering and data retrieval within a single request.

## Common Table Expressions (CTEs)

CTEs allow you to define temporary result sets (aliases) that can be referenced within the main query's filters. This is particularly useful for pre-selecting a subset of vectors (e.g., "top vendors") and using them to filter another dataset.

### Syntax

CTEs are defined in the top-level `with` array of the query ticket.

```json
{
  "with": [
    {
      "name": "top_vendors",
      "search": {
        "dataset": "vendors",
        "k": 10
      }
    }
  ],
  "name": "products",
  "filters": [
    {
      "field": "vendor_id", 
      "operator": "IN", 
      "value": "top_vendors"
    }
  ]
}
```

In this example:
1.  A CTE named `top_vendors` is defined by performing a search on the `vendors` dataset.
2.  The main query on `products` filters for `vendor_id` that exists in the `top_vendors` result set.

## Subqueries

Subqueries are filters that dynamically execute a secondary search to determine the matching values for a field. They are similar to CTEs but are defined inline within a filter.

### Syntax

Subqueries are defined using the `subquery` field within a filter object.

```json
{
  "name": "orders",
  "filters": [
    {
      "field": "user_id",
      "operator": "IN",
      "subquery": {
        "name": "active_users",
        "search": {
          "dataset": "users",
          "filters": [
            {
              "field": "status", 
              "operator": "==", 
              "value": "active"
            }
          ]
        }
      }
    }
  ]
}
```

In this example:
1.  The query on `orders` includes a filter on `user_id`.
2.  The `IN` operator uses a `subquery` to fetch `user_id` values from the `users` dataset where `status` is `active`.

## Key Differences

| Feature | Scope | Performance | Usage |
|---------|-------|-------------|-------|
| **CTE** | Top-level, can be referenced multiple times | Evaluated once per query | Reusable temporary sets |
| **Subquery** | Inline to a specific filter | Evaluated per filter | One-off dynamic filtering |

## Performance Considerations

*   **Recursion**: Longbow currently does not support recursive CTEs.
*   **Result Set Sizes**: CTE and Subquery result sets are held in memory during query execution. It is recommended to use `k` or restrictive filters to limit the size of intermediate results.
*   **Parallelism**: CTEs and the main query levels are executed sequentially, while subqueries within a single filter set may be parallelized where possible.

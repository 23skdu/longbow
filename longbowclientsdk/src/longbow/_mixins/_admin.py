"""Namespaces, datasets, backups, cloud clusters, streaming, billing, sharding, schemas, and views."""

import pyarrow.flight as flight
import json
from typing import Union, List, Dict, Any, Optional


class AdminMixin:
    def create_namespace(
        self,
        name: str,
        dims: int = 128,
        data_type: str = "float32",
        force: bool = False,
        **hnsw_config,
    ):
        """
        Create a new dataset/namespace.

        Args:
            name: Name of the namespace.
            dims: Vector dimensions.
            data_type: Storage type (float32, int8, turboquant, etc.).
            force: If True, overwrite if exists.
            **hnsw_config: HNSW parameters (m, ef_construction).
        """
        req: Dict[str, Any] = {
            "name": name,
            "dims": dims,
            "data_type": data_type,
            "overwrite": force,
            "hnsw_config": hnsw_config,
        }
        action_body = json.dumps(req).encode("utf-8")
        action = flight.Action("CreateNamespace", action_body)
        list(self._require_meta_client().do_action(action, options=self._get_call_options()))
    def create_dataset(
        self,
        name: str,
        dimensions: int,
        vector_type: str = "float32",
        turboquant_bits: int = 4,
        geo_enabled: bool = False,
        disk_enabled: bool = False,
        metric: str = "cosine",
    ):
        """
        Create a new dataset with specific feature configurations.

        Args:
            name: Name of the dataset (e.g. "tenant/vectors").
            dimensions: Vector dimensionality.
            vector_type: "float32", "turboquant" (tq), "int8", "float16".
            turboquant_bits: Bit depth for TQ (4 or 8; 1-3 encode but lose the vector
                direction). Defaults to 4, matching the server-side default.
            geo_enabled: Enable geospatial indexing (Quadtree).
            disk_enabled: Enable Disk-ANN offloading.
            metric: Distance metric ("cosine", "l2", "ip").
        """
        req: Dict[str, Any] = {
            "name": name,
            "dimension": dimensions,
            "vector_type": vector_type,
            "turboquant_bits": turboquant_bits,
            "geo_enabled": geo_enabled,
            "disk_enabled": disk_enabled,
            "metric": metric,
        }
        action_body = json.dumps(req).encode("utf-8")
        action = flight.Action("CreateDataset", action_body)
        list(self._require_meta_client().do_action(action, options=self._get_call_options()))
        # Check results if needed

    def list_namespaces(self) -> List[str]:
        """List all available datasets."""
        return [
            f.descriptor.path[0].decode("utf-8")
            for f in self._require_meta_client().list_flights()
        ]
    def list_datasets_in_namespace(self, namespace: str = "default") -> List[str]:
        req = {"name": namespace}
        action_body = json.dumps(req).encode("utf-8")
        action = flight.Action("ListDatasetsInNamespace", action_body)
        results = list(
            self._require_meta_client().do_action(
                action, options=self._get_call_options()
            )
        )

        if not results:
            return []

        resp = json.loads(results[0].body.to_py())
        return resp.get("datasets", [])
    def delete_namespace(self, dataset: str):
        """Delete an entire dataset."""
        self.delete(dataset)
    def drop_dataset(self, dataset: str):
        """Drop (delete) an entire dataset."""
        req = {"dataset": dataset}
        action_body = json.dumps(req).encode("utf-8")
        action = flight.Action("delete-dataset", action_body)
        list(self._require_meta_client().do_action(action, options=self._get_call_options()))
    def snapshot(self):
        """Trigger a manual snapshot of the database."""
        action = flight.Action("ForceSnapshot", b"")
        list(self._require_meta_client().do_action(action, options=self._get_call_options()))
    def get_info(self, dataset: str) -> Dict[str, Any]:
        """Get information about a dataset."""
        descriptor = flight.FlightDescriptor.for_path(dataset)
        info = self._require_meta_client().get_flight_info(
            descriptor, options=self._get_call_options()
        )
        return {
            "schema": str(info.schema),
            "total_records": info.total_records,
            "total_bytes": info.total_bytes,
        }
    def get_flight_info_metadata(self, dataset: str) -> Dict[str, Any]:
        """
        Retrieve FlightInfo for a dataset via the MetaServer.
        
        Args:
            dataset: Name of the dataset.
            
        Returns:
            Dict: Dataset statistics and metadata.
        """
        descriptor = flight.FlightDescriptor.for_path(dataset)
        info = self._require_meta_client().get_flight_info(
            descriptor, options=self._get_call_options()
        )
        return {
            "total_records": info.total_records,
            "total_bytes": info.total_bytes,
            "schema": str(info.schema),
            "endpoints": [str(e.locations) for e in info.endpoints],
        }
    def trigger_backup(self, dataset: str, destination: str = "") -> Dict[str, Any]:
        """Trigger an online snapshot backup for a dataset."""
        return self._do_action_json(
            "TriggerBackup",
            {"dataset": dataset, "destination": destination},
            default={},
        )
    def restore_backup(
        self, backup_id: str, dataset: str, pitr_timestamp: int = 0
    ) -> Dict[str, Any]:
        """Restore a dataset from a backup or point-in-time WAL timestamp (Unix ns)."""
        return self._do_action_json(
            "RestoreBackup",
            {
                "backup_id": backup_id,
                "dataset": dataset,
                "pitr_timestamp": pitr_timestamp,
            },
            default={},
        )
    # ---- Item 1: Managed Cloud Control Plane & Multi-Region Replication ----

    def cloud_create_cluster(
        self,
        name: str,
        primary_region: str,
        tier: str = "dedicated",
        tenant_id: str = "default",
        nodes: int = 3,
        replica_regions: Optional[Dict[str, str]] = None,
        replication_mode: str = "semi_sync",
        cluster_id: str = "",
    ) -> Dict[str, Any]:
        """Provision a managed multi-region Longbow Cloud cluster with tier SLA defaults."""
        return self._do_action_json(
            "CreateCloudCluster",
            {
                "id": cluster_id,
                "name": name,
                "tenant_id": tenant_id,
                "tier": tier,
                "primary_region": primary_region,
                "replica_regions": replica_regions or {},
                "nodes": nodes,
                "replication_mode": replication_mode,
            },
            default={},
        )
    def cloud_list_clusters(self, tenant_id: str = "") -> List[Dict[str, Any]]:
        """List managed cloud clusters, optionally filtered by tenant ID."""
        return self._do_action_json(
            "ListCloudClusters", {"tenant_id": tenant_id}, default={}
        ).get("clusters", [])
    def cloud_scale_cluster(
        self, cluster_id: str, nodes: int = 0, tier: str = ""
    ) -> Dict[str, Any]:
        """Scale node count and/or tier on an existing managed cloud cluster."""
        return self._do_action_json(
            "ScaleCloudCluster",
            {"id": cluster_id, "nodes": nodes, "tier": tier},
            default={},
        )
    def cloud_add_replica_region(
        self, cluster_id: str, region: str, endpoint: str = "local"
    ) -> Dict[str, Any]:
        """Attach a new geographic replica region to a managed cloud cluster."""
        return self._do_action_json(
            "AddReplicaRegion",
            {"id": cluster_id, "region": region, "endpoint": endpoint},
            default={},
        )
    def cloud_promote_region(
        self, cluster_id: str, target_region: str = "", reason: str = "failover"
    ) -> Dict[str, Any]:
        """Promote a replica region to primary during manual or automated failover."""
        return self._do_action_json(
            "PromoteClusterRegion",
            {
                "cluster_id": cluster_id,
                "target_region": target_region,
                "reason": reason,
            },
            default={},
        )
    def cloud_get_sla(
        self, cluster_id: str, observed_p99_ms: float = 0.0
    ) -> Dict[str, Any]:
        """Evaluate live SLA compliance (RPO/RTO/availability) for a managed cloud cluster."""
        return self._do_action_json(
            "GetCloudSLAReport",
            {"cluster_id": cluster_id, "observed_p99_ms": observed_p99_ms},
            default={},
        )
    def list_backups(self, dataset: str = "") -> List[Dict[str, Any]]:
        """List online backups (optionally filtered by dataset)."""
        return self._do_action_json(
            "ListBackups", {"dataset": dataset}, default={}
        ).get("backups", [])
    def verify_backup(self, backup_id: str, dataset: str = "") -> Dict[str, Any]:
        """Verify backup integrity via dry-run PITR replay."""
        return self._do_action_json(
            "VerifyBackup", {"backup_id": backup_id, "dataset": dataset}, default={}
        )
    def create_stream_connector(
        self,
        name: str,
        dataset: str,
        source_type: str = "memory",
        brokers: str = "",
        topic: str = "",
        format: str = "json",
        start: bool = False,
        **kwargs: Any,
    ) -> Dict[str, Any]:
        """Create a streaming CDC connector (Kafka, Pulsar, or Memory)."""
        payload: Dict[str, Any] = {
            "name": name,
            "dataset": dataset,
            "source_type": source_type,
            "brokers": brokers,
            "topic": topic,
            "format": format,
            "start": start,
        }
        payload.update(kwargs)
        return self._do_action_json("CreateStreamConnector", payload, default={})
    def stop_stream_connector(self, name: str) -> Dict[str, Any]:
        """Stop and remove a streaming CDC connector."""
        return self._do_action_json("StopStreamConnector", {"name": name}, default={})
    def list_stream_connectors(self) -> List[Dict[str, Any]]:
        """List streaming CDC connectors and their runtime stats."""
        return self._do_action_json("ListStreamConnectors", {}, default={}).get(
            "connectors", []
        )
    def get_stream_stats(self) -> List[Dict[str, Any]]:
        """Alias for list_stream_connectors returning connector throughput/lag stats."""
        return self._do_action_json("GetStreamStats", {}, default={}).get(
            "connectors", []
        )
    def ingest_stream_event(
        self, connector: str, envelopes: List[Dict[str, Any]]
    ) -> Dict[str, Any]:
        """Ingest streaming envelopes into a named connector."""
        return self._do_action_json(
            "IngestStreamEvent",
            {"connector": connector, "envelopes": envelopes},
            default={},
        )
    def register_stream_schema(
        self, subject: str, format: str = "json", fields: Optional[Dict[str, str]] = None
    ) -> Dict[str, Any]:
        """Register a local streaming schema subject."""
        return self._do_action_json(
            "RegisterStreamSchema",
            {"subject": subject, "format": format, "fields": fields or {}},
            default={},
        )
    def get_tenant_usage(self, tenant_id: str = "") -> Dict[str, Any]:
        """Retrieve per-tenant or global usage and billing metrics."""
        return self._do_action_json(
            "GetTenantUsage", {"tenant_id": tenant_id}, default={}
        )
    def set_tenant_quota(
        self,
        tenant_id: str,
        max_queries: int = 0,
        max_vectors: int = 0,
        max_storage_bytes: int = 0,
        soft_limit_ratio: float = 0.8,
        enforcement_mode: str = "hard",
    ) -> Dict[str, Any]:
        """Configure tenant usage quotas and enforcement mode."""
        return self._do_action_json(
            "SetTenantQuota",
            {
                "tenant_id": tenant_id,
                "max_queries": max_queries,
                "max_vectors": max_vectors,
                "max_storage_bytes": max_storage_bytes,
                "soft_limit_ratio": soft_limit_ratio,
                "enforcement_mode": enforcement_mode,
            },
            default={},
        )
    def export_billing_report(
        self, format: str = "json", tenant_id: str = ""
    ) -> Union[Dict[str, Any], str]:
        """Export tenant billing report in json, csv, or stripe format."""
        action = flight.Action(
            "ExportBillingReport",
            json.dumps({"format": format, "tenant_id": tenant_id}).encode("utf-8"),
        )
        results = list(
            self._require_meta_client().do_action(
                action, options=self._get_call_options()
            )
        )
        if not results:
            return {} if format == "json" else ""
        raw = results[0].body.to_pybytes().decode("utf-8")
        if format == "csv":
            return raw
        try:
            return json.loads(raw)
        except Exception:
            return raw
    def get_cluster_topology(self) -> Dict[str, Any]:
        """Fetch the elastic sharding ring topology and shard assignment snapshot."""
        return self._do_action_json("GetClusterTopology", {}, default={})
    def get_autoscale_config(self) -> Dict[str, Any]:
        """Fetch current elastic sharding & auto-scaling policy."""
        return self._do_action_json("GetAutoScaleConfig", {}, default={})
    def set_autoscale_config(self, policy: Dict[str, Any]) -> Dict[str, Any]:
        """Update elastic sharding & auto-scaling policy."""
        return self._do_action_json("SetAutoScaleConfig", policy, default={})
    def trigger_shard_split(self, dataset: str, shard_id: str) -> Dict[str, Any]:
        """Split a hot shard into left and right child shards."""
        return self._do_action_json(
            "TriggerShardSplit", {"dataset": dataset, "shard_id": shard_id}, default={}
        )
    def trigger_shard_merge(
        self, dataset: str, left_shard_id: str, right_shard_id: str
    ) -> Dict[str, Any]:
        """Merge two adjacent shards."""
        return self._do_action_json(
            "TriggerShardMerge",
            {
                "dataset": dataset,
                "left_shard_id": left_shard_id,
                "right_shard_id": right_shard_id,
            },
            default={},
        )
    def trigger_rebalance(self, dataset: str = "") -> Dict[str, Any]:
        """Trigger shard rebalancing across cluster nodes."""
        return self._do_action_json(
            "TriggerRebalance", {"dataset": dataset}, default={}
        )
    def register_schema(
        self,
        dataset: str,
        constraints: List[Dict[str, Any]],
        compatibility: str = "BACKWARD",
        metadata_types: Optional[Dict[str, str]] = None,
        strict_metadata: bool = False,
    ) -> Dict[str, Any]:
        """Register versioned field constraints and compatibility rules for a dataset."""
        return self._do_action_json(
            "RegisterSchema",
            {
                "dataset": dataset,
                "compatibility": compatibility,
                "constraints": constraints,
                "metadata_types": metadata_types or {},
                "strict_metadata": strict_metadata,
            },
            default={},
        )
    def get_schema_registry(self, dataset: str) -> Dict[str, Any]:
        """Get the latest registered schema specification for a dataset."""
        return self._do_action_json(
            "GetSchemaRegistry", {"dataset": dataset}, default={}
        )
    def validate_schema(
        self, dataset: str, vectors: List[Dict[str, Any]]
    ) -> Dict[str, Any]:
        """Dry-run validate a list of vector records against the dataset's registered schema."""
        return self._do_action_json(
            "ValidateSchema", {"dataset": dataset, "vectors": vectors}, default={}
        )
    def migrate_schema(
        self,
        dataset: str,
        target_schema: Dict[str, Any],
        add_string_cols: Optional[Dict[str, str]] = None,
    ) -> Dict[str, Any]:
        """Migrate dataset schema and backfill default values for newly added columns."""
        target = dict(target_schema)
        target["dataset"] = dataset
        return self._do_action_json(
            "MigrateSchema",
            {"target": target, "add_string_cols": add_string_cols or {}},
            default={},
        )
    def create_materialized_view(
        self,
        name: str,
        source_dataset: str,
        group_by: List[str],
        aggregations: Optional[List[Dict[str, str]]] = None,
        refresh_mode: str = "streaming",
        filter_equals: Optional[Dict[str, str]] = None,
    ) -> Dict[str, Any]:
        """Create a pre-aggregated materialized view over a dataset."""
        return self._do_action_json(
            "CreateMaterializedView",
            {
                "name": name,
                "source_dataset": source_dataset,
                "group_by": group_by,
                "aggregations": aggregations or [{"alias": "count", "op": "COUNT"}],
                "refresh_mode": refresh_mode,
                "filter_equals": filter_equals or {},
            },
            default={},
        )
    def refresh_materialized_view(self, name: str) -> Dict[str, Any]:
        """Refresh a materialized view on demand."""
        return self._do_action_json(
            "RefreshMaterializedView", {"name": name}, default={}
        )
    def query_materialized_view(
        self, name: str, group_filter: Optional[Dict[str, str]] = None
    ) -> Dict[str, Any]:
        """Query pre-aggregated rows from a materialized view."""
        return self._do_action_json(
            "QueryMaterializedView",
            {"name": name, "group_filter": group_filter or {}},
            default={},
        )
    def list_materialized_views(self) -> List[Dict[str, Any]]:
        """List all materialized views and their snapshots."""
        return self._do_action_json("ListMaterializedViews", {}, default={}).get(
            "views", []
        )
